#!/usr/bin/env python3
"""Ranker: turns the collector's quote history into route rankings.

Every RANK_INTERVAL it runs DuckDB over the Parquet files the collector wrote
and produces, per window (24h and 7d by default), corridor and size bucket, a
ranked list of sources:

  median_bps / p90_bps   typical and bad-moment effective cost (successful quotes)
  win_rate               share of *contested* rounds (>= 2 sources answered) where
                         this source was within TIE_BPS of the cheapest
  availability           share of attempts that returned a quote, ignoring our own
                         problems (rate_limited, unauthorized)
  median_fee_bps         fees as bps of input; LI.FI's 0.25% shows up here
  top_routes             which bridge/DEX the source actually used

Outputs (both written atomically):
  data/rankings/latest.json                                 what the API serves
  data/rankings/history/dt=YYYY-MM-DD/rankings-HHMMSS.parquet  flat copy per run

It runs separately from the Go services so DuckDB's C library never has to be
linked into Go (no cgo).

  python ranker/rank.py            # loop every RANK_INTERVAL
  python ranker/rank.py --once     # one run, e.g. from cron
"""

from __future__ import annotations

import argparse
import datetime as dt
import glob
import json
import math
import os
import signal
import sys
import tempfile
import time

import duckdb

SCHEMA_VERSION = 1
SERVICE = "exchange-logger-ranker"

# Error codes that say something about *us* (budget, keys), not about the route.
EXCLUDED_FROM_AVAILABILITY = ("rate_limited", "unauthorized")

# One row per (corridor, size_bucket, source) for one window.
RANKINGS_SQL = """
WITH q AS (
    SELECT * FROM quotes
    WHERE dt >= $since_date AND ts >= $since
),
ok AS (
    SELECT * FROM q WHERE quote_ok AND cost_bps IS NOT NULL
),
round_best AS (
    SELECT corridor, size_bucket, poll_round,
           min(cost_bps)          AS best_bps,
           count(DISTINCT source) AS n_sources
    FROM ok
    GROUP BY ALL
),
wins AS (
    SELECT ok.corridor, ok.size_bucket, ok.source,
           count(*)                                         AS contested_rounds,
           sum((ok.cost_bps <= r.best_bps + $tie_bps)::INT) AS wins
    FROM ok
    JOIN round_best r USING (corridor, size_bucket, poll_round)
    WHERE r.n_sources >= 2
    GROUP BY ALL
),
avail AS (
    SELECT corridor, size_bucket, source,
           count(*)                AS attempts,
           avg(quote_ok::INT)      AS availability
    FROM q
    WHERE coalesce(error_code, '') NOT IN ('rate_limited', 'unauthorized')
    GROUP BY ALL
),
cost AS (
    SELECT corridor, size_bucket, source,
           count(*)                                       AS samples,
           median(cost_bps)                               AS median_bps,
           quantile_cont(cost_bps, 0.9)                   AS p90_bps,
           -- 0 means "not reported" (LI.FI sends 0 for same-chain swaps)
           median(NULLIF(est_seconds, 0))                 AS median_seconds,
           median(protocol_fee_usd / input_usd * 10000)   AS median_fee_bps,
           epoch_ms(max(ts))                              AS last_ok_ms
    FROM ok
    GROUP BY ALL
)
SELECT a.corridor, a.size_bucket, a.source,
       a.attempts, a.availability,
       coalesce(c.samples, 0) AS samples,
       c.median_bps, c.p90_bps, c.median_seconds, c.median_fee_bps, c.last_ok_ms,
       coalesce(w.contested_rounds, 0) AS contested_rounds,
       coalesce(w.wins, 0)             AS wins
FROM avail a
LEFT JOIN cost c USING (corridor, size_bucket, source)
LEFT JOIN wins w USING (corridor, size_bucket, source)
ORDER BY a.corridor, a.size_bucket, a.source
"""

# Rounds per (corridor, size): all rounds, and contested ones.
ROUNDS_SQL = """
WITH ok AS (
    SELECT corridor, size_bucket, poll_round, source FROM quotes
    WHERE dt >= $since_date AND ts >= $since AND quote_ok AND cost_bps IS NOT NULL
),
per_round AS (
    SELECT corridor, size_bucket, poll_round, count(DISTINCT source) AS n_sources
    FROM ok GROUP BY ALL
)
SELECT corridor, size_bucket,
       count(*)                        AS rounds,
       count(*) FILTER (WHERE n_sources >= 2) AS contested_rounds
FROM per_round
GROUP BY ALL
"""

# Which internal route each source used, and what it cost.
ROUTES_SQL = """
SELECT corridor, size_bucket, source,
       coalesce(route_label, '') AS route_label,
       count(*)                  AS n,
       median(cost_bps)          AS median_bps
FROM quotes
WHERE dt >= $since_date AND ts >= $since AND quote_ok AND cost_bps IS NOT NULL
GROUP BY ALL
ORDER BY corridor, size_bucket, source, n DESC, route_label
"""


# --------------------------------------------------------------------------
# config and logging


def load_dotenv(path: str = ".env") -> None:
    """Set variables from .env that aren't already set (same rules as the Go services)."""
    try:
        with open(path, encoding="utf-8") as f:
            lines = f.readlines()
    except FileNotFoundError:
        return
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):]
        key, sep, value = line.partition("=")
        if not sep:
            continue
        key, value = key.strip(), value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        elif " #" in value:
            value = value.split(" #", 1)[0].strip()
        os.environ.setdefault(key, value)


def parse_duration(text: str) -> dt.timedelta:
    """Parse 30s, 10m, 24h, 7d."""
    text = text.strip().lower()
    units = {"s": "seconds", "m": "minutes", "h": "hours", "d": "days"}
    if len(text) < 2 or text[-1] not in units:
        raise ValueError(f"bad duration {text!r}: use a number with s, m, h or d (e.g. 10m, 7d)")
    value = float(text[:-1])
    if value <= 0:
        raise ValueError(f"duration must be positive: {text!r}")
    return dt.timedelta(**{units[text[-1]]: value})


def log(event: str, level: str = "INFO", **fields) -> None:
    rec = {"time": dt.datetime.now(dt.timezone.utc).isoformat(), "level": level, "service": SERVICE, "event": event}
    rec.update(fields)
    print(json.dumps(rec, default=str), flush=True)


# --------------------------------------------------------------------------
# ranking


def _iso(t):
    if t is None:
        return None
    if t.tzinfo is None:
        t = t.replace(tzinfo=dt.timezone.utc)
    return t.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _iso_ms(ms):
    if ms is None:
        return None
    return _iso(dt.datetime.fromtimestamp(ms / 1000, tz=dt.timezone.utc))


def _num(v, digits=2):
    if v is None:
        return None
    v = float(v)
    if math.isnan(v) or math.isinf(v):
        return None
    return round(v, digits)


def _sql_str(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def compute(data_dir: str, windows: list[str], tie_bps: float, min_samples: int,
            now: dt.datetime | None = None, top_routes: int = 3) -> dict:
    """Compute rankings for every window. Returns the latest.json document."""
    now = now or dt.datetime.now(dt.timezone.utc)
    doc = {
        "schema_version": SCHEMA_VERSION,
        "computed_at": _iso(now),
        "data_through": None,
        "windows": windows,
        "params": {
            "tie_bps": tie_bps,
            "min_samples": min_samples,
            "availability_excludes": list(EXCLUDED_FROM_AVAILABILITY),
        },
        "rankings": [],
    }
    pattern = os.path.join(data_dir, "quotes", "**", "*.parquet")
    if not glob.glob(pattern, recursive=True):
        return doc  # nothing collected yet; the API reports "no data"

    con = duckdb.connect()
    con.execute(
        "CREATE VIEW quotes AS SELECT * FROM read_parquet("
        + _sql_str(pattern)
        + ", hive_partitioning = true, union_by_name = true)"
    )
    # Timestamps come back as epoch ms: fetching TIMESTAMPTZ values would need pytz.
    doc["data_through"] = _iso_ms(con.execute("SELECT epoch_ms(max(ts)) FROM quotes").fetchone()[0])

    for window in windows:
        since = now - parse_duration(window)
        params = {"since": since, "since_date": since.date()}

        rounds = {
            (c, s): (n, contested)
            for c, s, n, contested in con.execute(ROUNDS_SQL, params).fetchall()
        }

        routes_by_key: dict[tuple, list] = {}
        for corridor, size, source, label, n, med in con.execute(ROUTES_SQL, params).fetchall():
            routes_by_key.setdefault((corridor, size, source), []).append((label, n, med))

        groups: dict[tuple, list] = {}
        cols = [
            "corridor", "size_bucket", "source", "attempts", "availability", "samples",
            "median_bps", "p90_bps", "median_seconds", "median_fee_bps", "last_ok_ms",
            "contested_rounds", "wins",
        ]
        for row in con.execute(RANKINGS_SQL, {**params, "tie_bps": tie_bps}).fetchall():
            r = dict(zip(cols, row))
            key = (r["corridor"], r["size_bucket"])
            labels = routes_by_key.get((r["corridor"], r["size_bucket"], r["source"]), [])
            route = {
                "source": r["source"],
                "median_bps": _num(r["median_bps"]),
                "p90_bps": _num(r["p90_bps"]),
                "win_rate": _num(r["wins"] / r["contested_rounds"], 3) if r["contested_rounds"] else None,
                "availability": _num(r["availability"], 3),
                "attempts": int(r["attempts"]),
                "samples": int(r["samples"]),
                "contested_rounds": int(r["contested_rounds"]),
                "median_seconds": _num(r["median_seconds"], 1),
                "median_fee_bps": _num(r["median_fee_bps"]),
                "low_sample": int(r["samples"]) < min_samples,
                "last_ok": _iso_ms(r["last_ok_ms"]),
                "top_routes": [
                    {"label": label or None, "share": _num(n / r["samples"], 3), "median_bps": _num(med)}
                    for label, n, med in labels[:top_routes]
                ] if r["samples"] else [],
            }
            groups.setdefault(key, []).append(route)

        for (corridor, size), routes in sorted(groups.items()):
            # Enough data first, then cheapest; sources that never answered go last.
            routes.sort(key=lambda x: (x["low_sample"], x["median_bps"] is None, x["median_bps"] or 0.0, x["source"]))
            n_rounds, n_contested = rounds.get((corridor, size), (0, 0))
            doc["rankings"].append({
                "window": window,
                "corridor": corridor,
                "size_bucket": int(size),
                "rounds": int(n_rounds),
                "contested_rounds": int(n_contested),
                "routes": routes,
            })
    con.close()
    return doc


# --------------------------------------------------------------------------
# output


def write_json_atomic(path: str, doc: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".latest-", suffix=".json.tmp", dir=os.path.dirname(path))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(doc, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.remove(tmp)
        except OSError:
            pass
        raise


def write_history(data_dir: str, doc: dict) -> str | None:
    """Write one flat Parquet file per run so ranking drift can be studied later."""
    rows = []
    for rk in doc["rankings"]:
        for rank, r in enumerate(rk["routes"], start=1):
            rows.append((
                doc["computed_at"], rk["window"], rk["corridor"], rk["size_bucket"], rank, r["source"],
                r["median_bps"], r["p90_bps"], r["win_rate"], r["availability"], r["attempts"],
                r["samples"], r["median_seconds"], r["median_fee_bps"], r["low_sample"],
            ))
    if not rows:
        return None
    computed = dt.datetime.fromisoformat(doc["computed_at"].replace("Z", "+00:00"))
    out_dir = os.path.join(data_dir, "rankings", "history", "dt=" + computed.strftime("%Y-%m-%d"))
    os.makedirs(out_dir, exist_ok=True)
    final = os.path.join(out_dir, "rankings-" + computed.strftime("%H%M%S") + ".parquet")
    tmp = os.path.join(out_dir, "." + os.path.basename(final) + ".tmp")

    con = duckdb.connect()
    con.execute("""
        CREATE TABLE h (
            computed_at TIMESTAMPTZ, "window" VARCHAR, corridor VARCHAR, size_bucket INTEGER,
            rank INTEGER, source VARCHAR, median_bps DOUBLE, p90_bps DOUBLE, win_rate DOUBLE,
            availability DOUBLE, attempts INTEGER, samples INTEGER, median_seconds DOUBLE,
            median_fee_bps DOUBLE, low_sample BOOLEAN)
    """)
    con.executemany("INSERT INTO h VALUES (?::TIMESTAMPTZ, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    con.execute(f"COPY h TO {_sql_str(tmp)} (FORMAT PARQUET, COMPRESSION ZSTD)")
    con.close()
    os.replace(tmp, final)
    return final


def run_once(data_dir: str, windows: list[str], tie_bps: float, min_samples: int, history: bool) -> dict:
    started = time.monotonic()
    doc = compute(data_dir, windows, tie_bps, min_samples)
    write_json_atomic(os.path.join(data_dir, "rankings", "latest.json"), doc)
    hist = write_history(data_dir, doc) if history else None
    log("ranking_complete",
        computed_at=doc["computed_at"], data_through=doc["data_through"],
        groups=len(doc["rankings"]), routes=sum(len(r["routes"]) for r in doc["rankings"]),
        history_file=hist, duration_ms=int((time.monotonic() - started) * 1000))
    return doc


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    p = argparse.ArgumentParser(description="Rank quote sources from the collector's Parquet history.")
    p.add_argument("--once", action="store_true", help="run once and exit")
    p.add_argument("--data-dir", default=os.environ.get("DATA_DIR", "./data"))
    p.add_argument("--interval", default=os.environ.get("RANK_INTERVAL", "10m"))
    p.add_argument("--windows", default=os.environ.get("RANK_WINDOWS", "24h,7d"))
    p.add_argument("--tie-bps", type=float, default=float(os.environ.get("RANK_TIE_BPS", "2")))
    p.add_argument("--min-samples", type=int, default=int(os.environ.get("RANK_MIN_SAMPLES", "30")))
    p.add_argument("--no-history", action="store_true", help="skip the per-run history Parquet file")
    args = p.parse_args(argv)

    try:
        interval = parse_duration(args.interval)
        windows = [w.strip() for w in args.windows.split(",") if w.strip()]
        for w in windows:
            parse_duration(w)
        if not windows:
            raise ValueError("RANK_WINDOWS is empty")
        if args.tie_bps < 0 or args.min_samples < 1:
            raise ValueError("RANK_TIE_BPS must be >= 0 and RANK_MIN_SAMPLES >= 1")
    except ValueError as e:
        log("ranker_config_invalid", "ERROR", error=str(e))
        return 2

    if args.once:
        try:
            run_once(args.data_dir, windows, args.tie_bps, args.min_samples, not args.no_history)
            return 0
        except Exception as e:  # noqa: BLE001 - report and exit non-zero for cron
            log("ranking_failed", "ERROR", error=f"{type(e).__name__}: {e}")
            return 1

    stopping = False

    def stop(*_):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    log("ranker_started", data_dir=args.data_dir, interval=args.interval, windows=windows,
        tie_bps=args.tie_bps, min_samples=args.min_samples)
    while not stopping:
        started = time.monotonic()
        try:
            run_once(args.data_dir, windows, args.tie_bps, args.min_samples, not args.no_history)
        except Exception as e:  # noqa: BLE001 - keep the last good latest.json and try again
            log("ranking_failed", "ERROR", error=f"{type(e).__name__}: {e}")
        deadline = started + interval.total_seconds()
        while not stopping and time.monotonic() < deadline:
            time.sleep(min(1.0, deadline - time.monotonic()))
    log("ranker_stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
