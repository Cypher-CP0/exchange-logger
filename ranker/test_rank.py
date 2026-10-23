"""Tests for the ranker on synthetic quote files with known answers.

Run from the repo root:  python -m unittest discover -s ranker
"""

import datetime as dt
import json
import os
import sys
import tempfile
import unittest

import duckdb

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import rank  # noqa: E402

NOW = dt.datetime(2026, 10, 23, 12, 0, 0, tzinfo=dt.timezone.utc)
COR = "base-usdc__sol-usdc"


def q(round_min_ago, source, cost=None, error=None, route=None, est=None, fee=None, corridor=COR, size=1000):
    """One quote row; cost=None with error set means a failed quote."""
    ts = NOW - dt.timedelta(minutes=round_min_ago)
    return {
        "quote_id": f"{source}-{round_min_ago}-{corridor}-{size}",
        "poll_round": int(ts.timestamp() * 1000),
        "ts": ts,
        "source": source,
        "route_label": route,
        "corridor": corridor,
        "size_bucket": size,
        "input_usd": float(size),
        "protocol_fee_usd": fee,
        "est_seconds": est,
        "cost_bps": cost,
        "quote_ok": error is None,
        "error_code": error,
    }


ROWS = [
    # round 1: s1 and s2 within the 2 bps tie margin -> both win
    q(10, "s1", cost=10.0, route="relay", est=5, fee=2.5),
    q(10, "s2", cost=11.0, route="relay", est=0, fee=2.5),
    q(10, "s3", error="timeout"),
    # round 2: s1 clearly cheaper
    q(20, "s1", cost=10.0, route="relay", est=7, fee=2.5),
    q(20, "s2", cost=20.0, route="relay", est=0, fee=2.5),
    q(20, "s3", error="no_route"),
    # round 3: s1 times out -> not a contested round
    q(30, "s1", error="timeout"),
    q(30, "s2", cost=15.0, route="relay", fee=2.5),
    # round 4: s1 rate limited (our budget, excluded from availability)
    q(40, "s1", error="rate_limited"),
    q(40, "s2", cost=12.0, route="cctp", fee=2.5),
    # 3 days ago: only in the 7d window
    q(3 * 24 * 60, "s2", cost=100.0, route="relay", fee=2.5),
    # another corridor and size, single source
    q(10, "jupiter", cost=1.5, corridor="sol-sol__sol-usdc", size=100, route="metis:ZeroFi"),
]


def write_quotes(data_dir, rows):
    """Write rows the way the Go collector does: one file per dt/corridor, columns kept in the file."""
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE t (quote_id VARCHAR, poll_round BIGINT, ts TIMESTAMPTZ, source VARCHAR,
            route_label VARCHAR, corridor VARCHAR, size_bucket INTEGER, input_usd DOUBLE,
            protocol_fee_usd DOUBLE, est_seconds INTEGER, cost_bps DOUBLE, quote_ok BOOLEAN,
            error_code VARCHAR)""")
    cols = list(rows[0].keys())
    con.executemany(f"INSERT INTO t VALUES ({', '.join('?' for _ in cols)})", [[r[c] for c in cols] for r in rows])
    parts = con.execute("SELECT DISTINCT strftime(ts AT TIME ZONE 'UTC', '%Y-%m-%d'), corridor FROM t").fetchall()
    for day, corridor in parts:
        d = os.path.join(data_dir, "quotes", f"dt={day}", f"corridor={corridor}")
        os.makedirs(d, exist_ok=True)
        path = os.path.join(d, "part-test-0001.parquet").replace("'", "''")
        con.execute(f"""COPY (SELECT * FROM t WHERE corridor = ? AND strftime(ts AT TIME ZONE 'UTC', '%Y-%m-%d') = ?)
                        TO '{path}' (FORMAT PARQUET)""", [corridor, day])
    con.close()


class RankerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.data = self.tmp.name
        write_quotes(self.data, ROWS)
        self.doc = rank.compute(self.data, ["24h", "7d"], tie_bps=2.0, min_samples=3, now=NOW)

    def tearDown(self):
        self.tmp.cleanup()

    def group(self, window, corridor=COR, size=1000):
        for g in self.doc["rankings"]:
            if (g["window"], g["corridor"], g["size_bucket"]) == (window, corridor, size):
                return g
        self.fail(f"no group {window} {corridor} {size}")

    def route(self, g, source):
        return next(r for r in g["routes"] if r["source"] == source)

    def test_metrics(self):
        g = self.group("24h")
        self.assertEqual(g["rounds"], 4)
        self.assertEqual(g["contested_rounds"], 2)

        s1 = self.route(g, "s1")
        self.assertEqual(s1["attempts"], 3, "rate_limited attempt must not count")
        self.assertAlmostEqual(s1["availability"], 0.667)
        self.assertEqual(s1["samples"], 2)
        self.assertEqual(s1["median_bps"], 10.0)
        self.assertEqual(s1["win_rate"], 1.0)
        self.assertEqual(s1["contested_rounds"], 2)
        self.assertEqual(s1["median_seconds"], 6.0)
        self.assertEqual(s1["median_fee_bps"], 25.0)

        s2 = self.route(g, "s2")
        self.assertEqual(s2["availability"], 1.0)
        self.assertEqual(s2["samples"], 4)
        self.assertEqual(s2["median_bps"], 13.5)
        self.assertEqual(s2["win_rate"], 0.5, "round 1 is a tie (1 bps apart), round 2 is lost")
        self.assertIsNone(s2["median_seconds"], "est_seconds 0 means not reported")
        self.assertEqual(s2["top_routes"][0], {"label": "relay", "share": 0.75, "median_bps": 15.0})

        s3 = self.route(g, "s3")
        self.assertEqual(s3["availability"], 0.0)
        self.assertIsNone(s3["median_bps"])
        self.assertIsNone(s3["win_rate"])
        self.assertEqual(s3["top_routes"], [])

    def test_order_puts_enough_data_first(self):
        # s1 is cheaper but has 2 samples < min_samples=3; s3 never answered.
        self.assertEqual([r["source"] for r in self.group("24h")["routes"]], ["s2", "s1", "s3"])
        self.assertTrue(self.route(self.group("24h"), "s1")["low_sample"])

    def test_windows(self):
        self.assertEqual(self.route(self.group("24h"), "s2")["samples"], 4)
        self.assertEqual(self.route(self.group("7d"), "s2")["samples"], 5)
        self.assertEqual(self.route(self.group("7d"), "s2")["median_bps"], 15.0)

    def test_single_source_has_no_win_rate(self):
        g = self.group("24h", "sol-sol__sol-usdc", 100)
        self.assertEqual(g["contested_rounds"], 0)
        self.assertIsNone(g["routes"][0]["win_rate"])

    def test_document_and_files(self):
        self.assertEqual(self.doc["schema_version"], 1)
        self.assertEqual(self.doc["computed_at"], "2026-10-23T12:00:00Z")
        self.assertEqual(self.doc["data_through"], "2026-10-23T11:50:00Z")
        json.dumps(self.doc)  # must be serializable

        path = os.path.join(self.data, "rankings", "latest.json")
        rank.write_json_atomic(path, self.doc)
        with open(path) as f:
            self.assertEqual(json.load(f)["computed_at"], self.doc["computed_at"])
        self.assertEqual([n for n in os.listdir(os.path.dirname(path)) if n.endswith(".tmp")], [])

        hist = rank.write_history(self.data, self.doc)
        con = duckdb.connect()
        n = con.execute("SELECT count(*) FROM read_parquet(?)", [hist]).fetchall()[0][0]
        con.close()
        self.assertEqual(n, sum(len(g["routes"]) for g in self.doc["rankings"]))

    def test_no_data_yet(self):
        with tempfile.TemporaryDirectory() as empty:
            doc = rank.compute(empty, ["24h"], 2.0, 30, now=NOW)
        self.assertEqual(doc["rankings"], [])
        self.assertIsNone(doc["data_through"])

    def test_parse_duration(self):
        self.assertEqual(rank.parse_duration("10m"), dt.timedelta(minutes=10))
        self.assertEqual(rank.parse_duration("7d"), dt.timedelta(days=7))
        with self.assertRaises(ValueError):
            rank.parse_duration("soon")


if __name__ == "__main__":
    unittest.main()