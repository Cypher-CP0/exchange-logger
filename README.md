# exchange-logger

Records swap and bridge quotes on a fixed schedule, keeps every one of them, and ranks routes by what they actually cost over time, not just what they cost right now. Live aggregators answer "what's the best route right now"; nobody publishes the history. FluxPay's exchange engine uses these rankings to decide which 2–3 sources are worth asking for a live quote.

```
quote sources ──► collector ──► Parquet ──► ranker ──► latest.json ──► rankings API ──► FluxPay
 Jupiter, LI.FI    Go, every     by day,     DuckDB,                     Go, from memory
 Binance, OKX      30s           corridor    every 10m                   GET /v1/rankings
```

Three processes, each on its own clock, sharing only the `data/` directory:

| Process | Language | What it does |
| --- | --- | --- |
| `cmd/collector` | Go | Polls sources every 30s, writes every quote (failures included) to Parquet. A worker: nothing can trigger it over HTTP |
| `ranker/rank.py` | Python + DuckDB | Every 10 min, ranks sources per corridor and size over 24h and 7d windows, writes `data/rankings/latest.json` |
| `cmd/api` | Go | Serves `latest.json` from memory, reloads it when it changes. Never touches Parquet |

DuckDB runs in Python so its C library is never linked into Go: both Go binaries are cgo-free and build natively on Windows.

## Quick start

Needs Go 1.22+ and Python 3.10+.

```bash
cp .env.example .env              # add JUPITER_API_KEY, LIFI_API_KEY, API_KEY

# terminal 1: collector
go run ./cmd/collector

# terminal 2: ranker (first run: create a venv)
python3 -m venv .venv && . .venv/bin/activate && pip install -r ranker/requirements.txt
python ranker/rank.py             # loops every 10 min; --once for a single run

# terminal 3: API
go run ./cmd/api                  # listens on 127.0.0.1:8081

curl -H "X-API-Key: $API_KEY" "http://127.0.0.1:8081/v1/rankings?from=base&to=solana&token=USDC&amount=500"
```

The collector flushes every 5 minutes, so the ranker has nothing to rank until the first flush (set `FLUSH_INTERVAL=1m` while trying it out). Every process stops cleanly on Ctrl+C.

### API keys

| Source | Key | Without a key |
| --- | --- | --- |
| Jupiter | Required. Get one at [developers.jup.ag/portal](https://developers.jup.ag/portal) | The jupiter source is disabled at startup |
| LI.FI | Optional, but you want one: [portal.li.fi](https://portal.li.fi) | `/v1/quote` is capped at 75 requests per 2 hours, so most LI.FI quotes are skipped |
| Binance, OKX | None (public tickers) | — |

With default settings a round needs 3 Jupiter quotes and 18 LI.FI quotes. With a LI.FI key (100 requests/min) that fits comfortably at 30-second rounds. At startup the collector logs `budget_insufficient` if a source's rate budget can't cover a round.

## What it collects

Six corridors, three sizes ($100, $1k, $10k), every 30 seconds:

| Corridor id | Route | Sources today |
| --- | --- | --- |
| `sol-sol__sol-usdc` | SOL → USDC on Solana | Jupiter, LI.FI |
| `base-usdc__sol-usdc` | USDC Base → USDC Solana | LI.FI |
| `arb-usdc__sol-usdc` | USDC Arbitrum → USDC Solana | LI.FI |
| `eth-usdc__sol-usdc` | USDC Ethereum → USDC Solana | LI.FI |
| `base-eth__sol-usdc` | ETH Base → USDC Solana | LI.FI |
| `base-eth__base-usdc` | ETH → USDC on Base | LI.FI |

LI.FI is an aggregator, so the bridge or DEX it chose (Mayan, CCTP, Across, Relay…) is kept in `route_label`. deBridge, Mayan direct, CCTP and 0x are step 4.

### How a round works

1. Take **one price snapshot** (SOL, ETH, USDC from Binance and OKX, averaged). If no price can be had for a symbol, the whole round is skipped: costing quotes against stale prices would corrupt rankings.
2. Work out the exact input amount for each corridor and size from that snapshot ($1,000 of SOL at $200 is 5 SOL).
3. Ask every source for every corridor and size it supports, spread out by a per-source token bucket, each request with a 5-second timeout.
4. Normalize each answer into one row and compute `cost_bps`. **Failed quotes are rows too**, with `quote_ok = false` and an `error_code`. Availability rankings depend on them.
5. Buffer rows; flush to Parquet every 5 minutes, at 50,000 rows, and on shutdown.

All quotes in a round share the same `poll_round` id and the same prices, so they can be compared directly against each other.

### Effective cost

```
cost_bps = (input_usd − (output_usd − gas_usd_src − gas_usd_dst − extra_fee_usd)) / input_usd × 10000
```

Everything the user loses between sending and receiving, relative to the reference price. Each adapter documents how its fees are counted so nothing is counted twice:

- **Jupiter**: `outAmount` is already net of Jupiter's fees, so `extra_fee_usd` is 0. Gas is the signature fee plus priority fee, priced at the snapshot's SOL price. Rent is left out; it's mostly refundable.
- **LI.FI**: fees marked `included: true` are already taken out of `toAmount`; fees marked `included: false` are paid on top and go into `extra_fee_usd`. Gas and fees are re-priced with the round's own snapshot when they're in ETH, SOL or USDC. LI.FI doesn't report destination gas separately, so `gas_usd_dst` is NULL.

## Output

```
data/
  quotes/dt=2026-10-22/corridor=base-usdc__sol-usdc/part-20261022T181647Z-0010.parquet
  prices/dt=2026-10-22/part-20261022T181647Z-0002.parquet
  status/collector.json          # last round's stats, for the API's /v1/health later
```

Files are immutable. Each is written under a hidden `.tmp` name and renamed when complete, so a reader globbing `*.parquet` never sees half a file.

### Quote schema

| Column | Type | Notes |
| --- | --- | --- |
| quote_id | VARCHAR | UUIDv7 |
| poll_round | BIGINT | Round start, unix ms. Shared by all quotes in a round |
| ts | TIMESTAMPTZ | When the response arrived |
| source | VARCHAR | `jupiter`, `lifi` |
| route_label | VARCHAR | Source's internal route, e.g. `mayan`, `metis:Raydium CLMM+Orca Whirlpool` |
| corridor, size_bucket | VARCHAR, INTEGER | e.g. `base-usdc__sol-usdc`, `1000` |
| input_token, output_token | VARCHAR | Mint or contract address |
| input_amount, expected_output, min_output | DECIMAL(38,0) | Raw token units. `min_output` is after slippage |
| input_usd, output_usd | DOUBLE | At the round's snapshot prices |
| gas_usd_src, gas_usd_dst | DOUBLE | NULL when the source doesn't report it |
| protocol_fee_usd | DOUBLE | All bridge/relayer/aggregator fees (informational) |
| extra_fee_usd | DOUBLE | Fees not already deducted from `expected_output` |
| est_seconds | INTEGER | Source's own time estimate |
| cost_bps | DOUBLE | See formula above. NULL for failed quotes |
| latency_ms | INTEGER | How long the source took to answer |
| quote_ok | BOOLEAN | |
| error_code, error_detail | VARCHAR | See below |

DuckDB adds `dt` (DATE) from the partition path.

### Error codes

| Code | Meaning | Counts against availability? |
| --- | --- | --- |
| `timeout` | No response within `REQUEST_TIMEOUT` | Yes |
| `network` | Connection failed | Yes |
| `no_route` | Source answered but has no route | Yes |
| `http_<status>` | Other HTTP error, e.g. `http_500` | Yes |
| `bad_response` | Undecodable or missing fields | Yes |
| `source_paused` | Circuit open after 10 failed rounds in a row; no request sent | Yes (by design) |
| `rate_limited` | HTTP 429 from the source | Probably not: it's our budget, not their route. The ranker should exclude it |
| `unauthorized` | 401/403, usually a bad API key | No; fix the key |

Requests skipped because of our own rate budget or a 429 backoff are **not** written as rows; they're counted in the `round_complete` log line instead.

### Reliability behavior

- **429**: the source backs off exponentially (5s, 10s, 20s… up to 5 min, or longer if `Retry-After` says so).
- **Circuit breaker**: after 10 rounds in a row where every attempt failed, a source is paused for 5 minutes. Paused rounds still produce `source_paused` rows.
- **Shutdown**: stops sending new requests, lets in-flight ones finish, then flushes.
- **Disk errors**: a failed flush keeps the rows buffered and retries next time. Past 10× `FLUSH_MAX_ROWS` new rows are dropped with a `rows_dropped` error log.
- **Crash**: at most one flush interval (5 min) of data is lost. Acceptable for analytics.

## Looking at the data

With the DuckDB CLI (or `pip install duckdb`):

```bash
duckdb -c ".read scripts/inspect.sql"
```

It shows availability and latency per source, failures by code, a preview of the ranking from the design doc, and which bridge each aggregator picked.

## Ranker

`ranker/rank.py` runs three DuckDB queries over the quote files for each window (default `24h` and `7d`) and writes one ranked list per window, corridor and size bucket.

| Metric | Definition |
| --- | --- |
| `median_bps`, `p90_bps` | Median and 90th-percentile `cost_bps` of successful quotes |
| `win_rate` | Share of **contested** rounds (2+ sources answered) where the source was within `tie_bps` (default 2) of the cheapest. `null` when the source never faced competition |
| `availability` | Share of attempts that returned a quote. `rate_limited` and `unauthorized` are left out: they're our budget or keys, not the route |
| `median_fee_bps` | Fees as bps of input. LI.FI's 0.25% service fee shows up here as 25 |
| `median_seconds` | Median of the source's own time estimate. A reported 0 is treated as "not reported" |
| `samples`, `attempts`, `contested_rounds` | How much the numbers are based on |
| `low_sample` | Fewer than `min_samples` (default 30) successful quotes |
| `top_routes` | The 3 bridges or DEXes the source used most, with share and median cost |

Why these choices:

- **Win rate only counts contested rounds.** Otherwise a source "wins" every round its competitor failed to answer.
- **The tie margin.** Costs are measured against a CEX reference price that is itself off by a bp or so, so a 1 bp gap isn't a real difference.
- **Order.** Routes are sorted by: enough samples first, then cheapest median, then sources that never answered. A cheap source with 5 samples doesn't outrank one with 5,000.

Output:

```
data/rankings/latest.json                                    what the API serves (written atomically)
data/rankings/history/dt=2026-10-23/rankings-061715.parquet  flat copy of every run, to study ranking drift
```

A failed run leaves the previous `latest.json` in place; the API flags rankings as `stale` after 30 minutes.

## Rankings API

### `GET /v1/rankings`

| Parameter | |
| --- | --- |
| `from`, `to` | Chains: `solana`/`sol`, `base`, `arbitrum`/`arb`, `ethereum`/`eth` |
| `token` | Same token on both sides, e.g. `USDC`. Or `from_token` + `to_token` for swaps (`from_token=ETH&to_token=USDC`) |
| `corridor` | Alternative to the above: `base-usdc__sol-usdc` |
| `amount` | Payment size in USD. Mapped to the nearest size bucket **on a log scale** ($500 → $1,000 bucket; $150 → $100), because fixed fees make cost depend on order of magnitude |
| `size_bucket` | Alternative to `amount` |
| `window` | `7d` (default) or `24h` |

```json
{
  "corridor": "base-usdc__sol-usdc",
  "size_bucket": 1000,
  "requested_amount_usd": 500,
  "window": "7d",
  "computed_at": "2026-10-23T06:17:15Z",
  "age_seconds": 8,
  "data_through": "2026-10-23T06:17:14Z",
  "stale": false,
  "rounds": 1210,
  "contested_rounds": 0,
  "tie_bps": 2,
  "routes": [
    { "source": "lifi", "median_bps": 29.4, "p90_bps": 33.1, "win_rate": null, "availability": 1,
      "attempts": 1210, "samples": 1210, "contested_rounds": 0, "median_seconds": 21,
      "median_fee_bps": 25, "low_sample": false, "last_ok": "2026-10-23T06:17:12Z",
      "top_routes": [ { "label": "lifiIntents", "share": 0.8, "median_bps": 27.4 } ] }
  ]
}
```

| Response | When |
| --- | --- |
| 200 with routes | Normal |
| 200, `routes: []` and a `note` | Tracked corridor, but no data for that size yet |
| 200, `stale: true` | `stale_reason` says why: `ranker_behind` (rankings older than `RANKINGS_STALE_AFTER`, 30 min), `collector_behind` (the newest quote is older than that, i.e. the collector stopped even if the ranker didn't), or `no_data` |
| 400 | Bad or missing parameters; the body says which |
| 401 | Missing or wrong API key |
| 404 | Corridor isn't tracked; the body lists the tracked ones |
| 503 | No rankings loaded yet (ranker hasn't run) |

### How FluxPay should use it

1. Call `/v1/rankings` with the customer's corridor and amount.
2. Ask the top 2–3 routes for **live** quotes and take the best one. The ranking picks who to ask; it never sets the price.
3. Fall back to asking every source if the call fails, returns `stale: true`, or returns no routes.

### `GET /v1/corridors`

Tracked corridors, their chains and tokens, and which size buckets have data in each window.

### `GET /v1/health`

`status` is `ok` or `degraded`, with a `problems` list. It checks:

- **Rankings age:** stale after 30 min.
- **Collector freshness:** the collector rewrites `data/status/collector.json` every round, so it's stale after 2 min without an update. Skipped rounds and paused sources also count as problems.
- **Per-source availability:** over the last 24h.

Health needs no API key, so a monitor (e.g. a Better Stack check) can watch it.

### Auth

Set `API_KEY` (16+ characters) and send it as `X-API-Key` or `Authorization: Bearer`. Without `API_KEY` the API only starts on a loopback address. To listen publicly without one you must set `API_ALLOW_NO_AUTH=true`, which is only sensible on a private network.

## Logs

All three processes log JSON on stdout, one object per line, with an `event` field:

- **Collector:** `collector_started`, `round_complete`, `round_skipped`, `source_rate_limited`, `source_paused`, `flush_complete`, `flush_failed`, `budget_insufficient`…
- **Ranker:** `ranking_complete`, `ranking_failed`.
- **API:** `api_started`, `rankings_loaded`, `rankings_load_failed`, `api_request`.

`round_complete` carries per-source attempted/ok/error counts and median latency.

## Configuration

All settings are environment variables. All three processes read the same `.env` if present, and it never overrides real environment variables. See `.env.example` for the full list. The useful ones:

| Variable | Default | |
| --- | --- | --- |
| `JUPITER_API_KEY`, `LIFI_API_KEY` | — | See above |
| `CORRIDORS` | all six | Comma-separated corridor ids |
| `SIZE_BUCKETS` | `100,1000,10000` | USD |
| `SOURCES` | `jupiter,lifi` | |
| `POLL_INTERVAL` | `30s` | |
| `REQUEST_TIMEOUT` | `5s` | Per quote |
| `FLUSH_INTERVAL` | `5m` | |
| `SLIPPAGE_BPS` | `50` | Same tolerance for every source, so `min_output` is comparable |
| `DATA_DIR` | `./data` | |
| `JUPITER_ORDER_URL` | `https://api.jup.ag/swap/v2/order` | Point at a mock to test without a key |
| `RANK_INTERVAL`, `RANK_WINDOWS` | `10m`, `24h,7d` | Ranker |
| `RANK_TIE_BPS`, `RANK_MIN_SAMPLES` | `2`, `30` | Ranker |
| `API_LISTEN_ADDR` | `127.0.0.1:8081` | API |
| `API_KEY` | — | API; 16+ characters. Required to listen on a non-loopback address |
| `RANKINGS_STALE_AFTER`, `COLLECTOR_STALE_AFTER` | `30m`, `2m` | API staleness thresholds |

### Running against mock-jupiter

`JUPITER_ORDER_URL=http://localhost:8090/quote` points the Jupiter adapter at the mock. Without a key, Jupiter is only enabled when the URL isn't the real one. The mock's failure modes (`slow`, `fail`, `timeout`) then show up as `timeout` and `http_5xx` rows, which is a quick way to see failure recording work.

## Development

```bash
go test -race ./...
go vet ./...
python -m unittest discover -s ranker
```

What the tests cover:

- **Adapters:** tested against fixtures shaped like the real responses, served with `httptest`.
- **Collector:** fake sources check comparable rows, skipped rounds, 429 backoff, the circuit breaker, rate budgets, job rotation and shutdown.
- **Ranker:** synthetic quote files with known answers check contested rounds, ties, availability exclusions, windows and ordering.
- **API:** lookups, nearest bucket, staleness, auth, health, and reloading a new `latest.json` without ever replacing a good one with a broken file.

## Layout

```
cmd/collector/          collector main: wiring, startup checks, graceful shutdown
cmd/api/                rankings API main
ranker/rank.py          DuckDB ranker (+ test_rank.py, requirements.txt)
internal/model/         chains, tokens, corridors, the quote row, cost_bps
internal/prices/        Binance + OKX snapshot
internal/sources/       Source interface, Jupiter and LI.FI adapters
internal/collector/     round loop, token buckets, backoff, circuit breaker
internal/sink/          partitioned Parquet writer
internal/config/        env + .env loading
internal/rankings/      latest.json types, in-memory store with hot reload
internal/api/           HTTP handlers, auth, health
scripts/inspect.sql     DuckDB queries over the raw quotes
```

## Next

4. More sources: Relay, deBridge, Mayan, CCTP fees, 0x/KyberSwap for Base swaps.
5. Production: Docker images, deploy with a persistent volume, copy Parquet to R2, daily compaction, Better Stack heartbeat and alerts.
6. FluxPay feeds its executed swaps back in, so rankings move from quoted output to delivered output.