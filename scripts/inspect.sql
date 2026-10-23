-- Quick looks at what the collector has written. Run from the repo root:
--   duckdb -c ".read scripts/inspect.sql"
-- or paste a query into `duckdb` / Python's duckdb.sql(...).
-- This is a preview, not the ranker: that is step 2.

-- 1. Volume and health per source
SELECT source,
       count(*)                              AS attempts,
       round(avg(quote_ok::INT), 3)          AS availability,
       count(DISTINCT poll_round)            AS rounds,
       median(latency_ms)                    AS median_latency_ms,
       max(ts)                               AS last_quote
FROM read_parquet('data/quotes/**/*.parquet', hive_partitioning = true)
GROUP BY source
ORDER BY source;

-- 2. Failures by code
SELECT source, error_code, count(*) AS n, any_value(error_detail) AS example
FROM read_parquet('data/quotes/**/*.parquet', hive_partitioning = true)
WHERE NOT quote_ok
GROUP BY ALL
ORDER BY n DESC;

-- 3. Cost by corridor, size and route (the design doc's ranking, ungated)
WITH q AS (
  SELECT * FROM read_parquet('data/quotes/**/*.parquet', hive_partitioning = true)
  WHERE ts > now() - INTERVAL 7 DAY
),
ok AS (
  SELECT *, RANK() OVER (PARTITION BY corridor, size_bucket, poll_round ORDER BY cost_bps) AS rnk
  FROM q WHERE quote_ok
),
avail AS (
  SELECT corridor, size_bucket, source, avg(quote_ok::INT) AS availability FROM q GROUP BY ALL
)
SELECT ok.corridor, ok.size_bucket, ok.source,
       round(median(cost_bps), 2)              AS median_bps,
       round(quantile_cont(cost_bps, 0.9), 2)  AS p90_bps,
       round(avg((rnk = 1)::INT), 2)           AS win_rate,
       round(any_value(a.availability), 2)     AS availability,
       median(est_seconds)                     AS median_seconds,
       count(*)                                AS samples
FROM ok JOIN avail a USING (corridor, size_bucket, source)
GROUP BY ALL
ORDER BY corridor, size_bucket, median_bps;

-- 4. Which bridge/DEX each aggregator actually picked
SELECT corridor, source, route_label, count(*) AS n, round(median(cost_bps), 2) AS median_bps
FROM read_parquet('data/quotes/**/*.parquet', hive_partitioning = true)
WHERE quote_ok
GROUP BY ALL
ORDER BY corridor, n DESC;
