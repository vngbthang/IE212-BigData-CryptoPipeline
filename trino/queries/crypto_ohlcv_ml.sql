-- Trino SQL: ML feature computation on the crypto_ohlcv Iceberg table
-- Note: Views are not supported on Iceberg/Nessie catalogs in Trino.
-- Instead, ML features are computed in Python (see src/dashboard/app.py).
-- These SQL queries can be used for ad-hoc analysis or to backfill features.

-- Query A: ad-hoc ML feature computation from raw OHLCV
-- Run this in Trino CLI:  docker exec trino trino --catalog iceberg --schema gold
SELECT
    symbol,
    window_start,
    window_end,
    open,
    high,
    low,
    close,
    volume,
    tick_count,
    LN(close / LAG(close, 1) OVER w)                       AS log_return,
    STDDEV(close) OVER w30                                 AS volatility_30m,
    (close - AVG(close) OVER w30) / NULLIF(STDDEV(close) OVER w30, 0) AS z_score,
    ingestion_time
FROM iceberg.gold.crypto_ohlcv
WINDOW
    w  AS (PARTITION BY symbol ORDER BY window_start),
    w30 AS (PARTITION BY symbol ORDER BY window_start ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
ORDER BY symbol, window_start;
