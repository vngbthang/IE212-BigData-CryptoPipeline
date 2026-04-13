-- ============================================================================
-- POSTGRESQL DDL SCRIPT - GOLD LAYER SCHEMA
-- Partitioned Tables + Composite Indexes for Real-time Crypto Pipeline
-- ============================================================================

-- ============================================================================
-- 1. CREATE SCHEMA
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS gold AUTHORIZATION crypto_user;
SET search_path TO gold, public;

-- ============================================================================
-- 2. ENUM TYPES
-- ============================================================================
CREATE TYPE price_direction AS ENUM ('UP', 'DOWN', 'NEUTRAL');
CREATE TYPE data_quality_status AS ENUM ('VALID', 'SUSPICIOUS', 'INVALID');

-- ============================================================================
-- 3. MAIN TABLE: gold_crypto_ohlcv (Partitioned by DATE)
-- ============================================================================
-- Parent table (range partitioned by date for retention policies)
CREATE TABLE IF NOT EXISTS gold.gold_crypto_ohlcv (
    timestamp TIMESTAMP NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    exchange VARCHAR(50) DEFAULT 'coinbase',
    
    -- OHLCV values
    open NUMERIC(20, 8) NOT NULL,
    high NUMERIC(20, 8) NOT NULL,
    low NUMERIC(20, 8) NOT NULL,
    close NUMERIC(20, 8) NOT NULL,
    volume NUMERIC(20, 8) NOT NULL,
    
    -- Technical Indicators (computed in Spark, stored here)
    ma_5m NUMERIC(20, 8),
    ma_20m NUMERIC(20, 8),
    rsi_14 NUMERIC(5, 2),
    volatility NUMERIC(10, 6),
    
    -- Predicted direction (from ML model)
    predicted_direction price_direction,
    confidence_score NUMERIC(5, 4),
    model_version VARCHAR(20),
    
    -- Data quality tracking
    quality_status data_quality_status DEFAULT 'VALID',
    data_source VARCHAR(50) DEFAULT 'kafka',
    
    -- [NEW] Observability: Latency & Lineage Tracking
    ingest_timestamp TIMESTAMPTZ NOT NULL,  -- When first tick arrived at Kafka
    process_timestamp TIMESTAMPTZ NOT NULL,  -- When Spark processed this batch
    display_timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,  -- When inserted to DB
    record_count INTEGER NOT NULL DEFAULT 1,  -- How many raw ticks in this 1-min bucket
    error_flag BOOLEAN DEFAULT FALSE,  -- TRUE if any record malformed
    error_messages TEXT,  -- Comma-separated error codes
    source_partition INTEGER,  -- Kafka partition source
    source_offset BIGINT,  -- Kafka offset for traceability
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) 
PARTITION BY RANGE (CAST(timestamp AS DATE));

-- ============================================================================
-- 3.1 CREATE WEEKLY PARTITIONS (Example: 4 weeks)
-- ============================================================================
-- Week 1
CREATE TABLE IF NOT EXISTS gold.gold_crypto_ohlcv_2026_04_01 
PARTITION OF gold.gold_crypto_ohlcv
FOR VALUES FROM ('2026-04-01') TO ('2026-04-08');

-- Week 2
CREATE TABLE IF NOT EXISTS gold.gold_crypto_ohlcv_2026_04_08 
PARTITION OF gold.gold_crypto_ohlcv
FOR VALUES FROM ('2026-04-08') TO ('2026-04-15');

-- Week 3
CREATE TABLE IF NOT EXISTS gold.gold_crypto_ohlcv_2026_04_15 
PARTITION OF gold.gold_crypto_ohlcv
FOR VALUES FROM ('2026-04-15') TO ('2026-04-22');

-- Week 4
CREATE TABLE IF NOT EXISTS gold.gold_crypto_ohlcv_2026_04_22 
PARTITION OF gold.gold_crypto_ohlcv
FOR VALUES FROM ('2026-04-22') TO ('2026-04-29');

-- ============================================================================
-- 3.2 CREATE COMPOSITE INDEXES (Critical for query performance)
-- ============================================================================
-- ⭐ PRIMARY COMPOSITE INDEX: (symbol, timestamp DESC)
-- Used for: Time-series queries, range scans, DISTINCT ON queries
CREATE INDEX IF NOT EXISTS idx_symbol_timestamp_desc 
ON gold.gold_crypto_ohlcv (symbol, timestamp DESC);

-- ⭐ SECONDARY COMPOSITE INDEX: (timestamp DESC, symbol)
-- Used for: Recent data queries across all symbols
CREATE INDEX IF NOT EXISTS idx_timestamp_symbol_desc 
ON gold.gold_crypto_ohlcv (timestamp DESC, symbol);

-- Single column indexes for filtering
CREATE INDEX IF NOT EXISTS idx_symbol 
ON gold.gold_crypto_ohlcv (symbol);

CREATE INDEX IF NOT EXISTS idx_timestamp 
ON gold.gold_crypto_ohlcv (timestamp);

-- ⭐ PARTIAL INDEX: For quality filtering (VALID records only)
CREATE INDEX IF NOT EXISTS idx_quality_valid 
ON gold.gold_crypto_ohlcv (timestamp DESC, symbol) 
WHERE quality_status = 'VALID';

-- ⭐ PARTIAL INDEX: For prediction queries (non-null predictions only)
CREATE INDEX IF NOT EXISTS idx_predictions 
ON gold.gold_crypto_ohlcv (timestamp DESC, symbol, predicted_direction)
WHERE predicted_direction IS NOT NULL;

-- [NEW] INDEXES FOR OBSERVABILITY & LATENCY TRACKING
-- ⭐ LATENCY TRACKING INDEX: For KPI queries on ingest-to-display latency
CREATE INDEX IF NOT EXISTS idx_latency_ingest_timestamp 
ON gold.gold_crypto_ohlcv (ingest_timestamp DESC);

-- ⭐ ERROR FILTERING INDEX: For data quality monitoring
CREATE INDEX IF NOT EXISTS idx_error_flag 
ON gold.gold_crypto_ohlcv (error_flag) 
WHERE error_flag = TRUE;

-- ⭐ DISPLAY TIMESTAMP INDEX: For recent data queries (dashboard refresh)
CREATE INDEX IF NOT EXISTS idx_display_timestamp 
ON gold.gold_crypto_ohlcv (display_timestamp DESC);

-- ============================================================================
-- 4. PREDICTIONS TABLE (Separate table for ML predictions)
-- ============================================================================
CREATE TABLE IF NOT EXISTS gold.gold_predictions (
    timestamp TIMESTAMP NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    
    -- Prediction fields
    predicted_direction price_direction NOT NULL,
    confidence_score NUMERIC(5, 4) NOT NULL,
    
    -- Model metadata
    model_version VARCHAR(20) NOT NULL,
    model_trained_at TIMESTAMP,
    feature_importance JSONB,  -- Store feature importance scores
    
    -- Data provenance
    prediction_source VARCHAR(50) DEFAULT 'pandas_udf',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PARTITION BY RANGE (CAST(timestamp AS DATE));

-- Partitions for predictions table (same as ohlcv)
CREATE TABLE IF NOT EXISTS gold.gold_predictions_2026_04_01 
PARTITION OF gold.gold_predictions
FOR VALUES FROM ('2026-04-01') TO ('2026-04-08');

CREATE TABLE IF NOT EXISTS gold.gold_predictions_2026_04_08 
PARTITION OF gold.gold_predictions
FOR VALUES FROM ('2026-04-08') TO ('2026-04-15');

CREATE TABLE IF NOT EXISTS gold.gold_predictions_2026_04_15 
PARTITION OF gold.gold_predictions
FOR VALUES FROM ('2026-04-15') TO ('2026-04-22');

CREATE TABLE IF NOT EXISTS gold.gold_predictions_2026_04_22 
PARTITION OF gold.gold_predictions
FOR VALUES FROM ('2026-04-22') TO ('2026-04-29');

-- Composite indexes for predictions
CREATE INDEX IF NOT EXISTS idx_pred_symbol_timestamp 
ON gold.gold_predictions (symbol, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_pred_direction 
ON gold.gold_predictions (predicted_direction, timestamp DESC);

-- ============================================================================
-- 5. DAILY AGGREGATES TABLE (Pre-aggregated data for dashboard)
-- ============================================================================
CREATE TABLE IF NOT EXISTS gold.gold_daily_aggregates (
    date DATE NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    
    -- Daily OHLCV
    open NUMERIC(20, 8) NOT NULL,
    high NUMERIC(20, 8) NOT NULL,
    low NUMERIC(20, 8) NOT NULL,
    close NUMERIC(20, 8) NOT NULL,
    volume NUMERIC(20, 8) NOT NULL,
    
    -- Daily statistics
    num_ticks BIGINT,
    avg_price NUMERIC(20, 8),
    volatility NUMERIC(10, 6),
    price_range NUMERIC(20, 8),  -- high - low
    
    -- Daily predictions
    bullish_ratio NUMERIC(5, 4),  -- % of UP predictions
    bearish_ratio NUMERIC(5, 4),  -- % of DOWN predictions
    avg_confidence NUMERIC(5, 4),
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (symbol, date)
);

-- Indexes for daily aggregates
CREATE INDEX IF NOT EXISTS idx_daily_symbol_date 
ON gold.gold_daily_aggregates (symbol, date DESC);

CREATE INDEX IF NOT EXISTS idx_daily_date 
ON gold.gold_daily_aggregates (date DESC);

-- ============================================================================
-- 6. HOURLY AGGREGATES TABLE (For faster dashboard queries)
-- ============================================================================
CREATE TABLE IF NOT EXISTS gold.gold_hourly_aggregates (
    hour_start TIMESTAMP NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    
    -- Hourly OHLCV
    open NUMERIC(20, 8) NOT NULL,
    high NUMERIC(20, 8) NOT NULL,
    low NUMERIC(20, 8) NOT NULL,
    close NUMERIC(20, 8) NOT NULL,
    volume NUMERIC(20, 8) NOT NULL,
    
    -- Statistics
    num_ticks BIGINT,
    avg_price NUMERIC(20, 8),
    volatility NUMERIC(10, 6),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (symbol, hour_start)
);

CREATE INDEX IF NOT EXISTS idx_hourly_symbol_hour 
ON gold.gold_hourly_aggregates (symbol, hour_start DESC);

-- ============================================================================
-- 7. DATA QUALITY & MONITORING TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS gold.gold_data_quality_metrics (
    measured_at TIMESTAMP NOT NULL PRIMARY KEY,
    symbol VARCHAR(20),
    
    -- Metrics
    total_records BIGINT,
    invalid_records BIGINT,
    null_count BIGINT,
    duplicate_count BIGINT,
    duplicate_ratio NUMERIC(5, 4),
    
    -- Time-series metrics
    latency_ms NUMERIC(10, 2),
    throughput_records_per_sec NUMERIC(10, 2),
    
    -- Model performance
    prediction_null_ratio NUMERIC(5, 4),
    avg_confidence NUMERIC(5, 4),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dq_measured_at 
ON gold.gold_data_quality_metrics (measured_at DESC);

-- ============================================================================
-- 8. VIEWS (For convenient querying)
-- ============================================================================

-- ⭐ View 1: Latest OHLCV for each symbol (DISTINCT ON pattern)
CREATE OR REPLACE VIEW gold.v_latest_ohlcv AS
SELECT DISTINCT ON (symbol)
    symbol,
    timestamp,
    open,
    high,
    low,
    close,
    volume,
    ma_5m,
    ma_20m,
    rsi_14,
    predicted_direction,
    confidence_score,
    created_at
FROM gold.gold_crypto_ohlcv
WHERE quality_status = 'VALID'
ORDER BY symbol, timestamp DESC;

-- ⭐ View 2: Recent predictions (last 24 hours)
CREATE OR REPLACE VIEW gold.v_recent_predictions_24h AS
SELECT
    symbol,
    timestamp,
    predicted_direction,
    confidence_score,
    model_version
FROM gold.gold_predictions
WHERE timestamp > CURRENT_TIMESTAMP - INTERVAL '24 hours'
  AND confidence_score > 0.5
ORDER BY timestamp DESC;

-- ⭐ View 3: Daily performance summary
CREATE OR REPLACE VIEW gold.v_daily_summary AS
SELECT
    date,
    symbol,
    open,
    close,
    ((close - open) / open) * 100 AS daily_return_pct,
    high - low AS price_range,
    volume,
    volatility,
    avg_confidence,
    bullish_ratio,
    bearish_ratio
FROM gold.gold_daily_aggregates
ORDER BY date DESC, symbol;

-- ============================================================================
-- 9. FUNCTIONS (For data maintenance & queries)
-- ============================================================================

-- ⭐ Function: Get OHLCV for a date range (optimized for partitions)
CREATE OR REPLACE FUNCTION gold.get_ohlcv_range(
    p_symbol VARCHAR,
    p_start_date DATE,
    p_end_date DATE
)
RETURNS TABLE (
    "timestamp" TIMESTAMP,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume NUMERIC,
    ma_5m NUMERIC,
    ma_20m NUMERIC,
    rsi_14 NUMERIC
) AS $$
BEGIN
  RETURN QUERY
  SELECT 
    t.timestamp,
    t.open,
    t.high,
    t.low,
    t.close,
    t.volume,
    t.ma_5m,
    t.ma_20m,
    t.rsi_14
  FROM gold.gold_crypto_ohlcv t
  WHERE t.symbol = p_symbol
    AND CAST(t.timestamp AS DATE) BETWEEN p_start_date AND p_end_date
    AND t.quality_status = 'VALID'
  ORDER BY t.timestamp ASC;
END;
$$ LANGUAGE plpgsql;

-- ⭐ Function: Calculate SMA (Simple Moving Average) from OHLCV data
CREATE OR REPLACE FUNCTION gold.calculate_sma(
    p_symbol VARCHAR,
    p_window INT,
    p_days INT DEFAULT 30
)
RETURNS TABLE (
    "timestamp" TIMESTAMP,
    sma NUMERIC
) AS $$
BEGIN
  RETURN QUERY
  WITH ranked_data AS (
    SELECT
      timestamp,
      close,
      ROW_NUMBER() OVER (ORDER BY timestamp ASC) AS rn
    FROM gold.gold_crypto_ohlcv
    WHERE symbol = p_symbol
      AND timestamp > CURRENT_TIMESTAMP - (p_days || ' days')::INTERVAL
      AND quality_status = 'VALID'
  )
  SELECT
    t.timestamp,
    AVG(t.close) OVER (
      ORDER BY t.rn 
      ROWS BETWEEN p_window - 1 PRECEDING AND CURRENT ROW
    ) AS sma
  FROM ranked_data t
  WHERE t.rn > p_window - 1
  ORDER BY t.timestamp DESC;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- 10. KPI & MONITORING FUNCTIONS (Phase 3 - New)
-- ============================================================================

-- KPI 1: Records per minute (last 5 minutes) - for throughput monitoring
CREATE OR REPLACE FUNCTION gold.get_throughput_last_5min() 
RETURNS TABLE(
    minute TIMESTAMP,
    record_count BIGINT,
    avg_latency_ms NUMERIC,
    records_per_sec NUMERIC
) AS $$
SELECT 
    DATE_TRUNC('minute', timestamp) AS minute,
    COUNT(*) as record_count,
    ROUND(AVG(EXTRACT(EPOCH FROM (display_timestamp - ingest_timestamp)) * 1000)::NUMERIC, 2) AS avg_latency_ms,
    ROUND((COUNT(*)::NUMERIC / 60), 2) AS records_per_sec
FROM gold.gold_crypto_ohlcv
WHERE timestamp >= NOW() - INTERVAL '5 minutes'
GROUP BY DATE_TRUNC('minute', timestamp)
ORDER BY minute DESC;
$$ LANGUAGE SQL;

-- KPI 2: Data quality metrics (last 1 hour)
CREATE OR REPLACE FUNCTION gold.get_data_quality_metrics() 
RETURNS TABLE(
    total_records BIGINT,
    valid_records BIGINT,
    error_records BIGINT,
    null_price_count BIGINT,
    null_size_count BIGINT,
    error_rate NUMERIC,
    valid_rate NUMERIC
) AS $$
SELECT 
    COUNT(*) AS total_records,
    COUNT(*) FILTER (WHERE error_flag = FALSE) AS valid_records,
    COUNT(*) FILTER (WHERE error_flag = TRUE) AS error_records,
    COUNT(*) FILTER (WHERE close IS NULL) AS null_price_count,
    COUNT(*) FILTER (WHERE volume IS NULL) AS null_size_count,
    ROUND((COUNT(*) FILTER (WHERE error_flag = TRUE)::NUMERIC / NULLIF(COUNT(*), 0)) * 100, 2) AS error_rate,
    ROUND((COUNT(*) FILTER (WHERE error_flag = FALSE)::NUMERIC / NULLIF(COUNT(*), 0)) * 100, 2) AS valid_rate
FROM gold.gold_crypto_ohlcv
WHERE timestamp >= NOW() - INTERVAL '1 hour';
$$ LANGUAGE SQL;

-- KPI 3: Ingest-to-display latency percentiles (last 1 hour)
CREATE OR REPLACE FUNCTION gold.get_latency_percentiles() 
RETURNS TABLE(
    p50_ms NUMERIC,
    p75_ms NUMERIC,
    p95_ms NUMERIC,
    p99_ms NUMERIC,
    max_ms NUMERIC,
    min_ms NUMERIC,
    avg_ms NUMERIC
) AS $$
SELECT
    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (display_timestamp - ingest_timestamp)) * 1000)::NUMERIC, 2) AS p50_ms,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (display_timestamp - ingest_timestamp)) * 1000)::NUMERIC, 2) AS p75_ms,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (display_timestamp - ingest_timestamp)) * 1000)::NUMERIC, 2) AS p95_ms,
    ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (display_timestamp - ingest_timestamp)) * 1000)::NUMERIC, 2) AS p99_ms,
    ROUND(MAX(EXTRACT(EPOCH FROM (display_timestamp - ingest_timestamp)) * 1000)::NUMERIC, 2) AS max_ms,
    ROUND(MIN(EXTRACT(EPOCH FROM (display_timestamp - ingest_timestamp)) * 1000)::NUMERIC, 2) AS min_ms,
    ROUND(AVG(EXTRACT(EPOCH FROM (display_timestamp - ingest_timestamp)) * 1000)::NUMERIC, 2) AS avg_ms
FROM gold.gold_crypto_ohlcv
WHERE timestamp >= NOW() - INTERVAL '1 hour'
  AND error_flag = FALSE;
$$ LANGUAGE SQL;

-- KPI 4: Data freshness (seconds since last update)
CREATE OR REPLACE FUNCTION gold.get_data_freshness() 
RETURNS TABLE(
    freshness_sec NUMERIC,
    last_update_timestamp TIMESTAMP,
    first_record_timestamp TIMESTAMP,
    total_records_hour BIGINT
) AS $$
SELECT 
    ROUND(EXTRACT(EPOCH FROM (NOW() - MAX(timestamp)))::NUMERIC, 1) AS freshness_sec,
    MAX(timestamp) AS last_update_timestamp,
    MIN(timestamp) AS first_record_timestamp,
    COUNT(*) AS total_records_hour
FROM gold.gold_crypto_ohlcv
WHERE timestamp >= NOW() - INTERVAL '1 hour';
$$ LANGUAGE SQL;

-- KPI 5: Per-symbol performance metrics
CREATE OR REPLACE FUNCTION gold.get_symbol_performance(p_symbol VARCHAR DEFAULT NULL) 
RETURNS TABLE(
    symbol VARCHAR,
    record_count BIGINT,
    avg_price NUMERIC,
    current_price NUMERIC,
    high_price NUMERIC,
    low_price NUMERIC,
    price_change_pct NUMERIC,
    avg_volume NUMERIC,
    avg_latency_ms NUMERIC
) AS $$
SELECT 
    symbol,
    COUNT(*) as record_count,
    ROUND(AVG(close)::NUMERIC, 8) as avg_price,
    (array_agg(close ORDER BY timestamp DESC))[1] as current_price,
    MAX(high) as high_price,
    MIN(low) as low_price,
    ROUND(((array_agg(close ORDER BY timestamp DESC))[1] - (array_agg(close ORDER BY timestamp ASC))[1]) / 
           (array_agg(close ORDER BY timestamp ASC))[1] * 100, 2) as price_change_pct,
    ROUND(AVG(volume)::NUMERIC, 8) as avg_volume,
    ROUND(AVG(EXTRACT(EPOCH FROM (display_timestamp - ingest_timestamp)) * 1000)::NUMERIC, 2) as avg_latency_ms
FROM gold.gold_crypto_ohlcv
WHERE (p_symbol IS NULL OR symbol = p_symbol)
  AND timestamp >= NOW() - INTERVAL '1 hour'
GROUP BY symbol
ORDER BY symbol;
$$ LANGUAGE SQL;

-- ============================================================================
-- 11. GRANTS (User permissions)
-- ============================================================================
GRANT ALL PRIVILEGES ON SCHEMA gold TO crypto_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA gold TO crypto_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA gold TO crypto_user;
GRANT USAGE ON TYPE price_direction TO crypto_user;
GRANT USAGE ON TYPE data_quality_status TO crypto_user;

-- Grant execute on all functions
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA gold TO crypto_user;

-- ============================================================================
-- 12. COMMENTS (Documentation)
-- ============================================================================
COMMENT ON TABLE gold.gold_crypto_ohlcv IS 
'Partitioned table storing 1-minute OHLCV candlesticks for crypto trading pairs. 
Partitioned by DATE for efficient retention policies and archival.
Includes technical indicators and ML predictions.';

COMMENT ON TABLE gold.gold_predictions IS 
'ML model predictions for price direction (UP/DOWN/NEUTRAL) with confidence scores.
Updated every minute based on latest data.';

COMMENT ON TABLE gold.gold_daily_aggregates IS 
'Pre-aggregated daily OHLCV and statistics. Optimized for dashboard queries.
Computed nightly via Airflow DAG.';

COMMENT ON INDEX gold.idx_symbol_timestamp_desc IS 
'⭐ PRIMARY COMPOSITE INDEX. Used for time-series queries and DISTINCT ON patterns.
Critical for dashboard performance.';

COMMENT ON INDEX gold.idx_predictions IS 
'⭐ PARTIAL INDEX. Indexes only rows where predictions exist (not null).
Speeds up prediction-based queries significantly.';

-- ============================================================================
-- 13. SAMPLE DATA (For testing - will be loaded via Spark streaming)
-- ============================================================================
-- Data will be inserted via Spark streaming producer
-- This section is left empty as production data comes from Kafka → Spark

COMMIT;
