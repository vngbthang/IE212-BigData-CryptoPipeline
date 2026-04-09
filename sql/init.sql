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
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (symbol, timestamp)
) 
PARTITION BY RANGE (CAST(timestamp AS DATE))
WITH (fillfactor=90);  -- Reserve 10% space for HOT updates

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
CREATE UNIQUE INDEX IF NOT EXISTS idx_symbol_timestamp_desc 
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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (symbol, timestamp)
)
PARTITION BY RANGE (CAST(timestamp AS DATE))
WITH (fillfactor=90);

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
    timestamp TIMESTAMP,
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
    timestamp TIMESTAMP,
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
-- 10. GRANTS (User permissions)
-- ============================================================================
GRANT ALL PRIVILEGES ON SCHEMA gold TO crypto_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA gold TO crypto_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA gold TO crypto_user;
GRANT USAGE ON TYPE price_direction TO crypto_user;
GRANT USAGE ON TYPE data_quality_status TO crypto_user;

-- ============================================================================
-- 11. COMMENTS (Documentation)
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
-- 12. SAMPLE DATA (For testing - will be loaded via Spark streaming)
-- ============================================================================
-- Data will be inserted via Spark streaming producer
-- This section is left empty as production data comes from Kafka → Spark

COMMIT;
