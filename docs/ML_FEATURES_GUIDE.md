# ML Features Guide: Crypto Pipeline Gold Layer

---

## Table of Contents

1. [Overview](#overview)
2. [Feature Catalog](#feature-catalog)
3. [Data Integrity & Filtering](#data-integrity--filtering)
4. [Implementation Guide](#implementation-guide)
5. [Streamlit Monitoring & ML Consumption](#streamlit-monitoring--ml-consumption)
6. [Pipeline Data Inventory](#pipeline-data-inventory)
7. [Best Practices & Troubleshooting](#best-practices--troubleshooting)

---

## Overview

The **Gold Layer** (`gold.gold_crypto_ohlcv`) in our Medallion Architecture serves as the feature store for machine learning models and operational monitoring. All data is aggregated into 1-minute OHLCV candles with engineered features computed in real-time via Apache Spark Structured Streaming, and the current project exposes the results through a Streamlit dashboard rather than a dedicated FastAPI or Next.js serving layer.

### Key Characteristics

| Aspect | Details |
|--------|---------|
| **Update Frequency** | Every 1 minute (streaming micro-batches) |
| **Latency** | < 2 seconds end-to-end (Kafka → Spark → Postgres) |
| **Data Retention** | 60 days (partition-based retention via Airflow DAG) |
| **Historical Records** | Continuously growing within the active 60-day retention window |
| **Granularity** | 1-minute candles (OHLCV aggregation) |
| **Availability** | 24/7 (real-time streaming pipeline) |

---

## Feature Catalog

### A. Core OHLCV Features

#### 1. **Open** (`open`)

- **Definition**: The price of the first trade in the 1-minute window.
- **Formula**: $\text{open} = \text{first price in window}$
- **Data Type**: `NUMERIC(20, 8)` (Decimal)
- **ML Utility**: 
  - Entry point for momentum-based models
  - Component of daily/hourly opening gap analysis
  - Used in mean-reversion strategies
- **Calculation Context**: Extracted from first tick in each 1-minute window during Spark aggregation
- **Typical Range**: 30,000 - 100,000 USD (for BTC-USD)

#### 2. **High** (`high`)

- **Definition**: The maximum price recorded in the 1-minute window.
- **Formula**: $\text{high} = \max(\text{prices in window})$
- **Data Type**: `NUMERIC(20, 8)` (Decimal)
- **ML Utility**:
  - Identifies support/resistance levels
  - Intraday volatility indicator
  - Used in range-based trading models
- **Calculation Context**: Aggregated using Spark's `max()` function over all ticks in the 1-minute bucket
- **Typical Range**: Slightly above `close` in bullish candles

#### 3. **Low** (`low`)

- **Definition**: The minimum price recorded in the 1-minute window.
- **Formula**: $\text{low} = \min(\text{prices in window})$
- **Data Type**: `NUMERIC(20, 8)` (Decimal)
- **ML Utility**:
  - Identifies support/resistance levels
  - Price floor for risk assessment
  - Used in swing trading models
- **Calculation Context**: Aggregated using Spark's `min()` function
- **Typical Range**: Slightly below `close` in bearish candles

#### 4. **Close** (`close`)

- **Definition**: The price of the last trade in the 1-minute window.
- **Formula**: $\text{close} = \text{last price in window}$
- **Data Type**: `NUMERIC(20, 8)` (Decimal)
- **ML Utility**:
  - Primary target variable for price prediction
  - Used in all technical indicators
  - Reference point for returns calculations
- **Calculation Context**: Extracted from last tick in each 1-minute window
- **Typical Range**: 30,000 - 100,000 USD (for BTC-USD)

#### 5. **Volume** (`volume`)

- **Definition**: Total base asset volume traded during the 1-minute window.
- **Formula**: $\text{volume} = \sum(\text{size of all trades in window})$
- **Data Type**: `NUMERIC(20, 8)` (Decimal)
- **ML Utility**:
  - Confirms price movements (volume-price correlation)
  - Identifies breakouts vs. fakeouts
  - Used in trend-following models
  - Measure of market activity/liquidity
- **Calculation Context**: Accumulated sum using Spark's `sum()` function
- **Typical Range**: 50 - 500 BTC per minute (depends on market conditions)

---

### B. Engineered Features (Real-time Computation)

#### 6. **Log Returns** (`log_returns`)

- **Definition**: Natural logarithm of the price ratio (current close / previous close).
- **Formula**: 
$$\text{log\_returns}_t = \ln\left(\frac{\text{close}_t}{\text{close}_{t-1}}\right)$$
- **Data Type**: `DOUBLE PRECISION` (Float)
- **ML Utility**:
  - **Stationarity**: Log returns are stationary, unlike raw prices (critical for ARIMA/VAR models)
  - **Normality**: Better approximation of normal distribution than simple returns
  - **Aggregation**: Additive across time periods (daily returns = sum of minute returns)
  - **Scale Invariance**: Percentage changes independent of price level
- **Null Handling**: NULL when `close_t ≤ 0` or `close_{t-1} ≤ 0` or previous close is missing
- **Calculation Context**: Computed in Spark using `lag()` window function over symbols ordered by timestamp
- **Typical Range**: -0.05 to +0.05 (represents -5% to +5% per minute)
- **Best Practice**: Always use log returns for statistical modeling, not simple returns

#### 7. **Volatility (30-minute rolling)** (`volatility_30m`)

- **Definition**: Standard deviation of close prices over the last 30 minutes (rolling window).
- **Formula**:
$$\sigma_{30m,t} = \sqrt{\frac{1}{N-1}\sum_{i=0}^{N-1}(\text{close}_{t-i} - \bar{\text{close}})^2}$$
where $N = 30$ and $\bar{\text{close}}$ is the mean of the 30 prices.
- **Data Type**: `DOUBLE PRECISION` (Float)
- **ML Utility**:
  - **Risk Metric**: Higher volatility = higher risk; used in portfolio optimization
  - **Regime Identification**: Sharp volatility spikes indicate market regime changes
  - **Feature Scaling**: Normalize returns by dividing by volatility (risk-adjusted returns)
  - **Model Input**: Direct feature for volatility prediction models (GARCH, Exponential Smoothing)
- **Null Handling**: NULL when fewer than 2 valid prices in the 30-minute window
- **Calculation Context**: Rolling standard deviation computed in Spark using `stddev_samp()` over a window of 30 rows (minutes)
- **Typical Range**: 0.001 - 0.05 (0.1% - 5% per minute)
- **Interpretation**: 
  - < 0.005: Low volatility (trending, stable)
  - 0.005 - 0.02: Normal volatility
  - > 0.02: High volatility (explosive moves, breakouts)

#### 8. **Z-Score (Normalized Price)** (`z_score`)

- **Definition**: Number of standard deviations a price is from the 30-minute rolling mean.
- **Formula**:
$$z_t = \frac{\text{close}_t - \mu_{30m}}{\sigma_{30m}}$$
where $\mu_{30m}$ is the 30-minute rolling mean and $\sigma_{30m}$ is the 30-minute rolling volatility.
- **Data Type**: `DOUBLE PRECISION` (Float)
- **ML Utility**:
  - **Anomaly Detection**: Identifies extreme price moves (typically $|z| > 2$ is anomalous)
  - **Mean Reversion**: High z-scores suggest oversold/overbought conditions
  - **Feature Normalization**: Standardized feature for ML models (zero mean, unit variance)
  - **Statistical Testing**: Enables hypothesis testing on price extremes
- **Null Handling**: NULL when volatility ≤ 0 (no variance) or rolling mean is NULL
- **Calculation Context**: Computed in Spark after both rolling mean and volatility are available
- **Typical Range**: -3 to +3 (normal market), > ±3 is rare and anomalous
- **Interpretation**:
  - $|z| < 1$: Close to mean (typical price action)
  - $1 < |z| < 2$: Moderate deviation (watch for reversal)
  - $|z| > 2$: Strong deviation (potential anomaly)
  - $|z| > 3$: Extreme (flag for outlier detection)

#### 9. **Is Outlier** (`is_outlier`)

- **Definition**: Binary flag indicating whether the price is statistically anomalous (extreme z-score).
- **Formula**:
$$\text{is\_outlier} = \begin{cases} 
\text{TRUE} & \text{if } |z_{\text{score}}| \geq 3.0 \\
\text{FALSE} & \text{otherwise}
\end{cases}$$
- **Data Type**: `BOOLEAN`
- **ML Utility**:
  - **Data Cleaning**: Filter outliers when training price prediction models
  - **Alert Triggers**: Identify potential market manipulation or data errors
  - **Risk Monitoring**: Flag extreme market conditions for traders
  - **Robust Statistics**: Outliers can skew model training; removing them improves generalization
- **Default**: FALSE (conservative threshold at 3σ)
- **Calculation Context**: Binary result of comparing z-score magnitude in Spark
- **Typical Count**: < 1% of records (by design at 3σ threshold)
- **Best Practice**: Always exclude from training data unless building a robustness model

#### 10. **Record Count** (`record_count`)

- **Definition**: Number of raw ticks aggregated into this 1-minute candle.
- **Data Type**: `INTEGER`
- **ML Utility**:
  - **Data Quality**: Low record count = potential data gaps or sparse trading
  - **Weighting**: Can weight candles by record count (more ticks = more reliable)
  - **Activity Level**: Proxy for market microstructure and liquidity
  - **Model Confidence**: Higher record count typically means more stable OHLCV values
- **Typical Range**: 10 - 500 ticks per minute (depends on market activity)
- **Interpretation**:
  - < 5: Very sparse; use with caution
  - 5 - 50: Normal quiet periods
  - 50 - 200: Active trading
  - > 200: High-frequency trading period

### E. Additional Stored Columns in Gold

The table also stores a few project-specific columns that support lineage, dashboarding, and downstream analysis:

| Column | Purpose |
|--------|---------|
| `ma_5m` | 5-minute moving average of close price |
| `ma_20m` | 20-minute moving average of close price |
| `rsi_14` | 14-period RSI indicator |
| `volatility` | Legacy rolling volatility field used by older queries |
| `predicted_direction` | Directional label used by downstream experiments |
| `confidence_score` | Model or heuristic confidence attached to the candle |
| `model_version` | Version tag for the producing model or pipeline |
| `quality_status` | Human-readable quality state for the row |
| `data_source` | Source system or feed identifier |
| `created_at` | Row creation timestamp |
| `updated_at` | Row update timestamp |

---

## Pipeline Data Inventory

This section lists the non-ML and operational data that the pipeline consumes, produces, or tracks. These fields are useful for lineage, debugging, DLQ triage, freshness monitoring, and model governance even when they are not direct training features.

### A. Bronze / Kafka Tick Fields

These are the raw Coinbase tick fields that enter the stream processor before aggregation.

| Field | Where It Appears | Purpose |
|------|------------------|---------|
| `timestamp` | Kafka message / Bronze tick | Source event time from Coinbase |
| `product_id` | Kafka message / Bronze tick | Symbol identifier used as `symbol` downstream |
| `exchange` | Kafka message / Bronze tick | Source exchange name |
| `price` | Kafka message / Bronze tick | Raw trade price used for OHLCV aggregation |
| `size` | Kafka message / Bronze tick | Trade size used for volume aggregation |
| `bid` | Kafka message / Bronze tick | Best bid at tick time; available for enrichment or spread analysis |
| `ask` | Kafka message / Bronze tick | Best ask at tick time; available for enrichment or spread analysis |
| `side` | Kafka message / Bronze tick | Trade direction metadata (`buy` / `sell` / `unknown`) |
| `sequence` | Kafka message / Bronze tick | Source sequence number for replay/audit |
| `ingestion_timestamp` | Kafka message / Bronze tick | Event ingestion time from the producer |
| `partition` / `offset` | Kafka metadata | Partition and offset for lineage and replay |
| `kafka_timestamp` | Kafka metadata | Broker timestamp for the message |

### B. Validation and DLQ Metadata

These fields are produced by the stream processor when validating or rejecting records.

| Field | Meaning |
|------|---------|
| `deserialization_failed` | Avro payload could not be decoded |
| `error_flag` | Validation failed for the row |
| `error_messages` | Validation reason code(s) such as `NULL_PRICE` or `INVALID_SIZE` |
| `dlq_reason` | Normalized reason routed to the dead-letter queue |
| `dlq_payload` | Serialized payload sent to the DLQ topic |

### C. Operational / Observability Tables

These tables are important for production readiness and freshness checks, but they are not model features themselves.

| Table | Key Columns | Purpose |
|------|-------------|---------|
| `gold.pipeline_heartbeat` | `id`, `updated_at` | Single-row liveness marker for the streaming job |
| `gold.pipeline_latency_samples` | `measured_at`, `latency_ms` | Continuous latency measurements for freshness and SLA checks |
| `gold.pipeline_alerts` | `alert_type`, `severity`, `symbol`, `gap_start`, `gap_end`, `details` | Operational warnings such as missing minute windows or data gaps |

### D. Downstream ML-Ready Data Summary

For model training and inference, the primary usable dataset is still `gold.gold_crypto_ohlcv` plus the operational tables above. In practice:

- Use `gold.gold_crypto_ohlcv` for training features and inference features.
- Use `gold.pipeline_heartbeat` and `gold.pipeline_latency_samples` to verify freshness before trusting the dataset.
- Use `gold.pipeline_alerts` and `error_flag` / `error_messages` to exclude bad windows or investigate gaps.
- Use Kafka metadata and DLQ fields to debug upstream ingestion issues when features look stale or incomplete.

---

### C. Observability & Lineage Features

#### 11. **Ingest Timestamp** (`ingest_timestamp`)

- **Definition**: When the first tick of this candle arrived at Kafka (millisecond precision).
- **Data Type**: `TIMESTAMPTZ`
- **ML Utility**:
  - **Latency Calculation**: `process_timestamp - ingest_timestamp` = data processing latency
  - **Time Alignment**: Join with external data sources using this timestamp
  - **Causality Tracking**: Ensures correct temporal ordering for time-series models
- **Calculation Context**: Captured from Coinbase WebSocket message timestamp

#### 12. **Process Timestamp** (`process_timestamp`)

- **Definition**: When Spark completed aggregating this candle (micro-batch completion time).
- **Data Type**: `TIMESTAMPTZ`
- **ML Utility**:
  - **Real-time Alignment**: Use this for feature freshness metrics
  - **Batch Tracking**: Identify which batch a record belongs to
  - **Pipeline Monitoring**: Monitor Spark processing latency
- **Calculation Context**: Set by Spark when `foreachBatch()` sink writes to Postgres

#### 13. **Display Timestamp** (`display_timestamp`)

- **Definition**: When the record was inserted into Postgres (database write time).
- **Data Type**: `TIMESTAMPTZ`
- **ML Utility**:
  - **Freshness**: Current timestamp relative to display time = data freshness
  - **Query Performance**: Optimized for recent data queries (indexed)
- **Calculation Context**: Set by Postgres `clock_timestamp()` during insert

---

### D. Data Quality & Lineage

#### 14. **Error Flag** (`error_flag`)

- **Definition**: Boolean indicating whether this record failed validation.
- **Data Type**: `BOOLEAN`
- **ML Utility**:
  - **Data Quality Filter**: Exclude error records from training (use `error_flag = FALSE`)
  - **Monitoring**: Track validation failure rates over time
  - **Debugging**: Identify problematic data sources
- **Default**: FALSE
- **Related Column**: `error_messages` (contains reason for failure)

#### 15. **Error Messages** (`error_messages`)

- **Definition**: Comma-separated list of validation error codes if `error_flag = TRUE`.
- **Data Type**: `TEXT`
- **Possible Values**:
  - `AVRO_DESERIALIZATION_FAILED`: Could not parse Kafka Avro message
  - `NULL_PRICE`: Price field is NULL
  - `INVALID_PRICE`: Price ≤ 0
  - `NULL_SIZE`: Volume field is NULL
  - `INVALID_SIZE`: Volume < 0
  - `NULL_TIME`: Timestamp is NULL
- **ML Utility**: Understand why records were rejected; helps debug pipeline issues

#### 16. **Source Partition** (`source_partition`)

- **Definition**: Kafka partition number from which this record originated.
- **Data Type**: `INTEGER`
- **ML Utility**:
  - **Partitioning Strategy**: Understanding which Kafka partition produced data
  - **Distributed Training**: Ensure no partition bias in sampling
- **Typical Value**: 0 (single partition for `crypto_ticks` topic)

#### 17. **Source Offset** (`source_offset`)

- **Definition**: Kafka message offset (sequential ID) for traceability.
- **Data Type**: `BIGINT`
- **ML Utility**:
  - **Reproducibility**: Replay exact data by offset
  - **Audit Trail**: Track data lineage back to source
- **Typical Range**: 0 to billions (continuously increasing)

---

## Data Integrity & Filtering

### Recommended Filters for Training Data

#### ✅ **RECOMMENDED FILTER COMBINATION**

```sql
SELECT 
    timestamp, symbol, close, volume, log_returns, 
    volatility_30m, z_score, record_count
FROM gold.gold_crypto_ohlcv
WHERE 
    -- Data Quality Checks
    error_flag = FALSE                              -- Exclude validation failures
    AND is_outlier = FALSE                          -- Exclude statistical anomalies
    AND record_count >= 5                           -- Exclude sparse candles
    
    -- Temporal Constraints
    AND timestamp >= NOW() - INTERVAL '60 days'     -- Last 60 days of data
    AND timestamp <= NOW() - INTERVAL '1 day'       -- Exclude today (incomplete)
    
    -- Symbol Filtering
    AND symbol IN ('BTC-USD', 'ETH-USD')            -- Select specific symbols
    
    -- Feature Availability
    AND log_returns IS NOT NULL                     -- Require returns calculated
    AND volatility_30m IS NOT NULL                  -- Require volatility computed
    AND z_score IS NOT NULL                         -- Require z-score available
ORDER BY symbol, timestamp DESC;
```

### Why Each Filter Matters

| Filter | Reason | Impact if Ignored |
|--------|--------|-------------------|
| `error_flag = FALSE` | Prevents malformed data from training | Model learns from garbage data; poor generalization |
| `is_outlier = FALSE` | Removes statistical extremes (3σ) | Outliers skew gradients in neural networks; overfitting |
| `record_count >= 5` | Ensures minimum data density | Sparse candles = unreliable OHLCV; can bias volatility |
| `timestamp >= 60 days` | Limits to recent historical data | Old data may not reflect current market regime |
| `timestamp <= 1 day` | Excludes incomplete current candle | Current candle may still be receiving ticks |
| Feature NULL checks | Ensures all features are computable | NULLs propagate as errors in most ML frameworks |

### Alternative Scenarios

#### **For Volatility Prediction** (Aggressive)
```sql
-- Focus on high-activity periods with clear trends
WHERE 
    error_flag = FALSE
    AND record_count >= 50              -- Higher activity threshold
    AND volatility_30m BETWEEN 0.002 AND 0.05  -- Exclude eerily calm/chaotic markets
    AND timestamp >= NOW() - INTERVAL '90 days'
```

#### **For Robust Models** (Conservative)
```sql
-- Maximum data quality; accept fewer samples
WHERE 
    error_flag = FALSE
    AND is_outlier = FALSE
    AND record_count >= 100             -- Very high activity
    AND volatility_30m IS NOT NULL
    AND log_returns IS NOT NULL
    AND z_score BETWEEN -3 AND 3        -- Explicit z-score bounds
```

#### **For Real-time Inference** (No Historical Filter)
```sql
-- Latest candles only; used for live prediction
WHERE 
    error_flag = FALSE
    AND is_outlier = FALSE
    AND timestamp >= NOW() - INTERVAL '2 hours'    -- Recent sliding window
    AND symbol = 'BTC-USD'
ORDER BY timestamp DESC
LIMIT 100
```

---

## Implementation Guide

### Prerequisites

```bash
# Install required Python packages
pip install pandas sqlalchemy psycopg2-binary numpy scikit-learn
```

### Example 1: Fetch & Prepare Training Data

```python
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

# ============================================================================
# DATABASE CONNECTION
# ============================================================================

DB_CONNECTION_STRING = (
    "postgresql://crypto_user:crypto_password@localhost:5432/cryptopipeline"
)
engine = create_engine(DB_CONNECTION_STRING)

# ============================================================================
# FETCH LAST 10,000 RECORDS FOR A SYMBOL
# ============================================================================

def fetch_training_data(symbol: str, limit: int = 10000) -> pd.DataFrame:
    """
    Fetch cleaned training data for a specific crypto symbol.
    
    Args:
        symbol: Crypto symbol (e.g., 'BTC-USD', 'ETH-USD')
        limit: Maximum number of records to fetch
    
    Returns:
        pd.DataFrame: Training data with all features
    """
    query = f"""
    SELECT 
        timestamp,
        symbol,
        open,
        high,
        low,
        close,
        volume,
        log_returns,
        volatility_30m,
        z_score,
        record_count,
        process_timestamp
    FROM gold.gold_crypto_ohlcv
    WHERE 
        symbol = '{symbol}'
        AND error_flag = FALSE
        AND is_outlier = FALSE
        AND record_count >= 5
        AND timestamp >= NOW() - INTERVAL '60 days'
        AND timestamp <= NOW() - INTERVAL '1 day'
        AND log_returns IS NOT NULL
        AND volatility_30m IS NOT NULL
    ORDER BY timestamp ASC
    LIMIT {limit}
    """
    
    df = pd.read_sql(query, engine)
    
    # Convert timestamp to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    
    # Sort by time (important for time-series models)
    df = df.sort_values('timestamp').reset_index(drop=True)
    
    print(f"✓ Fetched {len(df)} records for {symbol}")
    print(f"  Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
    print(f"  Null values:\n{df.isnull().sum()}")
    
    return df

# Usage
df = fetch_training_data('BTC-USD', limit=10000)
```

### Example 2: Feature Engineering & Preprocessing

```python
from sklearn.preprocessing import StandardScaler
import numpy as np

def preprocess_features(df: pd.DataFrame) -> tuple:
    """
    Prepare features and target for ML model.
    
    Args:
        df: Raw training dataframe from fetch_training_data()
    
    Returns:
        X (np.ndarray): Feature matrix (scaled)
        y (np.ndarray): Target variable (next close price)
        scaler (StandardScaler): Fitted scaler for inference
    """
    
    # Feature selection
    feature_cols = [
        'close',
        'volume',
        'log_returns',
        'volatility_30m',
        'z_score',
        'record_count'
    ]
    
    X = df[feature_cols].copy()
    
    # Target: Next close price (for supervised learning)
    y = df['close'].shift(-1).iloc[:-1]  # Predict next close
    X = X.iloc[:-1]  # Remove last row (no target)
    
    # Handle any remaining NaNs (edge case)
    mask = ~(X.isnull().any(axis=1) | y.isnull())
    X = X[mask]
    y = y[mask]
    
    print(f"✓ Prepared {len(X)} training samples")
    
    # Standardize features (crucial for neural networks, SVM, etc.)
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    return X_scaled, y.values, scaler, feature_cols

# Usage
X, y, scaler, feature_cols = preprocess_features(df)
print(f"Feature matrix shape: {X.shape}")
print(f"Target shape: {y.shape}")
```

### Example 3: Train a Simple Volatility Prediction Model

```python
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score

def train_volatility_model(df: pd.DataFrame):
    """
    Train a RandomForest model to predict next-minute volatility.
    
    Args:
        df: Training dataframe
    
    Returns:
        model: Trained RandomForestRegressor
        metrics: Dict of performance metrics
    """
    
    # Features for volatility prediction
    feature_cols = [
        'volume',
        'log_returns',
        'volatility_30m',
        'z_score',
        'record_count'
    ]
    
    X = df[feature_cols].copy()
    
    # Target: Next volatility (30-minute rolling std)
    y = df['volatility_30m'].shift(-1).iloc[:-1]
    X = X.iloc[:-1]
    
    # Remove NaNs
    mask = ~(X.isnull().any(axis=1) | y.isnull())
    X = X[mask]
    y = y[mask]
    
    # Train-test split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, shuffle=False  # No shuffle for time-series!
    )
    
    # Scale features
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    # Train model
    model = RandomForestRegressor(
        n_estimators=100,
        max_depth=10,
        random_state=42,
        n_jobs=-1
    )
    model.fit(X_train_scaled, y_train)
    
    # Evaluate
    y_pred = model.predict(X_test_scaled)
    mse = mean_squared_error(y_test, y_pred)
    rmse = np.sqrt(mse)
    r2 = r2_score(y_test, y_pred)
    
    metrics = {
        'mse': mse,
        'rmse': rmse,
        'r2': r2,
        'train_size': len(X_train),
        'test_size': len(X_test)
    }
    
    print(f"✓ Model trained on {len(X_train)} samples")
    print(f"  RMSE: {rmse:.6f}")
    print(f"  R²: {r2:.4f}")
    print(f"\n  Feature importance:")
    for feat, imp in zip(feature_cols, model.feature_importances_):
        print(f"    {feat}: {imp:.4f}")
    
    return model, scaler, feature_cols, metrics

# Usage
df = fetch_training_data('BTC-USD', limit=5000)
model, scaler, features, metrics = train_volatility_model(df)
```

### Example 4: Real-time Inference on Latest Candle

```python
def predict_next_volatility(symbol: str, model, scaler, feature_cols):
    """
    Fetch the latest candle and predict next volatility.
    
    Args:
        symbol: Crypto symbol
        model: Trained RandomForestRegressor
        scaler: Fitted StandardScaler
        feature_cols: List of feature column names
    
    Returns:
        dict: Prediction result with confidence
    """
    
    # Fetch latest candle (last 2 minutes for rolling window)
    query = f"""
    SELECT 
        timestamp,
        close,
        volume,
        log_returns,
        volatility_30m,
        z_score,
        record_count
    FROM gold.gold_crypto_ohlcv
    WHERE 
        symbol = '{symbol}'
        AND error_flag = FALSE
        AND timestamp >= NOW() - INTERVAL '5 minutes'
    ORDER BY timestamp DESC
    LIMIT 1
    """
    
    latest = pd.read_sql(query, engine)
    
    if latest.empty:
        return {'error': 'No recent data available'}
    
    # Extract features
    X_latest = latest[feature_cols].values
    X_latest_scaled = scaler.transform(X_latest)
    
    # Predict
    prediction = model.predict(X_latest_scaled)[0]
    
    result = {
        'symbol': symbol,
        'timestamp': latest['timestamp'].iloc[0],
        'current_volatility_30m': float(latest['volatility_30m'].iloc[0]),
        'predicted_next_volatility': float(prediction),
        'volatility_change_pct': (
            (prediction - latest['volatility_30m'].iloc[0]) / 
            latest['volatility_30m'].iloc[0] * 100
        )
    }
    
    print(f"✓ Prediction for {symbol} at {result['timestamp']}")
    print(f"  Current volatility: {result['current_volatility_30m']:.6f}")
    print(f"  Predicted volatility: {result['predicted_next_volatility']:.6f}")
    print(f"  Expected change: {result['volatility_change_pct']:+.2f}%")
    
    return result

# Usage
prediction = predict_next_volatility('BTC-USD', model, scaler, features)
```

---

## Streamlit Monitoring & ML Consumption

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                   REAL-TIME INFERENCE PIPELINE                  │
└─────────────────────────────────────────────────────────────────┘

    ┌──────────────────┐
    │   Coinbase       │
    │   WebSocket      │
    │   (Tick Events)  │
    └────────┬─────────┘
             │
             ▼
    ┌──────────────────┐
    │  Apache Kafka    │
    │  crypto_ticks    │
    └────────┬─────────┘
             │
             ▼
    ┌──────────────────────────────────────────────┐
    │  Spark Structured Streaming (local[*])       │
    │  ├─ Avro Deserialization                    │
    │  ├─ Validation & Error Handling (DLQ)       │
    │  ├─ 1-minute OHLCV Aggregation              │
    │  ├─ Feature Engineering:                     │
    │  │  • log_returns = ln(close_t/close_t-1)   │
    │  │  • volatility_30m = std(close[-30:])     │
    │  │  • z_score = (close - μ) / σ              │
    │  │  • is_outlier = |z_score| >= 3           │
    │  └─ Upsert to Postgres (foreachBatch)       │
    └────────┬─────────────────────────────────────┘
             │
             ▼
    ┌──────────────────────────────────────────────┐
    │  PostgreSQL (Gold Layer Feature Store)       │
    │  gold.gold_crypto_ohlcv (1-min candles)     │
    │  Columns: OHLCV + features + metadata       │
    └────────┬─────────────────────────────────────┘
             │
             ▼ (Query latest candle + features)
    ┌──────────────────────────────────────────────┐
    │  Streamlit Dashboard (Python UI)             │
    │  Realtime, History, System Status panels     │
    └────────┬─────────────────────────────────────┘
             │
         ▼
    ┌──────────────────────────────────────────────┐
    │  Analyst / ML Consumer                       │
    │  Queries latest candles and trains models    │
    └──────────────────────────────────────────────┘
```

### Data Flow Stages

#### **Stage 1: Feature Computation (Spark)**
- Receive raw tick from Coinbase
- Aggregate 60 ticks → 1-minute candle
- Compute rolling features (30-minute windows)
- Calculate indicators (log_returns, volatility, z_score)
- Write to Postgres (< 500ms latency)

#### **Stage 2: Feature Store (Postgres)**
- Store latest candle with all features
- Index on `(symbol, timestamp DESC)` for fast retrieval
- Maintain 60-day retention (partition-based)
- Provide JDBC endpoint for Spark history queries

#### **Stage 3: Streamlit Monitoring Dashboard**

The current repository exposes the Gold layer through a Streamlit dashboard in [src/dashboard/app.py](../src/dashboard/app.py). The dashboard auto-refreshes every second, reads from PostgreSQL, and shows realtime candles, latency metrics, and system status for operators and ML users.

```python
import streamlit as st
import pandas as pd
import psycopg2

conn = psycopg2.connect(
    host="postgres",
    port=5432,
    database="cryptopipeline",
    user="crypto_user",
    password="crypto_password",
)

latest = pd.read_sql(
    """
    SELECT timestamp, symbol, close, log_returns, volatility_30m, z_score, is_outlier
    FROM gold.gold_crypto_ohlcv
    WHERE symbol = 'BTC-USD' AND error_flag = FALSE
    ORDER BY timestamp DESC
    LIMIT 20
    """,
    conn,
)

st.line_chart(latest.set_index("timestamp")["close"])
st.dataframe(latest)
```

#### **Stage 4: ML Training and Consumption**

For machine learning, consume the Gold table directly from Python notebooks or batch jobs. The recommended flow is to query clean rows from `gold.gold_crypto_ohlcv`, apply filters such as `error_flag = FALSE`, `is_outlier = FALSE`, and `record_count >= 5`, then train or score models offline.

### Latency SLA

| Component | Latency | Target | Status |
|-----------|---------|--------|--------|
| Coinbase → Kafka | 50-100ms | < 200ms | ✅ |
| Kafka → Spark | 100-200ms | < 500ms | ✅ |
| Spark Feature Engineering | 300-500ms | < 1s | ✅ |
| Spark → Postgres Write | 100-300ms | < 500ms | ✅ |
| **Total (Tick to DB)** | **550-1100ms** | **< 2s** | ✅ |
| Postgres Query | 50-100ms | < 200ms | ✅ |
| Streamlit Refresh + Render | 100-500ms | < 1s | ✅ |
| **Total (Tick to UI)** | **710-1700ms** | **< 3s** | ✅ |

---

## Best Practices & Troubleshooting

### Best Practices

#### ✅ **Data Preparation**

1. **Always filter outliers** when training supervised models
   ```python
   df_clean = df[df['is_outlier'] == False]  # Remove 3σ extremes
   ```

2. **Use log returns for time-series models**, not simple returns
   ```python
   # ✓ Correct: Log returns are stationary
   model.fit(X[['log_returns']], y)
   
   # ✗ Wrong: Simple price changes are non-stationary
   simple_returns = df['close'].pct_change()
   ```

3. **Never shuffle time-series data** in train-test split
   ```python
   # ✓ Correct: Temporal order preserved
   X_train, X_test, y_train, y_test = train_test_split(
       X, y, test_size=0.2, shuffle=False
   )
   
   # ✗ Wrong: Temporal order destroyed (data leakage)
   train_test_split(X, y, test_size=0.2, shuffle=True)
   ```

4. **Scale features before training** neural networks or distance-based models
   ```python
   scaler = StandardScaler()
   X_scaled = scaler.fit_transform(X_train)
   ```

5. **Monitor data freshness** in production
   ```python
   freshness_sec = (datetime.now() - df['process_timestamp'].max()).total_seconds()
   if freshness_sec > 300:  # 5 minutes stale
       alert("Data pipeline lag detected!")
   ```

#### ✅ **Feature Engineering**

1. **Start with high-level indicators** (OHLCV, volume, volatility)
2. **Add interaction terms** for non-linear relationships
   ```python
   df['volatility_x_volume'] = df['volatility_30m'] * df['volume']
   ```
3. **Use time-based features** (hour of day, day of week)
4. **Engineer features on train set only** to avoid data leakage

#### ✅ **Model Training**

1. **Use appropriate metrics**: RMSE for regression, Sharpe ratio for trading models
2. **Cross-validate on time windows** (walk-forward validation)
3. **Backtest on out-of-sample data** before deployment
4. **Monitor prediction drift** in production

### Troubleshooting Guide

#### ⚠️ **Issue: "No recent data available"**

**Symptoms**: 
- API returns empty dataframe from fetch query
- Latest timestamp is > 5 minutes old

**Diagnosis**:
```sql
-- Check data freshness
SELECT MAX(process_timestamp), COUNT(*) 
FROM gold.gold_crypto_ohlcv
WHERE symbol = 'BTC-USD'
  AND process_timestamp >= NOW() - INTERVAL '30 minutes';
```

**Solutions**:
1. Verify Spark processor is running: `docker ps | grep spark-master`
2. Check Kafka topic has messages: `kafka-console-consumer --topic crypto_ticks ...`
3. Check Postgres heartbeat: `SELECT * FROM gold.pipeline_heartbeat ORDER BY updated_at DESC LIMIT 1`

#### ⚠️ **Issue: "High NaN ratio in log_returns"**

**Symptoms**: 
- 10%+ NULL values in `log_returns` column
- Previous close was 0 or missing

**Solutions**:
```python
# Forward-fill small gaps
df['close'] = df['close'].fillna(method='ffill')

# Or use only records with valid returns
df_clean = df[df['log_returns'].notna()]
```

#### ⚠️ **Issue: "Model predictions are stale"**

**Symptoms**: 
- Predictions from old candles (timestamp > 1 minute old)
- Real-time features are from historical data

**Solutions**:
1. Add timestamp filter: `WHERE timestamp >= NOW() - INTERVAL '5 minutes'`
2. Ensure Spark checkpoint is clean: `rm -rf /tmp/spark_checkpoints/...`
3. Monitor Spark batch latency in logs

#### ⚠️ **Issue: "Model performance degraded in production"**

**Symptoms**: 
- RMSE increasing over time
- Predictions significantly off from actuals
- Training R² was 0.8, but production R² is 0.3

**Root Causes & Solutions**:
| Cause | Evidence | Fix |
|-------|----------|-----|
| Market regime change | High volatility spikes | Retrain on recent data |
| Data quality issues | is_outlier rate > 5% | Investigate pipeline errors |
| Feature staleness | `process_timestamp - ingest_timestamp > 2s` | Optimize Spark aggregation |
| Concept drift | Model trained on 60-day-old data | Implement daily retraining |

**Retraining Strategy**:
```python
# Weekly retraining script
from datetime import datetime, timedelta

def weekly_retrain():
    # Fetch last 7 days of data
    df = fetch_training_data(
        symbol='BTC-USD',
        days=7,
        exclude_outliers=True
    )
    
    X, y, scaler, features = preprocess_features(df)
    model, metrics = train_volatility_model(X, y)
    
    # Compare against existing model
    if metrics['r2'] > current_model_r2:
        save_model(model, scaler)
        alert(f"Model updated: R² = {metrics['r2']:.4f}")
    else:
        log(f"Model NOT updated (R² down to {metrics['r2']:.4f})")
```

---

## Appendix: SQL Queries for Common Tasks

### Fetch Daily Aggregates

```sql
SELECT 
    DATE(timestamp) as date,
    symbol,
    MIN(low) as daily_low,
    MAX(high) as daily_high,
    FIRST_VALUE(open) OVER (PARTITION BY symbol, DATE(timestamp) ORDER BY timestamp) as open,
    LAST_VALUE(close) OVER (PARTITION BY symbol, DATE(timestamp) ORDER BY timestamp) as close,
    SUM(volume) as daily_volume,
    AVG(volatility_30m) as avg_volatility,
    COUNT(*) as candle_count
FROM gold.gold_crypto_ohlcv
WHERE error_flag = FALSE
GROUP BY DATE(timestamp), symbol
ORDER BY date DESC, symbol;
```

### Identify Regime Changes (High Volatility Periods)

```sql
SELECT 
    timestamp,
    symbol,
    volatility_30m,
    z_score,
    CASE 
        WHEN volatility_30m > (
            SELECT PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY volatility_30m)
            FROM gold.gold_crypto_ohlcv
            WHERE symbol = 'BTC-USD'
              AND timestamp >= NOW() - INTERVAL '30 days'
        ) THEN 'HIGH_VOLATILITY'
        ELSE 'NORMAL'
    END as regime
FROM gold.gold_crypto_ohlcv
WHERE symbol = 'BTC-USD'
  AND error_flag = FALSE
  AND timestamp >= NOW() - INTERVAL '7 days'
ORDER BY timestamp DESC
LIMIT 100;
```

### Calculate Rolling Sharpe Ratio

```sql
WITH returns AS (
    SELECT 
        timestamp,
        symbol,
        log_returns,
        AVG(log_returns) OVER (
            PARTITION BY symbol 
            ORDER BY timestamp ROWS BETWEEN 1440 PRECEDING AND CURRENT ROW
        ) as mean_return_1d,
        STDDEV(log_returns) OVER (
            PARTITION BY symbol 
            ORDER BY timestamp ROWS BETWEEN 1440 PRECEDING AND CURRENT ROW
        ) as std_return_1d
    FROM gold.gold_crypto_ohlcv
    WHERE error_flag = FALSE
      AND symbol = 'BTC-USD'
      AND timestamp >= NOW() - INTERVAL '30 days'
)
SELECT 
    timestamp,
    symbol,
    (mean_return_1d / NULLIF(std_return_1d, 0)) * SQRT(1440) as sharpe_ratio_daily
FROM returns
WHERE std_return_1d IS NOT NULL
ORDER BY timestamp DESC;
```

---

## Document History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | May 5, 2026 | ML Team | Initial release with feature catalog, implementation guide, and inference workflow |

---

**For questions or updates, contact the ML Platform team.**
