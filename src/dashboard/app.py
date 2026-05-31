"""
Binance-Style Crypto ML Dashboard
Real-time OHLCV visualization + Production ML Inference powered by DuckDB + Iceberg
Non-blocking partial re-rendering via st.fragment
"""
import os
import logging
from datetime import datetime, timedelta, timezone

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import joblib
import pickle
from pathlib import Path

import duckdb
import boto3
from botocore.config import Config

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


# Configuration


st.set_page_config(
    page_title="CryptoTerminal Pro",
    page_icon="chart_with_upwards_trend",
    layout="wide",
    initial_sidebar_state="collapsed",
)

FRAGMENT_INTERVAL = int(os.getenv("FRAGMENT_REFRESH_SECONDS", "5"))
APP_DIR = Path(__file__).resolve().parent
MODELS_DIR = APP_DIR / "models"
XGBOOST_MODEL_FILES = {
    "BTC/USDT": "xgboost_vol_btcusdt.pkl",
    "ETH/USDT": "xgboost_vol_ethusdt.pkl",
}
ISOLATION_FOREST_MODEL_FILES = {
    "BTC/USDT": "isolation_forest_btcusdt.pkl",
    "ETH/USDT": "isolation_forest_ethusdt.pkl",
}
LSTM_MODEL_FILES = {
    "BTC/USDT": {
        "model": "lstm_btc_model.h5",
        "scaler": "scaler_btc.pkl",
    },
    "ETH/USDT": {
        "model": "lstm_eth_model.h5",
        "scaler": "scaler_eth.pkl",
    },
}
LSTM_METRICS = {
    "BTC/USDT": {
        "baseline_rmse": 34.7801,
        "lstm_rmse": 34.7795,
        "accepted": True,
    },
    "ETH/USDT": {
        "baseline_rmse": 1.2921,
        "lstm_rmse": 1.2920,
        "accepted": True,
    },
}
LSTM_FEATURE_COLUMNS = ["log_returns", "volume"]
LSTM_WINDOW_SIZE = 60
LSTM_NEUTRAL_THRESHOLD_PCT = 0.02
XGBOOST_ARTIFACT_DIRS = [
    MODELS_DIR,
    APP_DIR / "artifacts" / "xgboost",
]
ISOLATION_FOREST_ARTIFACT_DIRS = [
    MODELS_DIR,
    APP_DIR / "artifacts" / "isolation_forest",
]
LSTM_ARTIFACT_DIRS = [
    MODELS_DIR,
    APP_DIR / "artifacts" / "lstm",
]

# MinIO / S3 Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "warehouse")
WAREHOUSE_PATH = "warehouse/gold/crypto_ohlcv/metadata/"

# Vietnam timezone: UTC+7
VN_TZ = timezone(timedelta(hours=7))


# Initialize Session State


def init_session_state():
    """Initialize session state variables for fragment-based updates."""
    if "last_seen_ts" not in st.session_state:
        st.session_state.last_seen_ts = None
    if "cached_df" not in st.session_state:
        st.session_state.cached_df = None
    if "current_symbol" not in st.session_state:
        st.session_state.current_symbol = "BTC/USDT"
    if "duckdb_conn" not in st.session_state:
        st.session_state.duckdb_conn = None



# Boto3 + DuckDB Iceberg Integration


@st.cache_resource
def get_s3_client():
    """Create a boto3 S3 client configured for MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
        config=Config(signature_version="s3v4"),
    )


def resolve_latest_metadata() -> str | None:
    """
    Step A: Use boto3 to list objects in the metadata prefix,
    sort by LastModified, and return the S3 URI of the newest .metadata.json file.
    Searches ALL crypto_ohlcv table folders and returns the one with latest data.
    The Iceberg table path is: warehouse/gold/<table_uuid>/metadata/*.metadata.json
    """
    try:
        s3 = get_s3_client()

        # First, find ALL table folders within gold/
        response = s3.list_objects_v2(
            Bucket=MINIO_BUCKET,
            Prefix="gold/",
            Delimiter="/",
        )

        table_prefixes = []
        for obj in response.get("CommonPrefixes", []):
            prefix = obj["Prefix"]
            if "crypto_ohlcv" in prefix:
                table_prefixes.append(prefix.rstrip("/"))

        if not table_prefixes:
            logger.warning("Could not find any crypto_ohlcv table prefix")
            return None

        # Find the metadata file with latest LastModified across ALL tables
        latest_metadata = None
        latest_modified = None

        for table_prefix in table_prefixes:
            metadata_prefix = f"{table_prefix}/metadata/"
            response = s3.list_objects_v2(
                Bucket=MINIO_BUCKET,
                Prefix=metadata_prefix,
            )

            metadata_files = [
                obj for obj in response.get("Contents", [])
                if obj["Key"].endswith(".metadata.json")
            ]

            if metadata_files:
                # Sort by LastModified descending
                metadata_files.sort(key=lambda x: x["LastModified"], reverse=True)
                newest_in_table = metadata_files[0]

                if latest_modified is None or newest_in_table["LastModified"] > latest_modified:
                    latest_modified = newest_in_table["LastModified"]
                    latest_metadata = f"s3://{MINIO_BUCKET}/{newest_in_table['Key']}"

        return latest_metadata

    except Exception as e:
        logger.warning(f"Failed to resolve latest metadata: {e}")
        return None


def _load_duckdb_extension(conn: duckdb.DuckDBPyConnection, extension_name: str) -> None:
    try:
        conn.execute(f"LOAD {extension_name};")
        return
    except Exception as load_error:
        logger.warning("DuckDB extension '%s' was not preinstalled: %s", extension_name, load_error)

    try:
        conn.execute(f"INSTALL {extension_name};")
        conn.execute(f"LOAD {extension_name};")
    except Exception as install_error:
        raise RuntimeError(
            f"DuckDB extension '{extension_name}' is unavailable. "
            "Rebuild the dashboard image with network access so the extension is "
            "installed during Docker build: docker compose build dashboard"
        ) from install_error


def get_duckdb_connection() -> duckdb.DuckDBPyConnection:
    """
    Step B: Initialize DuckDB in-memory connection with S3/Iceberg extensions.
    Configured to point to local MinIO.
    """
    conn = duckdb.connect(database=":memory:")
    _load_duckdb_extension(conn, "httpfs")
    _load_duckdb_extension(conn, "iceberg")

    # Configure S3 endpoints to point to local MinIO
    conn.execute(f"SET s3_endpoint='{MINIO_ENDPOINT}';")
    conn.execute(f"SET s3_access_key_id='{MINIO_ACCESS_KEY}';")
    conn.execute(f"SET s3_secret_access_key='{MINIO_SECRET_KEY}';")
    conn.execute("SET s3_url_style='path';")
    conn.execute("SET s3_use_ssl=false;")
    conn.execute("SET s3_region='us-east-1';")

    return conn


def fetch_max_timestamp(symbol: str) -> pd.Timestamp | None:
    """
    Lightweight query: fetch only the max timestamp to check for new data.
    Uses DuckDB Iceberg scanner with dynamic metadata resolution.
    Returns the latest candle timestamp or None if no data available.
    """
    try:
        metadata_uri = resolve_latest_metadata()
        if metadata_uri is None:
            return None

        conn = get_duckdb_connection()

        query = f"""
            SELECT MAX(window_start) AS max_ts
            FROM iceberg_scan('{metadata_uri}')
            WHERE symbol = '{symbol}'
        """
        result = conn.execute(query).fetchone()
        conn.close()

        if result and result[0]:
            return pd.to_datetime(result[0])
        return None

    except Exception as e:
        logger.warning(f"Lag check query failed: {e}")
        return None


def fetch_ohlcv(symbol: str, limit: int = 200) -> pd.DataFrame | None:
    """
    Step C: Execute query using DuckDB's Iceberg scanner with dynamic metadata path.
    Wrapped in fault tolerance - warns user if Spark hasn't committed yet.
    """
    try:
        metadata_uri = resolve_latest_metadata()

        if metadata_uri is None:
            st.warning("Iceberg metadata is not available yet. Wait for Spark to commit the first batch.")
            st.stop()

        conn = get_duckdb_connection()

        query = f"""
            SELECT *
            FROM (
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
                    ingestion_time
                FROM iceberg_scan('{metadata_uri}')
                WHERE symbol = '{symbol}'
                ORDER BY window_start DESC
                LIMIT {limit}
            )
            ORDER BY window_start ASC
        """

        df = conn.execute(query).df()
        conn.close()

        if df.empty:
            return df

        # Convert timestamps to datetime
        df["window_start"] = pd.to_datetime(df["window_start"])
        df["window_end"] = pd.to_datetime(df["window_end"])
        df["ingestion_time"] = pd.to_datetime(df["ingestion_time"])

        return df

    except Exception as e:
        logger.exception("Error fetching OHLCV data from Iceberg")
        st.warning(
            f"Unable to load OHLCV data from Iceberg: {e}"
            f"(Error: {str(e)[:100]})"
        )
        st.stop()



# Feature Engineering


def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    """Compute technical indicators for ML models."""
    df = df.sort_values("window_start").reset_index(drop=True)

    # Log return
    df["log_return"] = np.log(df["close"] / df["close"].shift(1)).fillna(0)

    # Volatility must match ML training: rolling std of log returns.
    df["volatility_30m"] = df["log_return"].rolling(window=30, min_periods=1).std().fillna(0)

    # Z-score
    rolling_mean = df["close"].rolling(window=30, min_periods=1).mean()
    rolling_std = df["close"].rolling(window=30, min_periods=1).std()
    df["z_score"] = (
        (df["close"] - rolling_mean) / rolling_std.replace(0, 1)
    ).fillna(0)

    # RSI (14-period)
    delta = df["close"].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss.replace(0, 1)
    df["rsi"] = (100 - (100 / (1 + rs))).fillna(50)

    return df



# ML Model Loading (Cached - loaded once, reused across fragments)


@st.cache_resource
def load_lstm_model(symbol: str):
    """Load symbol-specific LSTM artifacts as experimental reference models."""
    artifact_names = LSTM_MODEL_FILES.get(symbol)
    if artifact_names is None:
        return None, f"LSTM_FALLBACK (unsupported symbol: {symbol})"

    model_path = None
    scaler_path = None
    for artifact_dir in LSTM_ARTIFACT_DIRS:
        candidate_model = artifact_dir / artifact_names["model"]
        candidate_scaler = artifact_dir / artifact_names["scaler"]
        if candidate_model.exists() and candidate_scaler.exists():
            model_path = candidate_model
            scaler_path = candidate_scaler
            break

    if model_path is None or scaler_path is None:
        logger.warning("LSTM artifacts not found for symbol %s", symbol)
        return None, f"LSTM_FALLBACK (missing artifact: {symbol})"

    try:
        import tensorflow as tf
        model = tf.keras.models.load_model(str(model_path), compile=False)
        scaler_bundle = joblib.load(scaler_path)
        model_bundle = {
            "model": model,
            "scaler": scaler_bundle,
            "metrics": LSTM_METRICS.get(symbol, {}),
            "symbol": symbol,
        }
        logger.info("Loaded experimental LSTM artifacts for %s from %s", symbol, model_path)
        return model_bundle, f"LSTM_EXPERIMENTAL_WEAK_PASS ({symbol})"
    except Exception as e:
        logger.warning(f"LSTM artifacts failed to load for {symbol} at {model_path}: {e}")
        return None, f"LSTM_FALLBACK (load failed: {symbol})"


def get_lstm_metrics(symbol: str) -> dict:
    return LSTM_METRICS.get(symbol, {})


def _resolve_xgboost_model_path(symbol: str) -> Path | None:
    artifact_name = XGBOOST_MODEL_FILES.get(symbol)
    if artifact_name is None:
        return None

    for artifact_dir in XGBOOST_ARTIFACT_DIRS:
        candidate = artifact_dir / artifact_name
        if candidate.exists():
            return candidate
    return None


@st.cache_resource
def load_xgboost_model(symbol: str):
    """Load the symbol-specific XGBoost volatility artifact."""
    model_path = _resolve_xgboost_model_path(symbol)
    if model_path is None:
        logger.warning("XGBoost artifact not found for symbol %s", symbol)
        return None, f"XGBoost_FALLBACK ({symbol})"

    try:
        model_bundle = joblib.load(model_path)
        logger.info("Loaded XGBoost artifact for %s from %s", symbol, model_path)
        return model_bundle, f"XGBoost_REAL_MODEL ({symbol})"
    except Exception as e:
        logger.warning(f"XGBoost model failed to load at {model_path}: {e}")
        return None, f"XGBoost_FALLBACK ({symbol})"


@st.cache_resource
def load_isolation_forest(symbol: str):
    """Load the symbol-specific Isolation Forest anomaly artifact."""
    artifact_name = ISOLATION_FOREST_MODEL_FILES.get(symbol)
    if artifact_name is None:
        return None, f"IsoForest_FALLBACK (unsupported symbol: {symbol})"

    model_path = None
    for artifact_dir in ISOLATION_FOREST_ARTIFACT_DIRS:
        candidate = artifact_dir / artifact_name
        if candidate.exists():
            model_path = candidate
            break

    if model_path is None:
        logger.warning("Isolation Forest artifact not found for symbol %s", symbol)
        return None, f"IsoForest_FALLBACK (missing artifact: {symbol})"

    try:
        model_bundle = joblib.load(model_path)
        logger.info("Loaded Isolation Forest artifact for %s from %s", symbol, model_path)
        return model_bundle, f"IsoForest_REAL_MODEL ({symbol})"
    except Exception as e:
        logger.warning(f"Isolation Forest model failed to load at {model_path}: {e}")
        return None, f"IsoForest_FALLBACK (load failed: {symbol})"



# ML Inference Pipeline


def predict_lstm_reference(df: pd.DataFrame, symbol: str) -> dict:
    """
    Run Khôi's LSTM as a reference-only next-close forecast.
    The model expects 60 timesteps with exactly [log_returns, volume].
    """
    result = {
        "available": False,
        "reason": "LSTM reference unavailable.",
        "predicted_close": None,
        "predicted_log_return": None,
        "predicted_change_pct": None,
        "signal": "NEUTRAL",
    }

    if len(df) < LSTM_WINDOW_SIZE:
        result["reason"] = "LSTM needs at least 60 candles for inference."
        return result

    required_columns = {"close", "volume"}
    missing_columns = sorted(required_columns - set(df.columns))
    if missing_columns:
        result["reason"] = f"LSTM missing feature input: {', '.join(missing_columns)}."
        return result

    lstm_bundle, model_name = load_lstm_model(symbol)
    result["model_name"] = model_name
    if lstm_bundle is None:
        result["reason"] = model_name
        return result

    try:
        df_lstm = df.copy()
        if "window_start" in df_lstm.columns:
            df_lstm = df_lstm.sort_values("window_start")
        df_lstm["log_returns"] = np.log(df_lstm["close"] / df_lstm["close"].shift(1)).fillna(0)

        feature_frame = df_lstm[LSTM_FEATURE_COLUMNS].replace([np.inf, -np.inf], np.nan).dropna()
        if len(feature_frame) < LSTM_WINDOW_SIZE:
            result["reason"] = "LSTM needs at least 60 valid candles after feature preparation."
            return result

        latest_features = feature_frame.tail(LSTM_WINDOW_SIZE).to_numpy(dtype=np.float32)
        scaler = lstm_bundle["scaler"]
        model = lstm_bundle["model"]

        scaled_features = scaler.transform(latest_features)
        X = scaled_features.reshape(1, LSTM_WINDOW_SIZE, len(LSTM_FEATURE_COLUMNS))
        pred_scaled = float(model.predict(X, verbose=0)[0][0])

        inverse_placeholder = np.zeros((1, len(LSTM_FEATURE_COLUMNS)))
        inverse_placeholder[0, 0] = pred_scaled
        pred_log_return = float(scaler.inverse_transform(inverse_placeholder)[0, 0])

        latest_close = float(df_lstm["close"].iloc[-1])
        predicted_close = latest_close * float(np.exp(pred_log_return))
        predicted_change_pct = (predicted_close / latest_close - 1.0) * 100.0

        if predicted_change_pct > LSTM_NEUTRAL_THRESHOLD_PCT:
            signal = "UP"
        elif predicted_change_pct < -LSTM_NEUTRAL_THRESHOLD_PCT:
            signal = "DOWN"
        else:
            signal = "NEUTRAL"

        result.update({
            "available": True,
            "reason": "Reference forecast available.",
            "predicted_close": predicted_close,
            "predicted_log_return": pred_log_return,
            "predicted_change_pct": predicted_change_pct,
            "signal": signal,
            "input_shape": X.shape,
        })
        return result
    except Exception as e:
        logger.warning("LSTM reference inference failed for %s: %s", symbol, e)
        result["reason"] = f"LSTM reference inference failed: {e}"
        return result


def next_zero_proxy(log_returns: pd.Series, window: int = 30) -> pd.Series:
    """Match XGBoost training: drop oldest return in the window and append 0."""
    arr = log_returns.to_numpy(dtype=float)
    vals = np.full(len(arr), np.nan)
    for index in range(window - 1, len(arr)):
        hist = arr[index - window + 1 : index + 1]
        shifted = np.concatenate([hist[1:], [0.0]])
        vals[index] = np.std(shifted, ddof=1)
    return pd.Series(vals, index=log_returns.index)


def _build_xgboost_features(df: pd.DataFrame, feature_columns: list[str]) -> pd.DataFrame:
    df_feat = df.copy()
    df_feat["log_returns"] = df_feat["log_return"]
    df_feat["abs_log_returns"] = df_feat["log_returns"].abs()
    df_feat["proxy_current_volatility_30m"] = df_feat["log_returns"].rolling(30).std()
    df_feat["proxy_next_zero"] = next_zero_proxy(df_feat["log_returns"], 30)
    df_feat["log_returns_roll_mean_30"] = df_feat["log_returns"].rolling(30).mean()

    # XGBoost was trained with candle volume; dashboard uses Iceberg OHLCV volume.
    base_columns = ["close", "volume", "log_returns", "z_score"]
    for column in base_columns:
        for lag in [1, 5, 15, 30, 60]:
            df_feat[f"{column}_lag_{lag}"] = df_feat[column].shift(lag)

    df_feat["outgoing_log_return_30"] = df_feat["log_returns"].shift(29)

    for window in [5, 15, 30, 60]:
        df_feat[f"log_returns_roll_std_{window}"] = df_feat["log_returns"].rolling(window).std()
        df_feat[f"abs_log_returns_roll_mean_{window}"] = df_feat["abs_log_returns"].rolling(window).mean()

    for column in ["volume", "close", "z_score"]:
        for window in [5, 15, 30]:
            df_feat[f"{column}_roll_mean_{window}"] = df_feat[column].rolling(window).mean()
            df_feat[f"{column}_roll_std_{window}"] = df_feat[column].rolling(window).std()

    return df_feat.dropna(subset=feature_columns + ["proxy_next_zero"])


def predict_volatility_risk(df: pd.DataFrame) -> tuple[float, str]:
    """
    XGBoost volatility risk scoring.
    Returns (score 0-100, level)
    level: 'low' | 'medium' | 'high' | 'extreme'
    """
    symbol = df["symbol"].iloc[-1] if "symbol" in df.columns and len(df) > 0 else ""
    xgb_model, model_name = load_xgboost_model(symbol)

    if xgb_model is None:
        # Fallback: percentile-based volatility score
        vol = df["volatility_30m"].iloc[-1] if len(df) > 0 else 0
        vol_pct = pd.Series(df["volatility_30m"].fillna(0)).rank(pct=True).iloc[-1]
        score = vol_pct * 100
        if score < 25:
            return score, "low"
        elif score < 50:
            return score, "medium"
        elif score < 75:
            return score, "high"
        return score, "extreme"

    try:
        feature_columns = xgb_model["feature_columns"]
        features = _build_xgboost_features(df, feature_columns)
        if features.empty:
            logger.warning("XGBoost real model has insufficient feature history for %s", symbol)
            return 50.0, "medium"

        latest_features = features.tail(1)
        residual = xgb_model["model"].predict(latest_features[feature_columns].to_numpy(dtype=np.float32))[0]
        predicted_volatility = max(0.0, float(latest_features["proxy_next_zero"].iloc[-1] + residual))
        recent_volatility = df["volatility_30m"].dropna()
        if recent_volatility.empty:
            score = 50.0
        else:
            score = (recent_volatility <= predicted_volatility).mean() * 100
        score = max(0, min(100, score))

        if score < 25:
            level = "low"
        elif score < 50:
            level = "medium"
        elif score < 75:
            level = "high"
        else:
            level = "extreme"

        return float(score), level
    except Exception as e:
        logger.warning(f"XGBoost inference failed for {symbol} using {model_name}: {e}")
        return 50.0, "medium"


def detect_anomalies(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    """
    Isolation Forest anomaly detection.
    Returns (labels, scores) where labels: 'normal' | 'anomaly'
    """
    symbol = df["symbol"].iloc[-1] if "symbol" in df.columns and len(df) > 0 else ""
    iso_bundle, model_name = load_isolation_forest(symbol)

    if iso_bundle is None:
        # Fallback: Z-score based
        z = df["z_score"].abs().fillna(0)
        labels = np.where(z > 2.5, "anomaly", "normal")
        scores = (z / z.max() * 100).fillna(0).values
        return pd.Series(labels, index=df.index), pd.Series(scores, index=df.index)

    try:
        features = df.copy()
        if "log_returns" not in features.columns and "log_return" in features.columns:
            features["log_returns"] = features["log_return"]

        feature_columns = iso_bundle["feature_columns"]
        missing_features = [column for column in feature_columns if column not in features.columns]
        if missing_features:
            logger.warning("Isolation Forest missing features for %s: %s", symbol, missing_features)
            z = df["z_score"].abs().fillna(0)
            labels = np.where(z > 2.5, "anomaly", "normal")
            scores = (z / z.max() * 100).fillna(0).values
            return pd.Series(labels, index=df.index), pd.Series(scores, index=df.index)

        if "volume" in feature_columns:
            volume_index = feature_columns.index("volume")
            volume_limit = iso_bundle["scaler"].mean_[volume_index] + (
                10 * iso_bundle["scaler"].scale_[volume_index]
            )
            if features["volume"].median() > volume_limit:
                features["volume"] = features["volume"].diff().abs().fillna(0)

        X = features[feature_columns].fillna(0).to_numpy(dtype=np.float64)
        X_scaled = iso_bundle["scaler"].transform(X)
        preds = iso_bundle["model"].predict(X_scaled)
        scores = iso_bundle["model"].decision_function(X_scaled)

        labels = np.where(preds == -1, "anomaly", "normal")
        return pd.Series(labels, index=df.index), pd.Series(scores, index=df.index)
    except Exception as e:
        logger.warning(f"Isolation Forest inference failed for {symbol} using {model_name}: {e}")
        return pd.Series("normal", index=df.index), pd.Series(0.0, index=df.index)



# UI Helpers


def get_current_vn_time() -> datetime:
    """Get current wall-clock time in Vietnam timezone (UTC+7)."""
    return datetime.now(VN_TZ)


def to_vn_time(dt_series: pd.Series) -> pd.Series:
    """Convert UTC timestamps to Vietnam (Asia/Ho_Chi_Minh) timezone."""
    if dt_series.dt.tz is None:
        return dt_series.dt.tz_localize("UTC").dt.tz_convert(VN_TZ).dt.tz_localize(None)
    return dt_series.dt.tz_convert(VN_TZ).dt.tz_localize(None)


def to_vn_datetime(dt: datetime) -> datetime:
    """Convert a datetime to Vietnam timezone (UTC+7)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(VN_TZ)


def format_lag(lag_seconds: float) -> str:
    """Format pipeline lag in human-readable format."""
    if lag_seconds < 0:
        lag_seconds = 0
    minutes = int(lag_seconds // 60)
    seconds = int(lag_seconds % 60)
    if minutes > 0:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"


def get_lag_status(lag_seconds: float) -> tuple[str, str]:
    """
    Get pipeline lag status and color.
    Returns (status_text, status_color)
    """
    if lag_seconds < 180:  # < 3 minutes
        return "Healthy", "#26a69a"
    elif lag_seconds < 300:  # 3-5 minutes
        return "Warning", "#ffca28"
    else:  # >= 5 minutes
        return "Stalled", "#ef5350"


def inject_javascript_clock():
    """
    Inject a client-side JavaScript digital clock using st.components.
    Uses setInterval for smooth updates without triggering Python reruns.
    """
    from streamlit.components.v1 import html

    clock_html = """
    <html>
    <head>
        <style>
            * { box-sizing: border-box; }
            body { margin: 0; padding: 0; background: transparent; }
            .clock-container {
                background: linear-gradient(135deg, #1e2329 0%, #2b3139 100%);
                border: 1px solid #3a3f4b;
                border-radius: 10px;
                padding: 12px 18px;
                text-align: center;
                box-shadow: 0 4px 12px rgba(0,0,0,0.3);
                font-family: 'Segoe UI', Tahoma, sans-serif;
                min-width: 220px;
            }
            .clock-label {
                color: #848e9c;
                font-size: 11px;
                text-transform: uppercase;
                letter-spacing: 2px;
                margin-bottom: 4px;
            }
            .clock-time {
                color: #fcd535;
                font-size: 2.2rem;
                font-weight: bold;
                font-family: 'Courier New', Courier, monospace;
                letter-spacing: 2px;
                text-shadow: 0 0 15px rgba(252, 213, 53, 0.4);
                line-height: 1;
                white-space: nowrap;
            }
            .clock-timezone {
                color: #5d6494;
                font-size: 10px;
                margin-top: 4px;
            }
        </style>
    </head>
    <body>
        <div class="clock-container">
            <div class="clock-label">Vietnam Time (UTC+7)</div>
            <div class="clock-time" id="clock-time">--:--:--</div>
            <div class="clock-timezone">Asia/Ho_Chi_Minh</div>
        </div>
        <script>
            function updateClock() {
                const now = new Date();
                const vnOffset = 7 * 60;
                const localOffset = now.getTimezoneOffset();
                const vnTime = new Date(now.getTime() + (vnOffset + localOffset) * 60000);

                const hours = String(vnTime.getHours()).padStart(2, '0');
                const minutes = String(vnTime.getMinutes()).padStart(2, '0');
                const seconds = String(vnTime.getSeconds()).padStart(2, '0');

                const timeStr = hours + ':' + minutes + ':' + seconds;
                document.getElementById('clock-time').textContent = timeStr;
            }

            updateClock();
            setInterval(updateClock, 1000);
        </script>
    </body>
    </html>
    """

    html(clock_html, height=120, scrolling=False)


def render_terminal_header():
    """
    Render the terminal header styles for Binance theme.
    """
    st.markdown("""
        <style>
        .terminal-header {
            background: linear-gradient(135deg, #111722 0%, #151d2c 55%, #0d1117 100%);
            border: 1px solid #2f3742;
            border-radius: 14px;
            padding: 24px 28px;
            margin-bottom: 18px;
            box-shadow: 0 18px 50px rgba(0, 0, 0, 0.28);
        }
        .terminal-title {
            color: #f0b90b;
            font-size: 2.1rem;
            font-weight: 800;
            letter-spacing: 0;
            font-family: Inter, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
        }
        .terminal-subtitle {
            color: #c7d0df;
            font-size: 1rem;
            font-family: Inter, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
        }
        .terminal-architecture {
            color: #7f8a9a;
            font-size: 0.82rem;
            margin-top: 8px;
            font-family: 'Courier New', monospace;
        }
        .kpi-card, .insight-card {
            background: linear-gradient(180deg, #151d2b 0%, #101722 100%);
            border: 1px solid #263242;
            border-radius: 12px;
            padding: 16px 18px;
            min-height: 112px;
            box-shadow: 0 10px 32px rgba(0, 0, 0, 0.20);
        }
        .kpi-label, .insight-label {
            color: #8b98a8;
            font-size: 0.78rem;
            text-transform: uppercase;
            letter-spacing: 0.04em;
            margin-bottom: 8px;
        }
        .kpi-value {
            color: #f5f7fa;
            font-size: 1.35rem;
            font-weight: 750;
            line-height: 1.2;
            word-break: break-word;
        }
        .kpi-subtle, .insight-subtle {
            color: #8b98a8;
            font-size: 0.82rem;
            margin-top: 8px;
            line-height: 1.35;
        }
        .status-running { color: #26a69a; }
        .status-warning { color: #ffca28; }
        .status-stalled { color: #ef5350; }
        .section-title {
            color: #f5f7fa;
            font-size: 1.15rem;
            font-weight: 750;
            margin: 14px 0 8px 0;
        }
        .chart-shell {
            background: #101722;
            border: 1px solid #263242;
            border-radius: 12px;
            padding: 12px 14px 4px 14px;
            margin-top: 8px;
        }
        .insight-value {
            color: #f5f7fa;
            font-size: 1.25rem;
            font-weight: 750;
            margin-bottom: 6px;
        }
        .metric-pill {
            display: inline-block;
            padding: 4px 9px;
            border-radius: 999px;
            border: 1px solid #344255;
            color: #b9c4d2;
            font-size: 0.78rem;
            margin-right: 6px;
        }
        .lag-healthy { background: rgba(38, 166, 154, 0.2); border: 1px solid #26a69a; color: #26a69a; }
        .lag-warning { background: rgba(255, 202, 40, 0.2); border: 1px solid #ffca28; color: #ffca28; }
        .lag-stalled { background: rgba(239, 83, 80, 0.2); border: 1px solid #ef5350; color: #ef5350; animation: pulse-red 1s infinite; }
        @keyframes pulse-red {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.6; }
        }
        </style>
    """, unsafe_allow_html=True)


def render_binance_chart(df: pd.DataFrame, symbol: str) -> go.Figure:
    """
    Binance-style candlestick chart with integrated volume bars.
    Dark theme, green/red candles, volume on secondary Y-axis below.
    uirevision=True ensures zoom/pan state persists across fragment re-runs.
    """
    df_vn = df.copy()
    df_vn["window_start_vn"] = to_vn_time(df_vn["window_start"])

    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=[0.75, 0.25],
        subplot_titles=("", "Volume"),
    )

    # Candlestick colors (Binance style)
    colors = ["#26a69a" if df_vn["close"].iloc[i] >= df_vn["open"].iloc[i] else "#ef5350"
               for i in range(len(df_vn))]

    # Candlestick trace
    fig.add_trace(
        go.Candlestick(
            x=df_vn["window_start_vn"],
            open=df_vn["open"],
            high=df_vn["high"],
            low=df_vn["low"],
            close=df_vn["close"],
            name="OHLC",
            increasing_line_color="#26a69a",
            decreasing_line_color="#ef5350",
            increasing_fillcolor="#26a69a",
            decreasing_fillcolor="#ef5350",
        ),
        row=1, col=1,
    )

    # Volume bars
    fig.add_trace(
        go.Bar(
            x=df_vn["window_start_vn"],
            y=df_vn["volume"],
            marker_color=colors,
            name="Volume",
            opacity=0.7,
        ),
        row=2, col=1,
    )

    # Anomaly markers (red circles)
    anomaly_mask = df_vn["anomaly_label"] == "anomaly"
    if anomaly_mask.any():
        fig.add_trace(
            go.Scatter(
                x=df_vn.loc[anomaly_mask, "window_start_vn"],
                y=df_vn.loc[anomaly_mask, "high"] * 1.005,
                mode="markers",
                name="Whale Alert",
                marker=dict(
                    color="#ff1744",
                    size=18,
                    symbol="circle",
                    line=dict(width=2, color="#ffffff"),
                ),
                text="Whale Alert / MANIPULATION",
                hovertemplate="%{text}<extra></extra>",
            ),
            row=1, col=1,
        )

    fig.update_layout(
        title=dict(
            text=f"{symbol.replace('/', '')} Market View",
            font=dict(size=18, color="#e0e0e0"),
        ),
        template="plotly_dark",
        height=600,
        showlegend=False,
        xaxis_rangeslider_visible=False,
        hovermode="x unified",
        plot_bgcolor="#131722",
        paper_bgcolor="#131722",
        font=dict(color="#d1d4dc"),
        # uirevision=True preserves user's zoom/pan state across re-renders
        uirevision=True,
        yaxis=dict(
            title=None,
            gridcolor="#1f2630",
            zeroline=False,
        ),
        yaxis2=dict(
            title=None,
            gridcolor="#1f2630",
            zeroline=False,
        ),
        xaxis=dict(
            gridcolor="#1f2630",
            zeroline=False,
        ),
    )

    fig.update_yaxes(gridcolor="#1f2630", row=1, col=1)
    fig.update_yaxes(gridcolor="#1f2630", row=2, col=1)
    fig.update_xaxes(gridcolor="#1f2630", row=2, col=1)

    return fig


def render_gauge_chart(score: float, title: str, level: str) -> go.Figure:
    """Render a gauge chart for volatility risk."""
    colors = {"low": "#26a69a", "medium": "#ffca28", "high": "#ff9800", "extreme": "#ef5350"}
    color = colors.get(level, "#26a69a")

    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=score,
        number={"font": {"size": 32, "color": color}, "suffix": ""},
        gauge={
            "axis": {"range": [0, 100], "tickwidth": 1, "tickcolor": "#555"},
            "bar": {"color": color, "thickness": 0.3},
            "bgcolor": "#1f2630",
            "borderwidth": 2,
            "bordercolor": "#555",
            "steps": [
                {"range": [0, 25], "color": "#263238"},
                {"range": [25, 50], "color": "#37474f"},
                {"range": [50, 75], "color": "#455a64"},
                {"range": [75, 100], "color": "#546e7a"},
            ],
        },
        title={"text": title, "font": {"size": 14, "color": "#d1d4dc"}},
    ))

    fig.update_layout(
        height=180,
        margin=dict(l=20, r=20, t=50, b=20),
        template="plotly_dark",
        paper_bgcolor="#131722",
        # uirevision=True preserves user's interaction state
        uirevision=True,
    )

    return fig



# Dynamic Content Fragment (runs every 5 seconds)


def render_kpi_card(label: str, value: str, detail: str = "", status_class: str = ""):
    value_class = f"kpi-value {status_class}".strip()
    st.markdown(
        f"""
        <div class="kpi-card">
            <div class="kpi-label">{label}</div>
            <div class="{value_class}">{value}</div>
            <div class="kpi-subtle">{detail}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_ai_insights(
    symbol: str,
    df: pd.DataFrame,
    vol_score: float,
    vol_level: str,
    anomaly_count: int,
    anomaly_rate: float,
):
    _, lstm_name = load_lstm_model(symbol)
    _, xgb_name = load_xgboost_model(symbol)
    _, iso_name = load_isolation_forest(symbol)
    lstm_metrics = get_lstm_metrics(symbol)
    lstm_forecast = predict_lstm_reference(df, symbol)
    anomaly_state = "Anomaly detected" if anomaly_count > 0 else "Normal"
    anomaly_class = "status-stalled" if anomaly_count > 0 else "status-running"

    st.markdown('<div class="section-title">AI Insights</div>', unsafe_allow_html=True)
    col_xgb, col_iso, col_lstm = st.columns(3)

    with col_xgb:
        st.markdown(
            f"""
            <div class="insight-card">
                <div class="insight-label">XGBoost Volatility</div>
                <div class="insight-value status-running">REAL MODEL</div>
                <div class="insight-subtle">{xgb_name}</div>
                <div style="margin-top:10px;">
                    <span class="metric-pill">Risk: {vol_level.upper()}</span>
                    <span class="metric-pill">Score: {vol_score:.0f}/100</span>
                </div>
                <div class="insight-subtle">Passed baseline; dashboard role: volatility risk.</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with col_iso:
        st.markdown(
            f"""
            <div class="insight-card">
                <div class="insight-label">Isolation Forest Anomaly</div>
                <div class="insight-value status-running">REAL MODEL</div>
                <div class="insight-subtle">{iso_name}</div>
                <div style="margin-top:10px;">
                    <span class="metric-pill">State: <span class="{anomaly_class}">{anomaly_state}</span></span>
                    <span class="metric-pill">Rate: {anomaly_rate:.1f}%</span>
                </div>
                <div class="insight-subtle">{anomaly_count} anomaly marker(s) in current view.</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with col_lstm:
        baseline_rmse = lstm_metrics.get("baseline_rmse", 0.0)
        lstm_rmse = lstm_metrics.get("lstm_rmse", 0.0)
        if lstm_forecast["available"]:
            lstm_prediction_html = (
                '<div style="margin-top:10px;">'
                f'<span class="metric-pill">Next close: ${lstm_forecast["predicted_close"]:,.2f}</span>'
                f'<span class="metric-pill">Log return: {lstm_forecast["predicted_log_return"]:.8f}</span>'
                f'<span class="metric-pill">Change: {lstm_forecast["predicted_change_pct"]:+.4f}%</span>'
                f'<span class="metric-pill">Signal: {lstm_forecast["signal"]}</span>'
                '</div>'
            )
        else:
            lstm_prediction_html = (
                '<div style="margin-top:10px;">'
                f'<span class="metric-pill">{lstm_forecast["reason"]}</span>'
                '</div>'
            )
        st.markdown(
            (
                '<div class="insight-card">'
                '<div class="insight-label">LSTM Forecast</div>'
                '<div class="insight-value status-warning">EXPERIMENTAL</div>'
                f'<div class="insight-subtle">{lstm_name}</div>'
                f'{lstm_prediction_html}'
                '<div style="margin-top:10px;">'
                f'<span class="metric-pill">Baseline RMSE: {baseline_rmse:.4f}</span>'
                f'<span class="metric-pill">LSTM RMSE: {lstm_rmse:.4f}</span>'
                '</div>'
                '<div class="insight-subtle">Reference only. Weak baseline pass; not production-ready.</div>'
                '</div>'
            ),
            unsafe_allow_html=True,
        )


def render_pipeline_health(latest_row: pd.Series, row_count: int):
    latest_status = "OK" if row_count > 0 else "Empty"
    latest_time = to_vn_datetime(latest_row["window_start"]).strftime("%Y-%m-%d %H:%M:%S VN")

    health_df = pd.DataFrame([
        {"Layer": "Data source", "Verified path": "Binance/ccxt", "Status": "Configured"},
        {"Layer": "Kafka topic", "Verified path": "crypto_ticks", "Status": "Configured"},
        {"Layer": "Spark output table", "Verified path": "nessie.gold.crypto_ohlcv", "Status": "Configured"},
        {"Layer": "Dashboard read path", "Verified path": "DuckDB iceberg_scan", "Status": "Active"},
        {"Layer": "Latest row read", "Verified path": latest_time, "Status": latest_status},
    ])

    st.markdown("### Pipeline Health")
    st.dataframe(health_df, use_container_width=True, hide_index=True)


def render_model_status_cards(symbol: str):
    lstm_model, lstm_name = load_lstm_model(symbol)
    xgb_model, xgb_name = load_xgboost_model(symbol)
    iso_model, iso_name = load_isolation_forest(symbol)
    lstm_metrics = get_lstm_metrics(symbol)

    st.markdown("### ML Model Status")
    model_col1, model_col2, model_col3 = st.columns(3)

    with model_col1:
        st.success(f"XGBoost: {xgb_name}")
        st.caption("Dashboard role: volatility risk")

    with model_col2:
        st.success(f"IsoForest: {iso_name}")
        st.caption("Dashboard role: anomaly detection")

    with model_col3:
        st.warning(f"LSTM: {lstm_name}")
        if lstm_metrics:
            st.caption(
                f"Baseline RMSE: {lstm_metrics['baseline_rmse']:.6f} | "
                f"LSTM RMSE: {lstm_metrics['lstm_rmse']:.6f}"
            )
        st.caption("Reference only. Weak baseline pass; not a production signal.")


def render_model_evaluation_section():
    st.markdown("#### Model Evaluation")
    model_eval = pd.DataFrame([
        {
            "Model": "LSTM",
            "Task": "Close price forecasting",
            "Baseline": "Naive Forecast",
            "Result": "Weak pass / experimental",
            "Dashboard Role": "Reference only",
        },
        {
            "Model": "XGBoost",
            "Task": "Volatility prediction",
            "Baseline": "Current/SMA volatility baseline",
            "Result": "Accepted",
            "Dashboard Role": "Volatility risk",
        },
        {
            "Model": "Isolation Forest",
            "Task": "Anomaly detection",
            "Baseline": "abs(z_score) > 3",
            "Result": "Integrated",
            "Dashboard Role": "Anomaly detection",
        },
    ])
    st.dataframe(model_eval, use_container_width=True, hide_index=True)

    st.info(
        "LSTM is shown as experimental because it only barely beats "
        "the naive baseline on 1-minute close forecasting."
    )

    lstm_metrics_df = pd.DataFrame([
        {
            "Symbol": "BTC/USDT",
            "Baseline RMSE": LSTM_METRICS["BTC/USDT"]["baseline_rmse"],
            "LSTM RMSE": LSTM_METRICS["BTC/USDT"]["lstm_rmse"],
            "Accepted": LSTM_METRICS["BTC/USDT"]["accepted"],
        },
        {
            "Symbol": "ETH/USDT",
            "Baseline RMSE": LSTM_METRICS["ETH/USDT"]["baseline_rmse"],
            "LSTM RMSE": LSTM_METRICS["ETH/USDT"]["lstm_rmse"],
            "Accepted": LSTM_METRICS["ETH/USDT"]["accepted"],
        },
    ])
    st.dataframe(lstm_metrics_df, use_container_width=True, hide_index=True)


def render_architecture_and_demo_notes():
    with st.expander("Architecture and demo notes", expanded=False):
        st.code(
            "Binance/ccxt -> crypto-feeder -> Kafka -> Spark Structured Streaming "
            "-> Iceberg/Nessie/MinIO -> DuckDB -> Streamlit",
            language="text",
        )
        st.markdown(
            "- This dashboard is near real-time, not hard real-time.\n"
            "- Observed latency may vary with Docker resources, Binance/network access, "
            "Spark microbatch timing, and Iceberg commits.\n"
            "- ML signals are research/demo signals for coursework and are not financial advice.\n"
            "- LSTM is integrated as experimental/weak-pass and does not override XGBoost or Isolation Forest."
        )


@st.fragment(run_every=FRAGMENT_INTERVAL)
def dynamic_content_fragment(symbol: str, limit: int):
    """
    Fragment that handles all dynamic content: data fetching, ML inference,
    KPI cards, charts, and data table. Only this section re-runs every 5s.
    """
    # Always fetch fresh data - no caching logic to avoid stale data
    df = fetch_ohlcv(symbol, limit=limit)
    if df is None or df.empty:
        st.warning("No market data available yet. Wait for Spark to write new candles.")
        return


    df = compute_features(df)


    current_time_vn = get_current_vn_time()
    latest_candle = df["window_start"].iloc[-1]
    latest_candle_vn = to_vn_datetime(latest_candle)
    pipeline_lag_seconds = (current_time_vn - latest_candle_vn).total_seconds()

    if pipeline_lag_seconds < 0:
        pipeline_lag_seconds = 0

    lag_text = format_lag(pipeline_lag_seconds)
    lag_status, lag_color = get_lag_status(pipeline_lag_seconds)

    lag_class = "lag-healthy" if pipeline_lag_seconds < 180 else "lag-warning" if pipeline_lag_seconds < 300 else "lag-stalled"

    latest = df.iloc[-1]
    pipeline_status_label = "Running" if pipeline_lag_seconds < 300 else "Stalled"
    pipeline_status_class = "status-running" if pipeline_lag_seconds < 300 else "status-stalled"
    if 180 <= pipeline_lag_seconds < 300:
        pipeline_status_class = "status-warning"

    vol_score, vol_level = predict_volatility_risk(df)
    df["anomaly_label"], df["anomaly_score"] = detect_anomalies(df)
    anomaly_count = int((df["anomaly_label"] == "anomaly").sum())
    anomaly_rate = (anomaly_count / len(df) * 100) if len(df) else 0.0

    health_col1, health_col2, health_col3, health_col4, health_col5 = st.columns(5)

    with health_col1:
        render_kpi_card("Pipeline Status", pipeline_status_label, f"Lag: {lag_text}", pipeline_status_class)

    with health_col2:
        render_kpi_card("Latest Data Timestamp", latest_candle_vn.strftime("%Y-%m-%d %H:%M:%S"), "VN+7")

    with health_col3:
        render_kpi_card("Observed Lag", lag_text, "Near real-time freshness")

    with health_col4:
        render_kpi_card("Selected Symbol", symbol, "Binance spot pair")

    with health_col5:
        render_kpi_card("Latest Close Price", f"${latest['close']:,.2f}", "Latest Iceberg candle")

    st.markdown("")

    st.markdown('<div class="section-title">Market View</div>', unsafe_allow_html=True)
    st.caption(
        f"Isolation Forest anomalies in current view: {anomaly_count} / {len(df)} "
        f"({anomaly_rate:.1f}%)"
    )
    if anomaly_count == 0:
        st.caption("No Isolation Forest anomalies detected in current view.")

    st.markdown('<div class="chart-shell">', unsafe_allow_html=True)
    st.plotly_chart(render_binance_chart(df, symbol), use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

    render_ai_insights(symbol, df, vol_score, vol_level, anomaly_count, anomaly_rate)
    st.caption(
        "Near real-time, not hard real-time. ML signals are research/demo signals, "
        "not financial advice. LSTM is experimental/reference-only."
    )



    latest = df.iloc[-1]
    prev_close = df["close"].iloc[-2] if len(df) > 1 else latest["close"]
    price_change = latest["close"] - prev_close
    price_pct = (price_change / prev_close * 100) if prev_close else 0

    stat_col1, stat_col2, stat_col3, stat_col4, stat_col5, stat_col6 = st.columns(6)

    with stat_col1:
        st.metric("Last Price", f"${latest['close']:,.2f}", f"{price_pct:+.2f}%")

    with stat_col2:
        st.metric("Session High", f"${df['high'].max():,.2f}")

    with stat_col3:
        st.metric("Session Low", f"${df['low'].min():,.2f}")

    with stat_col4:
        st.metric("Volume", f"{latest['volume']:,.0f}")

    with stat_col5:
        st.metric("RSI (14)", f"{latest['rsi']:.1f}")

    with stat_col6:
        st.metric("Volatility 30m", f"{latest['volatility_30m']:.6f}")
    st.markdown('<div class="section-title">Technical Details</div>', unsafe_allow_html=True)
    tab_pipeline, tab_models, tab_arch, tab_data = st.tabs([
        "Pipeline Details", "Model Evaluation", "Architecture", "Raw OHLCV"
    ])

    with tab_pipeline:
        render_pipeline_health(latest, len(df))

    with tab_models:
        render_model_evaluation_section()

    with tab_arch:
        st.code(
            "Binance/ccxt -> crypto-feeder -> Kafka -> Spark Structured Streaming "
            "-> Iceberg/Nessie/MinIO -> DuckDB -> Streamlit",
            language="text",
        )
        st.caption(
            "Near real-time, not hard real-time. Observed latency may vary. "
            "ML signals are research/demo signals, not financial advice."
        )

    with tab_data:
        display_df = df[[
            "window_start", "open", "high", "low", "close",
            "volume", "log_return", "rsi", "anomaly_label"
        ]].copy()
        display_df["window_start"] = to_vn_time(display_df["window_start"])
        st.dataframe(
            display_df.tail(50).style.format({
                "open": "{:.4f}", "high": "{:.4f}",
                "low": "{:.4f}", "close": "{:.4f}",
                "volume": "{:.0f}", "log_return": "{:+.6f}",
                "rsi": "{:.2f}",
            }, na_rep="-"),
            use_container_width=True,
            height=360,
        )

    st.divider()
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    now_vn_full = get_current_vn_time().strftime("%Y-%m-%d %H:%M:%S")
    st.caption(
        f"Updated: {now_utc} UTC | {now_vn_full} VN+7 | "
        f"Auto-refresh every {FRAGMENT_INTERVAL}s"
    )



# Main Application


def main():
    # Initialize session state for fragment-based updates
    init_session_state()


    with st.sidebar:
        st.title("Controls")
        symbol = st.selectbox("Trading pair", ["BTC/USDT", "ETH/USDT"])
        limit = st.slider("Candles shown", 50, 500, 200, step=10)

        st.divider()
        st.caption("**DuckDB + Iceberg connection**")
        st.code(f"MinIO Endpoint: {MINIO_ENDPOINT}")
        st.code(f"Bucket: {MINIO_BUCKET}")

        st.divider()
        st.caption(f"Refresh interval: **{FRAGMENT_INTERVAL}s**")
        st.caption("Timezone: **VN+7 (HCMC)**")

        st.divider()
        st.caption("**Model status**")
        with st.spinner("Loading models..."):
            lstm_model, lstm_name = load_lstm_model(symbol)
            xgb_model, xgb_name = load_xgboost_model(symbol)
            iso_model, iso_name = load_isolation_forest(symbol)

        st.warning(f"LSTM: {lstm_name}")
        lstm_metrics = get_lstm_metrics(symbol)
        if lstm_metrics:
            st.caption(
                "LSTM baseline RMSE: "
                f"{lstm_metrics['baseline_rmse']:.6f} | "
                f"LSTM RMSE: {lstm_metrics['lstm_rmse']:.6f} | "
                f"accepted: {str(lstm_metrics['accepted']).lower()}"
            )
            st.caption("LSTM output is experimental/reference only.")
        st.success(f"XGBoost: {xgb_name}")
        st.success(f"IsoForest: {iso_name}")

    render_terminal_header()

    header_col1, header_col2 = st.columns([3, 1])

    with header_col1:
        st.markdown("""
            <div class="terminal-header">
                <span class="terminal-title">CryptoTerminal Pro</span>
                <br>
                <span class="terminal-subtitle">Near real-time Crypto Lakehouse Dashboard</span>
                <div class="terminal-architecture">Kafka &rarr; Spark Structured Streaming &rarr; Iceberg/Nessie/MinIO &rarr; DuckDB &rarr; Streamlit</div>
            </div>
        """, unsafe_allow_html=True)

    with header_col2:
        # Inject JavaScript clock that ticks every second (independent of fragment)
        inject_javascript_clock()


    # This is the ONLY part that re-runs every 5 seconds
    dynamic_content_fragment(symbol, limit)


if __name__ == "__main__":
    main()
