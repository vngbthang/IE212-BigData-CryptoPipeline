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

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="CryptoTerminal Pro",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

FRAGMENT_INTERVAL = int(os.getenv("FRAGMENT_REFRESH_SECONDS", "5"))
MODELS_DIR = Path(__file__).parent / "models"

# MinIO / S3 Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "warehouse")
WAREHOUSE_PATH = "warehouse/gold/crypto_ohlcv/metadata/"

# Vietnam timezone: UTC+7
VN_TZ = timezone(timedelta(hours=7))

# ─────────────────────────────────────────────────────────────────────────────
# Initialize Session State
# ─────────────────────────────────────────────────────────────────────────────

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


# ─────────────────────────────────────────────────────────────────────────────
# Boto3 + DuckDB Iceberg Integration
# ─────────────────────────────────────────────────────────────────────────────

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


def get_duckdb_connection() -> duckdb.DuckDBPyConnection:
    """
    Step B: Initialize DuckDB in-memory connection with S3/Iceberg extensions.
    Configured to point to local MinIO.
    """
    conn = duckdb.connect(database=":memory:")
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute("INSTALL iceberg; LOAD iceberg;")

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
            st.warning("⏳ Đợi Spark chốt cây nến Iceberg đầu tiên...")
            st.stop()

        conn = get_duckdb_connection()

        query = f"""
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
            ORDER BY window_start ASC
            LIMIT {limit}
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
            f"⏳ Đợi Spark chốt cây nến Iceberg đầu tiên... "
            f"(Error: {str(e)[:100]})"
        )
        st.stop()


# ─────────────────────────────────────────────────────────────────────────────
# Feature Engineering
# ─────────────────────────────────────────────────────────────────────────────

def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    """Compute technical indicators for ML models."""
    df = df.sort_values("window_start").reset_index(drop=True)

    # Log return
    df["log_return"] = np.log(df["close"] / df["close"].shift(1)).fillna(0)

    # Volatility (rolling std)
    df["volatility_30m"] = df["close"].rolling(window=30, min_periods=1).std().fillna(0)

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


# ─────────────────────────────────────────────────────────────────────────────
# ML Model Loading (Cached - loaded once, reused across fragments)
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_resource
def load_lstm_model():
    """Load pre-trained LSTM model for trend prediction."""
    model_path = MODELS_DIR / "lstm_trend.h5"
    try:
        import tensorflow as tf
        model = tf.keras.models.load_model(str(model_path), compile=False)
        return model, "LSTM"
    except Exception as e:
        logger.warning(f"LSTM model not found at {model_path}: {e}")
        return None, "LSTM_FALLBACK"


@st.cache_resource
def load_xgboost_model():
    """Load pre-trained XGBoost model for volatility risk scoring."""
    model_path = MODELS_DIR / "xgboost_vol.pkl"
    try:
        model = joblib.load(model_path)
        return model, "XGBoost"
    except Exception as e:
        logger.warning(f"XGBoost model not found at {model_path}: {e}")
        return None, "XGBoost_FALLBACK"


@st.cache_resource
def load_isolation_forest():
    """Load pre-trained Isolation Forest for anomaly detection."""
    model_path = MODELS_DIR / "iso_forest.pkl"
    try:
        model = joblib.load(model_path)
        return model, "IsoForest"
    except Exception as e:
        logger.warning(f"Isolation Forest not found at {model_path}: {e}")
        return None, "IsoForest_FALLBACK"


# ─────────────────────────────────────────────────────────────────────────────
# ML Inference Pipeline
# ─────────────────────────────────────────────────────────────────────────────

def predict_lstm_trend(df: pd.DataFrame) -> tuple[str, float]:
    """
    LSTM trend prediction: returns (signal, confidence)
    signal: 'bullish' | 'bearish' | 'neutral'
    """
    lstm_model, model_name = load_lstm_model()

    if lstm_model is None:
        # Fallback: simple MA crossover
        if len(df) < 20:
            return "neutral", 0.5
        ma5 = df["close"].rolling(5).mean().iloc[-1]
        ma20 = df["close"].rolling(20).mean().iloc[-1]
        current = df["close"].iloc[-1]
        if current > ma5 > ma20:
            return "bullish", 0.65
        elif current < ma5 < ma20:
            return "bearish", 0.65
        return "neutral", 0.5

    try:
        features = df[["close", "volume", "log_return", "volatility_30m", "rsi"]].fillna(0)
        X = features.tail(60).values.reshape(1, 60, 5)

        pred = lstm_model.predict(X, verbose=0)
        prob = float(pred[0][0])

        if prob > 0.6:
            return "bullish", prob
        elif prob < 0.4:
            return "bearish", 1 - prob
        return "neutral", 0.5
    except Exception as e:
        logger.warning(f"LSTM inference failed: {e}")
        return "neutral", 0.5


def predict_volatility_risk(df: pd.DataFrame) -> tuple[float, str]:
    """
    XGBoost volatility risk scoring.
    Returns (score 0-100, level)
    level: 'low' | 'medium' | 'high' | 'extreme'
    """
    xgb_model, model_name = load_xgboost_model()

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
        features = df[["close", "volume", "log_return", "volatility_30m", "z_score"]].fillna(0)
        X = features.tail(30)

        score = xgb_model.predict(X)[-1]
        score = max(0, min(100, score * 100))

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
        logger.warning(f"XGBoost inference failed: {e}")
        return 50.0, "medium"


def detect_anomalies(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    """
    Isolation Forest anomaly detection.
    Returns (labels, scores) where labels: 'normal' | 'anomaly'
    """
    iso_model, model_name = load_isolation_forest()

    if iso_model is None:
        # Fallback: Z-score based
        z = df["z_score"].abs().fillna(0)
        labels = np.where(z > 2.5, "anomaly", "normal")
        scores = (z / z.max() * 100).fillna(0).values
        return pd.Series(labels, index=df.index), pd.Series(scores, index=df.index)

    try:
        features = df[["close", "volume", "log_return", "volatility_30m", "z_score"]].fillna(0)
        X = features.values

        preds = iso_model.predict(X)
        scores = iso_model.decision_function(X)

        labels = np.where(preds == -1, "anomaly", "normal")
        return pd.Series(labels, index=df.index), pd.Series(scores, index=df.index)
    except Exception as e:
        logger.warning(f"Isolation Forest inference failed: {e}")
        return pd.Series("normal", index=df.index), pd.Series(0.0, index=df.index)


# ─────────────────────────────────────────────────────────────────────────────
# UI Helpers
# ─────────────────────────────────────────────────────────────────────────────

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
        return "🟢 Healthy", "#26a69a"
    elif lag_seconds < 300:  # 3-5 minutes
        return "🟡 Warning", "#ffca28"
    else:  # >= 5 minutes
        return "🔴 Stalled", "#ef5350"


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
            <div class="clock-label">🕒 Vietnam (UTC+7)</div>
            <div class="clock-time" id="clock-time">--:--:--</div>
            <div class="clock-timezone">SGT · Asia/Ho_Chi_Minh</div>
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
            background: linear-gradient(90deg, #131722 0%, #1e222d 100%);
            border: 1px solid #2f3742;
            border-radius: 8px;
            padding: 15px 25px;
            margin-bottom: 10px;
        }
        .terminal-title {
            color: #f0b90b;
            font-size: 1.4rem;
            font-weight: bold;
            font-family: 'Courier New', monospace;
        }
        .terminal-subtitle {
            color: #848e9c;
            font-size: 0.85rem;
            font-family: 'Courier New', monospace;
        }
        .lag-indicator {
            border-radius: 6px;
            padding: 8px 12px;
            font-family: 'Courier New', monospace;
            font-size: 0.85rem;
            text-align: center;
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
                name="⚠️ Whale Alert",
                marker=dict(
                    color="#ff1744",
                    size=18,
                    symbol="circle",
                    line=dict(width=2, color="#ffffff"),
                ),
                text="🐋 MANIPULATION",
                hovertemplate="%{text}<extra></extra>",
            ),
            row=1, col=1,
        )

    fig.update_layout(
        title=dict(
            text=f"📈 {symbol.replace('/', '')}/USDT — Binance Style",
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


# ─────────────────────────────────────────────────────────────────────────────
# Dynamic Content Fragment (runs every 5 seconds)
# ─────────────────────────────────────────────────────────────────────────────

@st.fragment(run_every=FRAGMENT_INTERVAL)
def dynamic_content_fragment(symbol: str, limit: int):
    """
    Fragment that handles all dynamic content: data fetching, ML inference,
    KPI cards, charts, and data table. Only this section re-runs every 5s.
    """
    # Always fetch fresh data - no caching logic to avoid stale data
    df = fetch_ohlcv(symbol, limit=limit)
    if df is None or df.empty:
        st.warning("⚠️ Không thể lấy dữ liệu. Kiểm tra kết nối MinIO.")
        return

    # ── Feature Engineering ──────────────────────────────────────────────────
    df = compute_features(df)

    # ── Pipeline Lag & Health Indicator ─────────────────────────────────────
    current_time_vn = get_current_vn_time()
    latest_candle = df["window_start"].iloc[-1]
    latest_candle_vn = to_vn_datetime(latest_candle)
    pipeline_lag_seconds = (current_time_vn - latest_candle_vn).total_seconds()

    if pipeline_lag_seconds < 0:
        pipeline_lag_seconds = 0

    lag_text = format_lag(pipeline_lag_seconds)
    lag_status, lag_color = get_lag_status(pipeline_lag_seconds)

    lag_class = "lag-healthy" if pipeline_lag_seconds < 180 else "lag-warning" if pipeline_lag_seconds < 300 else "lag-stalled"

    health_col1, health_col2, health_col3, health_col4 = st.columns([1, 1, 1, 2])

    with health_col1:
        st.markdown(f"""
            <div class="lag-indicator {lag_class}">
                <strong>Pipeline Status</strong><br>
                {lag_status}<br>
                <small>Lag: {lag_text}</small>
            </div>
        """, unsafe_allow_html=True)

    with health_col2:
        st.metric("Last Candle (VN)", latest_candle_vn.strftime("%H:%M:%S"), f"{lag_text} ago")

    with health_col3:
        st.metric("Next Refresh", f"~{FRAGMENT_INTERVAL}s", "auto")

    with health_col4:
        latest_candle_str = latest_candle_vn.strftime("%Y-%m-%d %H:%M:%S")
        st.markdown(f"""
            <div style="background:#0d1117; border-radius:8px; padding:12px; text-align:center;">
                <small style="color:#848e9c;">Latest Data</small><br>
                <code style="color:#26a69a; font-size:0.9rem;">{latest_candle_str}</code>
            </div>
        """, unsafe_allow_html=True)

    st.divider()

    # ── ML Inference ─────────────────────────────────────────────────────────
    trend_signal, trend_confidence = predict_lstm_trend(df)
    vol_score, vol_level = predict_volatility_risk(df)
    df["anomaly_label"], df["anomaly_score"] = detect_anomalies(df)

    # ── AI Signals Dashboard ────────────────────────────────────────────────
    st.markdown("### 🤖 AI Signals Dashboard")
    sig1, sig2, sig3 = st.columns(3)

    # LSTM Trend Signal
    with sig1:
        trend_colors = {"bullish": "#26a69a", "bearish": "#ef5350", "neutral": "#78909c"}
        trend_color = trend_colors.get(trend_signal, "#78909c")
        trend_icons = {"bullish": "🟢", "bearish": "🔴", "neutral": "⚪"}

        st.markdown(f"""
            <div style="
                background: linear-gradient(135deg, #1a1f2e 0%, #0d1117 100%);
                border-radius: 12px;
                padding: 20px;
                border-left: 4px solid {trend_color};
                text-align: center;
            ">
                <h4 style="color:#888; margin:0;">🤖 LSTM Trend</h4>
                <h2 style="color:{trend_color}; margin:10px 0;">
                    {trend_icons.get(trend_signal, '⚪')} {trend_signal.upper()}
                </h2>
                <p style="color:#888; font-size:12px;">
                    Độ tin cậy: {trend_confidence:.0%}
                </p>
            </div>
        """, unsafe_allow_html=True)

    # XGBoost Volatility Risk
    with sig2:
        vol_colors = {"low": "#26a69a", "medium": "#ffca28", "high": "#ff9800", "extreme": "#ef5350"}
        vol_color = vol_colors.get(vol_level, "#78909c")
        vol_icons = {"low": "🟢", "medium": "🟡", "high": "🟠", "extreme": "🔴"}

        st.plotly_chart(
            render_gauge_chart(vol_score, f"📊 XGBoost Risk — {vol_level.upper()}", vol_level),
            use_container_width=True,
        )

    # Isolation Forest Anomaly
    with sig3:
        anomaly_count = (df["anomaly_label"] == "anomaly").sum()
        is_anomaly = anomaly_count > 0

        if is_anomaly:
            st.markdown(f"""
                <div style="
                    background: linear-gradient(135deg, #2d1a1a 0%, #1a0d0d 100%);
                    border-radius: 12px;
                    padding: 20px;
                    border: 2px solid #ff1744;
                    text-align: center;
                    animation: pulse 2s infinite;
                ">
                    <style>
                        @keyframes pulse {{
                            0%, 100% {{ opacity: 1; }}
                            50% {{ opacity: 0.7; }}
                        }}
                    </style>
                    <h4 style="color:#ff1744; margin:0;">🛡️ Whale Alert</h4>
                    <h2 style="color:#ff1744; margin:10px 0;">
                        🚨 MANIPULATION DETECTED
                    </h2>
                    <p style="color:#ef5350; font-size:14px;">
                        Phát hiện {anomaly_count} điểm bất thường!
                    </p>
                </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
                <div style="
                    background: linear-gradient(135deg, #1a2e1a 0%, #0d170d 100%);
                    border-radius: 12px;
                    padding: 20px;
                    border-left: 4px solid #26a69a;
                    text-align: center;
                ">
                    <h4 style="color:#888; margin:0;">🛡️ Whale Alert</h4>
                    <h2 style="color:#26a69a; margin:10px 0;">
                        ✅ NORMAL
                    </h2>
                    <p style="color:#888; font-size:12px;">
                        Không phát hiện thao túng thị trường
                    </p>
                </div>
            """, unsafe_allow_html=True)

    # ── Main Chart ───────────────────────────────────────────────────────────
    st.markdown("### 📈 Biểu đồ giá — Binance Style")
    st.plotly_chart(render_binance_chart(df, symbol), use_container_width=True)

    # ── Market Stats ─────────────────────────────────────────────────────────
    latest = df.iloc[-1]
    prev_close = df["close"].iloc[-2] if len(df) > 1 else latest["close"]
    price_change = latest["close"] - prev_close
    price_pct = (price_change / prev_close * 100) if prev_close else 0

    stat_col1, stat_col2, stat_col3, stat_col4, stat_col5, stat_col6 = st.columns(6)

    with stat_col1:
        st.metric("Giá hiện tại", f"${latest['close']:,.2f}", f"{price_pct:+.2f}%")

    with stat_col2:
        st.metric("Cao nhất 24h", f"${df['high'].max():,.2f}")

    with stat_col3:
        st.metric("Thấp nhất 24h", f"${df['low'].min():,.2f}")

    with stat_col4:
        st.metric("Khối lượng", f"{latest['volume']:,.0f}")

    with stat_col5:
        st.metric("RSI (14)", f"{latest['rsi']:.1f}")

    with stat_col6:
        st.metric("Biến động", f"{latest['volatility_30m']:.2f}")

    # ── Data Table ────────────────────────────────────────────────────────────
    with st.expander("📋 Dữ liệu OHLCV (50 dòng gần nhất)"):
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
            }, na_rep="—"),
            use_container_width=True,
            height=400,
        )

    # ── Footer ────────────────────────────────────────────────────────────────
    st.divider()
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    now_vn_full = get_current_vn_time().strftime("%Y-%m-%d %H:%M:%S")
    st.caption(
        f"Cập nhật: {now_utc} UTC | {now_vn_full} VN+7 | "
        f"Tự động làm mới mỗi {FRAGMENT_INTERVAL}s (fragment)"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Main Application
# ─────────────────────────────────────────────────────────────────────────────

def main():
    # Initialize session state for fragment-based updates
    init_session_state()

    # ── Sidebar (Static - only re-renders on user interaction) ─────────────────
    with st.sidebar:
        st.title("⚙️ Cài đặt")
        symbol = st.selectbox("Cặp giao dịch", ["BTC/USDT", "ETH/USDT"])
        limit = st.slider("Số nến hiển thị", 50, 500, 200, step=10)

        st.divider()
        st.caption("**Kết nối DuckDB + Iceberg**")
        st.code(f"MinIO Endpoint: {MINIO_ENDPOINT}")
        st.code(f"Bucket: {MINIO_BUCKET}")

        st.divider()
        st.caption(f"🔄 Làm mới fragment: **{FRAGMENT_INTERVAL}s**")
        st.caption(f"🕐 Múi giờ: **VN+7 (HCMC)**")

        # Show ML model loading status
        st.divider()
        st.caption("**🤖 ML Models**")
        with st.spinner("Loading models..."):
            lstm_model, lstm_name = load_lstm_model()
            xgb_model, xgb_name = load_xgboost_model()
            iso_model, iso_name = load_isolation_forest()

        st.success(f"LSTM: {lstm_name}")
        st.success(f"XGBoost: {xgb_name}")
        st.success(f"IsoForest: {iso_name}")

    # ── Terminal Header (Static) ───────────────────────────────────────────────
    render_terminal_header()

    header_col1, header_col2 = st.columns([3, 1])

    with header_col1:
        st.markdown("""
            <div class="terminal-header">
                <span class="terminal-title">⚡ CRYPTO AI TRADING TERMINAL</span>
                <br>
                <span class="terminal-subtitle">Powered by DuckDB + Apache Iceberg | Real-time ML Inference</span>
            </div>
        """, unsafe_allow_html=True)

    with header_col2:
        # Inject JavaScript clock that ticks every second (independent of fragment)
        inject_javascript_clock()

    # ── Dynamic Content Fragment (Non-blocking, partial re-render) ─────────────
    # This is the ONLY part that re-runs every 5 seconds
    dynamic_content_fragment(symbol, limit)


if __name__ == "__main__":
    main()
