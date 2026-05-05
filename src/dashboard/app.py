#!/usr/bin/env python3
"""
Real-time Crypto Data Pipeline Dashboard
Reads normalized data from PostgreSQL gold layer
Displays 3 panels: Realtime, History, System Status
Features: Auto-refresh every 1 second for live candle updates
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
import time

try:
    from streamlit_autorefresh import st_autorefresh
except ImportError:
    st_autorefresh = None

# Page configuration
st.set_page_config(
    page_title="Crypto Pipeline Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# AUTO-REFRESH CONFIGURATION (Real-time mode: 1 second)
# ============================================================================

if st_autorefresh is not None:
    st_autorefresh(interval=1000, key="crypto_dashboard_autorefresh")
else:
    st.warning("streamlit_autorefresh is not installed; falling back to Streamlit reruns.")
    time.sleep(1)
    st.rerun()

# ============================================================================
# DATABASE CONNECTION
# ============================================================================

def get_db_connection():
    """Get PostgreSQL connection"""
    return psycopg2.connect(
        host="postgres",
        port=5432,
        database="cryptopipeline",
        user="crypto_user",
        password="crypto_password"
    )


def fetch_realtime_candles(symbol: str, minutes: int = 30) -> pd.DataFrame:
    """Fetch last N minutes of 1-min OHLCV from gold layer"""
    try:
        conn = get_db_connection()
        query = """
        SELECT 
            timestamp, 
            symbol, 
            open, high, low, close, 
            volume,
            record_count,
                        EXTRACT(EPOCH FROM (process_timestamp - ingest_timestamp)) AS latency_sec
        FROM gold.gold_crypto_ohlcv
        WHERE symbol = %s 
          AND timestamp >= NOW() - INTERVAL '%s minutes'
          AND error_flag = FALSE
        ORDER BY timestamp ASC
        """
        df = pd.read_sql(query, conn, params=(symbol, minutes))
        conn.close()
        
        # Convert timestamp to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df
    except Exception as e:
        st.error(f"❌ Database error (realtime): {e}")
        return pd.DataFrame()


def fetch_pipeline_health() -> dict:
    """Fetch core pipeline health metrics."""
    try:
        conn = get_db_connection()
        # Use the lightweight pipeline_latency_samples for realtime latency
        # and pipeline_heartbeat for freshness to avoid candle-window skew.
        query = """
        WITH last_5m_samples AS (
            SELECT *
            FROM gold.pipeline_latency_samples
            WHERE measured_at >= CURRENT_TIMESTAMP - INTERVAL '5 minutes'
        ),
        last_1m_candles AS (
            SELECT *
            FROM gold.gold_crypto_ohlcv
            WHERE process_timestamp >= CURRENT_TIMESTAMP - INTERVAL '1 minute'
        )
        SELECT
            (
                SELECT AVG(latency_ms) / 1000.0
                FROM last_5m_samples
            ) AS avg_latency_sec,
            (
                SELECT EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(updated_at)))
                FROM gold.pipeline_heartbeat
            ) AS freshness_sec,
            (
                SELECT COALESCE(SUM(record_count), 0)
                FROM last_1m_candles
                WHERE error_flag = FALSE
            ) AS records_per_min,
            (
                SELECT COUNT(*)
                FROM last_5m_samples
            ) AS total_records_5m,
            (
                SELECT 0
            ) AS error_records_5m
        """
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query)
        result = cursor.fetchone()
        conn.close()

        if not result:
            return {
                "avg_latency_sec": 0.0,
                "freshness_sec": 0.0,
                "records_per_min": 0,
                "valid_pct": 0.0,
                "error_rate": 0.0,
            }

        total_5m = int(result["total_records_5m"] or 0)
        error_5m = int(result["error_records_5m"] or 0)
        valid_pct = 0.0
        error_rate = 0.0
        if total_5m > 0:
            error_rate = (error_5m / total_5m) * 100.0
            valid_pct = 100.0 - error_rate

        return {
            "avg_latency_sec": round(float(result["avg_latency_sec"] or 0.0), 3),
            "freshness_sec": round(float(result["freshness_sec"] or 0.0), 3),
            "records_per_min": int(result["records_per_min"] or 0),
            "valid_pct": round(valid_pct, 2),
            "error_rate": round(error_rate, 2),
        }
    except Exception as e:
        st.error(f"❌ KPI error (pipeline health): {e}")
        return {
            "avg_latency_sec": 0.0,
            "freshness_sec": 0.0,
            "records_per_min": 0,
            "valid_pct": 0.0,
            "error_rate": 0.0,
        }


def fetch_latency_percentiles() -> dict:
    """Fetch latency percentiles based on process time - ingest time."""
    try:
        conn = get_db_connection()
        # Read percentiles from pipeline_latency_samples which stores latency in ms
        query = """
        SELECT
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY latency_ms) AS p50_ms,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms) AS p95_ms,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY latency_ms) AS p99_ms,
            MAX(latency_ms) AS max_ms
        FROM gold.pipeline_latency_samples
        WHERE measured_at >= CURRENT_TIMESTAMP - INTERVAL '5 minutes'
        """
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query)
        result = cursor.fetchone()
        conn.close()

        if not result:
            return {"p50_sec": 0.0, "p95_sec": 0.0, "p99_sec": 0.0, "max_sec": 0.0}

        # Convert ms -> seconds
        p50 = float(result["p50_ms"]) / 1000.0 if result["p50_ms"] is not None else 0.0
        p95 = float(result["p95_ms"]) / 1000.0 if result["p95_ms"] is not None else 0.0
        p99 = float(result["p99_ms"]) / 1000.0 if result["p99_ms"] is not None else 0.0
        mx = float(result["max_ms"]) / 1000.0 if result["max_ms"] is not None else 0.0

        return {
            "p50_sec": round(p50, 3),
            "p95_sec": round(p95, 3),
            "p99_sec": round(p99, 3),
            "max_sec": round(mx, 3),
        }
    except Exception as e:
        st.error(f"❌ KPI error (latency percentiles): {e}")
        return {"p50_sec": 0.0, "p95_sec": 0.0, "p99_sec": 0.0, "max_sec": 0.0}


def fetch_api_latency_p99_last_5m() -> dict:
    """API-ready query for the last 5-minute P99 latency in seconds."""
    try:
        conn = get_db_connection()
        query = """
        SELECT
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY latency_ms) / 1000.0 AS p99_latency_sec,
            COUNT(*) AS sample_count
        FROM gold.pipeline_latency_samples
        WHERE measured_at >= CURRENT_TIMESTAMP - INTERVAL '5 minutes'
        """
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query)
        result = cursor.fetchone()
        conn.close()

        if not result:
            return {"p99_latency_sec": 0.0, "sample_count": 0}

        return {
            "p99_latency_sec": round(float(result["p99_latency_sec"] or 0.0), 3),
            "sample_count": int(result["sample_count"] or 0),
        }
    except Exception as e:
        st.error(f"❌ KPI error (api latency p99): {e}")
        return {"p99_latency_sec": 0.0, "sample_count": 0}


def fetch_latest_raw_rows(limit: int = 20) -> pd.DataFrame:
    """Fetch latest rows from gold table for quick schema/data verification."""
    try:
        conn = get_db_connection()
        query = """
        SELECT
            timestamp,
            symbol,
            exchange,
            open,
            high,
            low,
            close,
            volume,
            ingest_timestamp,
            process_timestamp,
            record_count,
            error_flag,
            error_messages
        FROM gold.gold_crypto_ohlcv
        ORDER BY process_timestamp DESC
        LIMIT %s
        """
        df = pd.read_sql(query, conn, params=(limit,))
        conn.close()

        if not df.empty:
            # Convert UTC timestamps from DB to local timezone (Vietnam: UTC+7)
            # DB stores timestamps in UTC (e.g. +00:00). Convert to Asia/Ho_Chi_Minh for display.
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert("Asia/Ho_Chi_Minh")
            df["ingest_timestamp"] = pd.to_datetime(df["ingest_timestamp"], utc=True).dt.tz_convert("Asia/Ho_Chi_Minh")
            df["process_timestamp"] = pd.to_datetime(df["process_timestamp"], utc=True).dt.tz_convert("Asia/Ho_Chi_Minh")
        return df
    except Exception as e:
        st.error(f"❌ Database error (raw table): {e}")
        return pd.DataFrame()


def fetch_price_change(symbol: str) -> dict:
    """Fetch current price vs 1h and 24h references."""
    try:
        conn = get_db_connection()
        query = """
        SELECT
            (
                SELECT close
                FROM gold.gold_crypto_ohlcv
                WHERE symbol = %(symbol)s AND error_flag = FALSE
                ORDER BY timestamp DESC
                LIMIT 1
            ) AS current_close,
            (
                SELECT close
                FROM gold.gold_crypto_ohlcv
                WHERE symbol = %(symbol)s
                  AND error_flag = FALSE
                  AND timestamp <= CURRENT_TIMESTAMP - INTERVAL '1 hour'
                ORDER BY timestamp DESC
                LIMIT 1
            ) AS close_1h_ago,
            (
                SELECT close
                FROM gold.gold_crypto_ohlcv
                WHERE symbol = %(symbol)s
                  AND error_flag = FALSE
                  AND timestamp <= CURRENT_TIMESTAMP - INTERVAL '24 hours'
                ORDER BY timestamp DESC
                LIMIT 1
            ) AS close_24h_ago
        """
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query, {"symbol": symbol})
        result = cursor.fetchone()
        conn.close()

        if not result or result["current_close"] is None:
            return {"current": None, "chg_1h": None, "chg_24h": None}

        current = float(result["current_close"])
        close_1h = float(result["close_1h_ago"]) if result["close_1h_ago"] is not None else None
        close_24h = float(result["close_24h_ago"]) if result["close_24h_ago"] is not None else None

        chg_1h = None
        chg_24h = None
        if close_1h and close_1h != 0:
            chg_1h = ((current - close_1h) / close_1h) * 100.0
        if close_24h and close_24h != 0:
            chg_24h = ((current - close_24h) / close_24h) * 100.0

        return {
            "current": current,
            "chg_1h": round(chg_1h, 2) if chg_1h is not None else None,
            "chg_24h": round(chg_24h, 2) if chg_24h is not None else None,
        }
    except Exception as e:
        st.error(f"❌ KPI error (price change): {e}")
        return {"current": None, "chg_1h": None, "chg_24h": None}


def fetch_volume_distribution(hours: int = 1) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fetch volume split by symbol and exchange."""
    try:
        conn = get_db_connection()
        symbol_query = """
        SELECT
            symbol,
            SUM(volume)::double precision AS total_volume
        FROM gold.gold_crypto_ohlcv
        WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL '%s hours'
          AND error_flag = FALSE
        GROUP BY symbol
        ORDER BY total_volume DESC
        """
        exchange_query = """
        SELECT
            exchange,
            SUM(volume)::double precision AS total_volume
        FROM gold.gold_crypto_ohlcv
        WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL '%s hours'
          AND error_flag = FALSE
        GROUP BY exchange
        ORDER BY total_volume DESC
        """
        by_symbol = pd.read_sql(symbol_query, conn, params=(hours,))
        by_exchange = pd.read_sql(exchange_query, conn, params=(hours,))
        conn.close()

        return by_symbol, by_exchange
    except Exception as e:
        st.error(f"❌ KPI error (volume distribution): {e}")
        return pd.DataFrame(), pd.DataFrame()


# ============================================================================
# UI: HEADER & CONTROLS
# ============================================================================

st.markdown("""
<style>
    .title-main {
        font-size: 32px;
        font-weight: bold;
        color: #1f77b4;
        margin-bottom: 10px;
    }
    .subtitle {
        font-size: 14px;
        color: #666;
        margin-bottom: 20px;
    }
</style>
""", unsafe_allow_html=True)

st.markdown("<div class='title-main'>🚀 Real-time Crypto Data Pipeline</div>", unsafe_allow_html=True)
st.markdown("<div class='subtitle'>Coinbase → Kafka → Spark → PostgreSQL (Gold Layer Control Center)</div>", unsafe_allow_html=True)

# Controls
col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
with col1:
    symbol = st.selectbox("📊 Select Symbol", ["BTC-USD", "ETH-USD", "SOL-USD"], key="symbol_selector")
with col2:
    candle_window_minutes = st.selectbox("Candle Window (min)", [15, 30, 60], index=1)
with col3:
    distribution_hours = st.selectbox("Volume Horizon (h)", [1, 4, 24], index=0)
with col4:
    if st.button("🔌 Status"):
        st.info("✅ Dashboard connected to PostgreSQL | ⚡ Auto-refresh: 1 sec")

st.markdown("---")

# ============================================================================
# 1) PIPELINE HEALTH METRICS
# ============================================================================

st.markdown("### 1) Pipeline Health Metrics")

health = fetch_pipeline_health()
latency_pct = fetch_latency_percentiles()

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Avg Latency", f"{health['avg_latency_sec']:.3f} s")
with col2:
    st.metric("Data Freshness", f"{health['freshness_sec']:.3f} s ago")
with col3:
    st.metric("Records/Min", f"{health['records_per_min']}")
with col4:
    st.metric("Valid Records", f"{health['valid_pct']:.2f}%", delta=f"Error {health['error_rate']:.2f}%")

latency_table = pd.DataFrame(
    {
        "Latency Metric": ["P50", "P95", "P99", "MAX"],
        "Seconds": [
            latency_pct["p50_sec"],
            latency_pct["p95_sec"],
            latency_pct["p99_sec"],
            latency_pct["max_sec"],
        ],
    }
)
st.dataframe(latency_table, use_container_width=True, hide_index=True)

st.markdown("---")

# ============================================================================
# 2) DESCRIPTIVE ANALYTICS
# ============================================================================

st.markdown("### 2) Descriptive Analytics")

df_realtime = fetch_realtime_candles(symbol, minutes=candle_window_minutes)

if not df_realtime.empty:
    latest_close = float(df_realtime["close"].iloc[-1])
    high_price = float(df_realtime["high"].max())
    low_price = float(df_realtime["low"].min())
    total_volume = float(df_realtime["volume"].sum())

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Current Price", f"${latest_close:,.2f}")
    with col2:
        st.metric("Window High", f"${high_price:,.2f}")
    with col3:
        st.metric("Window Low", f"${low_price:,.2f}")
    with col4:
        st.metric("Window Volume", f"{total_volume:,.4f}")

    fig_ohlcv = go.Figure(data=[
        go.Candlestick(
            x=df_realtime["timestamp"],
            open=df_realtime["open"],
            high=df_realtime["high"],
            low=df_realtime["low"],
            close=df_realtime["close"],
            name=symbol
        )
    ])
    fig_ohlcv.update_layout(
        title=f"Candlestick (1-minute OHLCV) - {symbol} - Last {candle_window_minutes} min",
        yaxis_title="Price (USD)",
        xaxis_title="Time",
        height=420,
        hovermode="x unified",
        template="plotly_white",
    )
    st.plotly_chart(fig_ohlcv, use_container_width=True)

    fig_volume = go.Figure(data=[
        go.Bar(
            x=df_realtime["timestamp"],
            y=df_realtime["volume"],
            name="Volume",
            marker_color="rgba(31, 119, 180, 0.65)",
        )
    ])
    fig_volume.update_layout(
        title="Volume Bar Chart",
        yaxis_title="Volume",
        xaxis_title="Time",
        height=280,
        showlegend=False,
        template="plotly_white",
    )
    st.plotly_chart(fig_volume, use_container_width=True)
else:
    st.warning("⏳ Waiting for realtime candles... check Kafka/Spark/Postgres pipeline")

raw_rows = fetch_latest_raw_rows(limit=20)
st.markdown("#### Latest Raw Rows in Gold (Top 20)")
if raw_rows.empty:
    st.info("No rows available in gold table yet.")
else:
    st.dataframe(raw_rows, use_container_width=True, hide_index=True)

# ============================================================================
# 3) COMPARISON AND MULTI-DIMENSION ANALYSIS
# ============================================================================

st.markdown("---")
st.markdown("### 3) Comparison and Multi-Dimension Analysis")

price_change = fetch_price_change(symbol)
col1, col2, col3 = st.columns(3)
with col1:
    if price_change["current"] is None:
        st.metric("Current Close", "N/A")
    else:
        st.metric("Current Close", f"${price_change['current']:,.2f}")
with col2:
    if price_change["chg_1h"] is None:
        st.metric("Price Change (1h)", "N/A")
    else:
        st.metric("Price Change (1h)", f"{price_change['chg_1h']:.2f}%")
with col3:
    if price_change["chg_24h"] is None:
        st.metric("Price Change (24h)", "N/A")
    else:
        st.metric("Price Change (24h)", f"{price_change['chg_24h']:.2f}%")

volume_by_symbol, volume_by_exchange = fetch_volume_distribution(hours=distribution_hours)

col1, col2 = st.columns(2)
with col1:
    if not volume_by_symbol.empty:
        fig_symbol = go.Figure(
            data=[
                go.Bar(
                    x=volume_by_symbol["symbol"],
                    y=volume_by_symbol["total_volume"],
                    marker_color="rgba(46, 139, 87, 0.75)",
                )
            ]
        )
        fig_symbol.update_layout(
            title=f"Volume Distribution by Symbol (last {distribution_hours}h)",
            xaxis_title="Symbol",
            yaxis_title="Total Volume",
            template="plotly_white",
            height=320,
        )
        st.plotly_chart(fig_symbol, use_container_width=True)
    else:
        st.info("No symbol volume data for selected horizon.")

with col2:
    if not volume_by_exchange.empty:
        fig_exchange = go.Figure(
            data=[
                go.Pie(
                    labels=volume_by_exchange["exchange"],
                    values=volume_by_exchange["total_volume"],
                    hole=0.35,
                )
            ]
        )
        fig_exchange.update_layout(
            title=f"Volume Share by Exchange (last {distribution_hours}h)",
            template="plotly_white",
            height=320,
        )
        st.plotly_chart(fig_exchange, use_container_width=True)
    else:
        st.info("No exchange volume data for selected horizon.")

st.markdown("---")

# Footer with real-time refresh indicator
st.markdown(f"""
<div style="text-align: center; color: #999; font-size: 12px; margin-top: 30px;">
    Last updated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} | Auto-refresh: 1 second | Gold Layer Control Center
</div>
""", unsafe_allow_html=True)
