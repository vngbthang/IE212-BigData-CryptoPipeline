#!/usr/bin/env python3
"""
Real-time Crypto Data Pipeline Dashboard
Reads normalized data from PostgreSQL gold layer
Displays 3 panels: Realtime, History, System Status
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor
import numpy as np

# Page configuration
st.set_page_config(
    page_title="Crypto Pipeline Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

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


@st.cache_data(ttl=10)  # Cache for 10 seconds (realtime feel)
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
            EXTRACT(EPOCH FROM (display_timestamp - ingest_timestamp)) * 1000 AS latency_ms
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


@st.cache_data(ttl=10)
def fetch_aggregated_history(symbol: str, hours: int = 4) -> pd.DataFrame:
    """Fetch aggregated candles (5-min) for history panel"""
    try:
        conn = get_db_connection()
        query = """
        SELECT 
            FLOOR(EXTRACT(EPOCH FROM timestamp) / 300) * 300 AS bucket_epoch,
            TO_TIMESTAMP(FLOOR(EXTRACT(EPOCH FROM timestamp) / 300) * 300) AS bucket,
            symbol,
            FIRST_VALUE(open) OVER (PARTITION BY FLOOR(EXTRACT(EPOCH FROM timestamp) / 300) ORDER BY timestamp) AS open,
            MAX(high) OVER (PARTITION BY FLOOR(EXTRACT(EPOCH FROM timestamp) / 300)) AS high,
            MIN(low) OVER (PARTITION BY FLOOR(EXTRACT(EPOCH FROM timestamp) / 300)) AS low,
            LAST_VALUE(close) OVER (PARTITION BY FLOOR(EXTRACT(EPOCH FROM timestamp) / 300) ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS close,
            SUM(volume) OVER (PARTITION BY FLOOR(EXTRACT(EPOCH FROM timestamp) / 300)) AS volume
        FROM gold.gold_crypto_ohlcv
        WHERE symbol = %s 
          AND timestamp >= NOW() - INTERVAL '%s hours'
          AND error_flag = FALSE
        ORDER BY bucket ASC
        """
        df = pd.read_sql(query, conn, params=(symbol, hours))
        conn.close()
        
        # Remove duplicates (keep first of each 5-min bucket)
        if len(df) > 0:
            df['bucket'] = pd.to_datetime(df['bucket'])
            df = df.drop_duplicates(subset=['bucket'], keep='first')
        
        return df
    except Exception as e:
        st.error(f"❌ Database error (history): {e}")
        return pd.DataFrame()


@st.cache_data(ttl=10)
def fetch_kpi_throughput() -> dict:
    """Fetch KPI: Records per minute (last 5 min)"""
    try:
        conn = get_db_connection()
        query = """
        SELECT 
            DATE_TRUNC('minute', timestamp) AS minute,
            COUNT(*) as record_count,
            AVG(EXTRACT(EPOCH FROM (display_timestamp - ingest_timestamp)) * 1000) AS avg_latency_ms
        FROM gold.gold_crypto_ohlcv
        WHERE timestamp >= NOW() - INTERVAL '5 minutes'
        GROUP BY DATE_TRUNC('minute', timestamp)
        ORDER BY minute DESC
        LIMIT 1
        """
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query)
        result = cursor.fetchone()
        conn.close()
        
        return {
            'records_per_min': result['record_count'] if result else 0,
            'avg_latency_ms': round(result['avg_latency_ms'], 2) if result else 0
        }
    except Exception as e:
        st.error(f"❌ KPI error (throughput): {e}")
        return {'records_per_min': 0, 'avg_latency_ms': 0}


@st.cache_data(ttl=10)
def fetch_kpi_data_quality() -> dict:
    """Fetch KPI: Data quality metrics"""
    try:
        conn = get_db_connection()
        query = """
        SELECT 
            COUNT(*) AS total_records,
            COUNT(CASE WHEN error_flag = FALSE THEN 1 END) AS valid_records,
            COUNT(CASE WHEN error_flag = TRUE THEN 1 END) AS error_records
        FROM gold.gold_crypto_ohlcv
        WHERE timestamp >= NOW() - INTERVAL '1 hour'
        """
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query)
        result = cursor.fetchone()
        conn.close()
        
        if result and result['total_records'] > 0:
            valid_pct = (result['valid_records'] / result['total_records']) * 100
            error_rate = (result['error_records'] / result['total_records']) * 100
        else:
            valid_pct = 0
            error_rate = 0
        
        return {
            'total': result['total_records'] if result else 0,
            'valid_pct': round(valid_pct, 1),
            'error_rate': round(error_rate, 2)
        }
    except Exception as e:
        st.error(f"❌ KPI error (quality): {e}")
        return {'total': 0, 'valid_pct': 0, 'error_rate': 0}


@st.cache_data(ttl=10)
def fetch_kpi_latency_percentiles() -> dict:
    """Fetch KPI: Latency percentiles"""
    try:
        conn = get_db_connection()
        query = """
        SELECT
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (display_timestamp - ingest_timestamp)) * 1000) AS p50_ms,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (display_timestamp - ingest_timestamp)) * 1000) AS p95_ms,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (display_timestamp - ingest_timestamp)) * 1000) AS p99_ms,
            MAX(EXTRACT(EPOCH FROM (display_timestamp - ingest_timestamp)) * 1000) AS max_ms
        FROM gold.gold_crypto_ohlcv
        WHERE timestamp >= NOW() - INTERVAL '1 hour'
          AND error_flag = FALSE
        """
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query)
        result = cursor.fetchone()
        conn.close()
        
        return {
            'p50': round(result['p50_ms'], 2) if result and result['p50_ms'] else 0,
            'p95': round(result['p95_ms'], 2) if result and result['p95_ms'] else 0,
            'p99': round(result['p99_ms'], 2) if result and result['p99_ms'] else 0,
            'max': round(result['max_ms'], 2) if result and result['max_ms'] else 0,
        }
    except Exception as e:
        st.error(f"❌ KPI error (latency): {e}")
        return {'p50': 0, 'p95': 0, 'p99': 0, 'max': 0}


@st.cache_data(ttl=10)
def fetch_data_freshness() -> dict:
    """Fetch KPI: Data freshness (seconds since last update)"""
    try:
        conn = get_db_connection()
        query = """
        SELECT 
            EXTRACT(EPOCH FROM (NOW() - MAX(timestamp))) AS seconds_ago
        FROM gold.gold_crypto_ohlcv
        WHERE timestamp >= NOW() - INTERVAL '1 hour'
        """
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query)
        result = cursor.fetchone()
        conn.close()
        
        return {
            'freshness_sec': round(result['seconds_ago'], 1) if result and result['seconds_ago'] else 0
        }
    except Exception as e:
        st.error(f"❌ KPI error (freshness): {e}")
        return {'freshness_sec': 0}


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
st.markdown("<div class='subtitle'>Kafka → Spark → PostgreSQL (Gold Layer)</div>", unsafe_allow_html=True)

# Controls
col1, col2, col3 = st.columns([2, 2, 1])
with col1:
    symbol = st.selectbox("📊 Select Symbol", ["BTC-USD", "ETH-USD", "SOL-USD"], key="symbol_selector")
with col2:
    refresh_interval = st.selectbox("🔄 Refresh Interval", [5, 10, 30], index=1, key="refresh_interval")
with col3:
    st.markdown("")  # Spacer
    if st.button("🔌 Status"):
        st.info("✅ Dashboard connected to PostgreSQL")

st.markdown("---")

# ============================================================================
# PANEL 1: REALTIME (Top)
# ============================================================================

st.markdown("### 📈 Panel 1: Real-time Data (Last 30 minutes)")

df_realtime = fetch_realtime_candles(symbol, minutes=30)

if not df_realtime.empty:
    col1, col2, col3, col4 = st.columns(4)
    
    # Display current metrics
    current_close = df_realtime['close'].iloc[-1]
    current_high = df_realtime['high'].max()
    current_low = df_realtime['low'].min()
    current_volume = df_realtime['volume'].sum()
    
    with col1:
        st.metric("Current Price", f"${current_close:,.2f}")
    with col2:
        change = ((current_close - df_realtime['open'].iloc[0]) / df_realtime['open'].iloc[0]) * 100
        st.metric("24h Change", f"{change:.2f}%", delta=f"{change:.2f}%")
    with col3:
        st.metric("High", f"${current_high:,.2f}")
    with col4:
        st.metric("Low", f"${current_low:,.2f}")
    
    # Candlestick chart
    fig_ohlcv = go.Figure(data=[
        go.Candlestick(
            x=df_realtime['timestamp'],
            open=df_realtime['open'],
            high=df_realtime['high'],
            low=df_realtime['low'],
            close=df_realtime['close'],
            name=symbol
        )
    ])
    fig_ohlcv.update_layout(
        title=f"{symbol} - 1-Minute OHLCV (Last 30 min)",
        yaxis_title="Price (USD)",
        xaxis_title="Time",
        height=400,
        hovermode='x unified',
        template='plotly_white'
    )
    st.plotly_chart(fig_ohlcv, use_container_width=True)
    
    # Volume chart
    fig_volume = go.Figure(data=[
        go.Bar(
            x=df_realtime['timestamp'],
            y=df_realtime['volume'],
            name='Volume',
            marker_color='rgba(31, 119, 180, 0.6)'
        )
    ])
    fig_volume.update_layout(
        title="Volume (Last 30 min)",
        yaxis_title="Volume (BTC/ETH/SOL)",
        xaxis_title="Time",
        height=300,
        showlegend=False,
        template='plotly_white'
    )
    st.plotly_chart(fig_volume, use_container_width=True)
else:
    st.warning("⏳ Waiting for realtime data... Check Kafka & Spark")

st.markdown("---")

# ============================================================================
# PANEL 2: SHORT-TERM HISTORY (Middle)
# ============================================================================

st.markdown("### 📊 Panel 2: Short-term History (Last 4 hours, 5-min candles)")

df_history = fetch_aggregated_history(symbol, hours=4)

if not df_history.empty:
    fig_history = go.Figure(data=[
        go.Candlestick(
            x=df_history['bucket'],
            open=df_history['open'],
            high=df_history['high'],
            low=df_history['low'],
            close=df_history['close'],
            name=symbol
        )
    ])
    fig_history.update_layout(
        title=f"{symbol} - 5-Minute OHLCV (Last 4 hours)",
        yaxis_title="Price (USD)",
        xaxis_title="Time",
        height=400,
        hovermode='x unified',
        template='plotly_white'
    )
    st.plotly_chart(fig_history, use_container_width=True)
else:
    st.warning("⏳ Waiting for historical data...")

st.markdown("---")

# ============================================================================
# PANEL 3: SYSTEM STATUS (Bottom)
# ============================================================================

st.markdown("### 🔧 Panel 3: System Health & KPIs")

kpi_throughput = fetch_kpi_throughput()
kpi_quality = fetch_kpi_data_quality()
kpi_latency = fetch_kpi_latency_percentiles()
kpi_freshness = fetch_data_freshness()

# 3 columns for status
col1, col2, col3 = st.columns(3)

# Column 1: Throughput
with col1:
    st.subheader("📤 Throughput")
    st.metric("Records/Min", f"{kpi_throughput['records_per_min']}")
    st.metric("Avg Latency", f"{kpi_throughput['avg_latency_ms']:.1f} ms")

# Column 2: Quality
with col2:
    st.subheader("✅ Data Quality")
    st.metric("Valid Records %", f"{kpi_quality['valid_pct']:.1f}%")
    st.metric("Error Rate", f"{kpi_quality['error_rate']:.2f}%", delta=f"-{kpi_quality['error_rate']:.2f}%")

# Column 3: Latency & Freshness
with col3:
    st.subheader("⏱️ Latency & Freshness")
    st.metric("P95 Latency", f"{kpi_latency['p95']:.1f} ms")
    st.metric("Data Freshness", f"{kpi_freshness['freshness_sec']:.1f} sec")

# Detailed metrics in expandable sections
with st.expander("📊 Detailed Metrics"):
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Latency Percentiles (last 1 hour)**")
        latency_data = {
            'Percentile': ['P50', 'P95', 'P99', 'MAX'],
            'Latency (ms)': [
                kpi_latency['p50'],
                kpi_latency['p95'],
                kpi_latency['p99'],
                kpi_latency['max']
            ]
        }
        st.dataframe(pd.DataFrame(latency_data), use_container_width=True)
    
    with col2:
        st.markdown("**Quality Summary (last 1 hour)**")
        quality_data = {
            'Metric': ['Total Records', 'Valid Records', 'Error Records', 'Error Rate %'],
            'Value': [
                kpi_quality['total'],
                int(kpi_quality['total'] * kpi_quality['valid_pct'] / 100),
                int(kpi_quality['total'] * kpi_quality['error_rate'] / 100),
                kpi_quality['error_rate']
            ]
        }
        st.dataframe(pd.DataFrame(quality_data), use_container_width=True)

st.markdown("---")

# Footer
st.markdown("""
<div style="text-align: center; color: #999; font-size: 12px; margin-top: 30px;">
    Last updated: {} | Auto-refresh: {} seconds
</div>
""".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), refresh_interval), unsafe_allow_html=True)
