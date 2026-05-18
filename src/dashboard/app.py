#!/usr/bin/env python3
"""
Real-time Crypto Data Pipeline Dashboard
==========================================
Binance Pro / TradingView aesthetic: Ultra-dark mode, neon accents,
high data density, financial terminal UX.

Data sources (PostgreSQL gold layer):
  - gold.gold_crypto_ohlcv      : OHLCV candles (1-min, aggregated)
  - silver.raw_ticks            : Individual trade ticks (< 5s latency)
  - gold.pipeline_latency_samples: E2E latency samples
  - gold.pipeline_heartbeat     : Pipeline liveness heartbeat
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.subplots as psub
from plotly.subplots import make_subplots
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timezone, timedelta
import time
import random

try:
    from streamlit_autorefresh import st_autorefresh
except ImportError:
    st_autorefresh = None

# ============================================================================
# PAGE CONFIG
# ============================================================================
st.set_page_config(
    page_title="CryptoPipeline Pro",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ============================================================================
# AUTO-REFRESH (1 second)
# ============================================================================
if st_autorefresh is not None:
    st_autorefresh(interval=1000, key="pro_dashboard_refresh")
else:
    time.sleep(1)
    st.rerun()

# ============================================================================
# SESSION STATE (persist user selections across reruns)
# ============================================================================
if "symbol" not in st.session_state:
    st.session_state.symbol = "BTC-USD"
if "tf" not in st.session_state:
    st.session_state.tf = "15m"

# ============================================================================
# CONSTANTS & PALETTE
# ============================================================================
BULL = "#0ECB81"    # Binance bullish green
BEAR = "#F6465D"    # Binance bearish red
NEON_CYAN = "#00D4FF"
NEON_YELLOW = "#F0B90B"
BG_DARK = "#0B0E11"
BG_CARD = "#131722"
BG_BORDER = "#1E222D"
TEXT_PRIMARY = "#D1D4DC"
TEXT_MUTED = "#787B86"
MONO_FONT = "JetBrains Mono, Consolas, Courier New, monospace"

# ============================================================================
# GLOBAL CSS (Binance Pro Dark Theme)
# ============================================================================
st.markdown(f"""
<style>
/* ── Reset & Base ─────────────────────────────────────── */
* {{ box-sizing: border-box; }}
body, .stApp {{ background-color: {BG_DARK} !important; color: {TEXT_PRIMARY}; }}
.stDeployButton, header[data-testid="stHeader"],
[data-testid="stToolbar"] {{ display: none !important; }}

/* ── Typography ──────────────────────────────────────── */
html, body, .stApp {{ font-family: 'Inter', system-ui, sans-serif; }}

/* ── Metric Cards ────────────────────────────────────── */
.metric-card {{
    background: {BG_CARD};
    border: 1px solid {BG_BORDER};
    border-radius: 8px;
    padding: 14px 18px;
    text-align: center;
    position: relative;
    overflow: hidden;
}}
.metric-card::before {{
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 2px;
    background: linear-gradient(90deg, {BULL}, transparent);
}}
.metric-card.bear::before {{
    background: linear-gradient(90deg, {BEAR}, transparent);
}}
.metric-card.neutral::before {{
    background: linear-gradient(90deg, {NEON_CYAN}, transparent);
}}
.metric-label {{
    font-size: 11px;
    font-weight: 500;
    color: {TEXT_MUTED};
    text-transform: uppercase;
    letter-spacing: 0.08em;
    margin-bottom: 6px;
}}
.metric-value {{
    font-family: '{MONO_FONT}';
    font-size: 22px;
    font-weight: 700;
    color: {TEXT_PRIMARY};
    line-height: 1;
}}
.metric-delta {{
    font-family: '{MONO_FONT}';
    font-size: 12px;
    color: {TEXT_MUTED};
    margin-top: 4px;
}}
.metric-value.bull {{ color: {BULL}; }}
.metric-value.bear {{ color: {BEAR}; }}

/* ── Ticker Banner ───────────────────────────────────── */
.ticker-banner {{
    background: {BG_CARD};
    border: 1px solid {BG_BORDER};
    border-radius: 10px;
    padding: 20px 28px;
    display: flex;
    align-items: center;
    gap: 40px;
    flex-wrap: wrap;
}}
.ticker-symbol {{
    font-size: 20px;
    font-weight: 700;
    color: {TEXT_PRIMARY};
    letter-spacing: 0.05em;
}}
.ticker-price {{
    font-family: '{MONO_FONT}';
    font-size: 42px;
    font-weight: 700;
    line-height: 1;
}}
.ticker-price.bull {{ color: {BULL}; }}
.ticker-price.bear {{ color: {BEAR}; }}
.ticker-change {{
    font-family: '{MONO_FONT}';
    font-size: 18px;
    font-weight: 600;
    padding: 4px 10px;
    border-radius: 4px;
}}
.ticker-change.bull {{ background: rgba(14,203,129,0.15); color: {BULL}; }}
.ticker-change.bear {{ background: rgba(246,70,93,0.15); color: {BEAR}; }}
.ticker-stat {{
    text-align: center;
}}
.ticker-stat-label {{
    font-size: 10px;
    color: {TEXT_MUTED};
    text-transform: uppercase;
    letter-spacing: 0.08em;
    margin-bottom: 4px;
}}
.ticker-stat-value {{
    font-family: '{MONO_FONT}';
    font-size: 16px;
    font-weight: 600;
    color: {TEXT_PRIMARY};
}}

/* ── Pipeline Health Bar ─────────────────────────────── */
.health-bar {{
    background: {BG_CARD};
    border: 1px solid {BG_BORDER};
    border-radius: 8px;
    padding: 10px 16px;
    display: flex;
    align-items: center;
    gap: 24px;
    flex-wrap: wrap;
}}
.health-dot {{
    width: 8px; height: 8px;
    border-radius: 50%;
    background: {BULL};
    box-shadow: 0 0 6px {BULL};
    flex-shrink: 0;
}}
.health-dot.stale {{ background: {BEAR}; box-shadow: 0 0 6px {BEAR}; }}
.health-label {{
    font-size: 12px;
    color: {TEXT_MUTED};
}}
.health-value {{
    font-family: '{MONO_FONT}';
    font-size: 13px;
    font-weight: 600;
    color: {TEXT_PRIMARY};
}}

/* ── Section Header ───────────────────────────────────── */
.section-header {{
    display: flex;
    align-items: center;
    gap: 10px;
    margin: 18px 0 10px;
}}
.section-title {{
    font-size: 13px;
    font-weight: 600;
    color: {TEXT_MUTED};
    text-transform: uppercase;
    letter-spacing: 0.1em;
}}
.section-line {{
    flex: 1;
    height: 1px;
    background: {BG_BORDER};
}}

/* ── Data Table (dark) ───────────────────────────────── */
.data-table table {{
    width: 100%;
    border-collapse: collapse;
    font-family: '{MONO_FONT}';
    font-size: 12px;
}}
.data-table th {{
    background: {BG_CARD};
    color: {TEXT_MUTED};
    font-weight: 500;
    text-transform: uppercase;
    font-size: 10px;
    letter-spacing: 0.08em;
    padding: 8px 12px;
    border-bottom: 1px solid {BG_BORDER};
    text-align: left;
}}
.data-table td {{
    padding: 7px 12px;
    border-bottom: 1px solid rgba(30,34,45,0.6);
    color: {TEXT_PRIMARY};
}}
.data-table tr:hover td {{
    background: rgba(30,34,45,0.5);
}}

/* ── Tab Styling ─────────────────────────────────────── */
.stTabs [data-baseweb="tab-list"] {{
    gap: 4px;
    background: {BG_CARD};
    border-radius: 8px;
    padding: 4px;
    border: 1px solid {BG_BORDER};
}}
.stTabs [data-baseweb="tab"] {{
    border-radius: 6px;
    font-size: 13px;
    font-weight: 500;
    padding: 6px 20px;
    color: {TEXT_MUTED};
}}
.stTabs [aria-selected="true"] {{
    background: {BG_BORDER} !important;
    color: {NEON_CYAN} !important;
}}

/* ── Selectbox / Slider overrides ───────────────────── */
.stSelectbox > div > div, .stMultiSelect > div > div {{
    background: {BG_CARD} !important;
    border-color: {BG_BORDER} !important;
    color: {TEXT_PRIMARY} !important;
}}

/* ── Status Badge ────────────────────────────────────── */
.badge {{
    display: inline-block;
    padding: 3px 10px;
    border-radius: 20px;
    font-size: 12px;
    font-weight: 600;
}}
.badge-normal {{ background: rgba(14,203,129,0.15); color: {BULL}; }}
.badge-alert {{ background: rgba(246,70,93,0.15); color: {BEAR}; }}
.badge-training {{ background: rgba(0,212,255,0.15); color: {NEON_CYAN}; }}

/* ── Scrollbar ───────────────────────────────────────── */
::-webkit-scrollbar {{ width: 4px; }}
::-webkit-scrollbar-track {{ background: {BG_DARK}; }}
::-webkit-scrollbar-thumb {{ background: {BG_BORDER}; border-radius: 2px; }}
</style>
""", unsafe_allow_html=True)


# ============================================================================
# DATABASE HELPERS
# ============================================================================
def get_conn():
    return psycopg2.connect(
        host="postgres", port=5432, database="cryptopipeline",
        user="crypto_user", password="crypto_password"
    )


def to_local(df, cols):
    """Convert UTC timestamp columns to Asia/Ho_Chi_Minh for display."""
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], utc=True).dt.tz_convert("Asia/Ho_Chi_Minh")
    return df


# ============================================================================
# DATA FETCH FUNCTIONS
# ============================================================================

def fetch_ticker(symbol: str) -> dict:
    """Latest 1-min candle for Binance-style ticker banner."""
    try:
        conn = get_conn()
        query = """
        SELECT
            timestamp,
            open, high, low, close,
            volume, record_count
        FROM gold.gold_crypto_ohlcv
        WHERE symbol = %s AND error_flag = FALSE
        ORDER BY timestamp DESC
        LIMIT 1
        """
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(query, (symbol,))
        row = cur.fetchone()
        conn.close()

        if not row:
            return {}

        # 24h change: compare to candle ~24h ago
        conn2 = get_conn()
        cur2 = conn2.cursor()
        cur2.execute("""
            SELECT close FROM gold.gold_crypto_ohlcv
            WHERE symbol = %s AND error_flag = FALSE
              AND timestamp <= CURRENT_TIMESTAMP - INTERVAL '23 hours'
            ORDER BY timestamp DESC LIMIT 1
        """, (symbol,))
        ref_row = cur2.fetchone()
        conn2.close()

        close_24h = float(ref_row[0]) if ref_row else None
        chg_24h = None
        if close_24h and close_24h != 0:
            chg_24h = ((float(row["close"]) - close_24h) / close_24h) * 100.0

        return {
            "ts": row["timestamp"],
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "volume": float(row["volume"]),
            "record_count": int(row["record_count"]),
            "chg_24h": round(chg_24h, 3) if chg_24h is not None else None,
        }
    except Exception as e:
        return {}


def fetch_candles(symbol: str, tf: str, limit: int = 200) -> pd.DataFrame:
    """Fetch candles aggregated by timeframe from 1-min OHLCV."""
    try:
        # Aggregate on the fly in Python for correctness
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT timestamp, open, high, low, close, volume, record_count
            FROM gold.gold_crypto_ohlcv
            WHERE symbol = %s
              AND error_flag = FALSE
              AND timestamp >= NOW() - INTERVAL '48 hours'
            ORDER BY timestamp ASC
        """, (symbol,))
        rows = cur.fetchall()
        conn.close()
        df = pd.DataFrame(rows)

        if df.empty:
            return pd.DataFrame()

        for col in ["open", "high", "low", "close", "volume", "record_count"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(float)

        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert("Asia/Ho_Chi_Minh")

        # Determine bucket size
        freq_map = {"15m": "15T", "1h": "1H", "4h": "4H"}
        freq = freq_map.get(tf, "15T")

        df = df.set_index("timestamp")
        agg = df.resample(freq).agg({
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
            "record_count": "sum",
        }).dropna(subset=["close"]).reset_index()
        agg.columns = ["candle_ts", "open", "high", "low", "close", "volume", "record_count"]

        return agg.tail(limit).reset_index(drop=True)
    except Exception:
        return pd.DataFrame()


def fetch_realtime_tick(symbol: str) -> dict:
    """Ultra-low latency latest tick from silver.raw_ticks."""
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT price, side, size, tick_timestamp,
                   EXTRACT(EPOCH FROM (clock_timestamp() - tick_timestamp)) AS age_sec
            FROM silver.raw_ticks
            WHERE symbol = %s
              AND tick_timestamp >= CURRENT_TIMESTAMP - INTERVAL '3 seconds'
            ORDER BY tick_timestamp DESC LIMIT 1
        """, (symbol,))
        row = cur.fetchone()
        conn.close()
        if row:
            return dict(row)
        # Fallback: last known tick
        cur2 = conn.cursor(cursor_factory=RealDictCursor) if conn else None
        conn2 = get_conn()
        cur2 = conn2.cursor(cursor_factory=RealDictCursor)
        cur2.execute("""
            SELECT price, side, size, tick_timestamp, NULL AS age_sec
            FROM silver.raw_ticks
            WHERE symbol = %s
            ORDER BY tick_timestamp DESC LIMIT 1
        """, (symbol,))
        row2 = cur2.fetchone()
        conn2.close()
        return dict(row2) if row2 else {}
    except Exception:
        return {}


def fetch_pipeline_health() -> dict:
    """DE Pipeline health from gold.pipeline_latency_samples + heartbeat."""
    try:
        conn = get_conn()
        query = """
        WITH samples AS (
            SELECT
                AVG(latency_ms)      AS avg_latency_ms,
                AVG(latency_ms) / 60.0 AS ticks_per_sec,
                MAX(latency_ms)      AS max_latency_ms,
                COUNT(*)             AS sample_count,
                COUNT(*) FILTER (WHERE latency_ms > 1000) AS error_count,
                MIN(measured_at)     AS oldest_sample
            FROM gold.pipeline_latency_samples
            WHERE measured_at >= CURRENT_TIMESTAMP - INTERVAL '5 minutes'
        ),
        heartbeat AS (
            SELECT EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(updated_at))) AS stale_sec
            FROM gold.pipeline_heartbeat
        ),
        candles AS (
            SELECT COALESCE(SUM(record_count), 0) AS ticks_last_min
            FROM gold.gold_crypto_ohlcv
            WHERE timestamp >= DATE_TRUNC('minute', CURRENT_TIMESTAMP) - INTERVAL '1 minute'
              AND timestamp < DATE_TRUNC('minute', CURRENT_TIMESTAMP)
              AND error_flag = FALSE
        )
        SELECT
            s.avg_latency_ms,
            s.ticks_per_sec,
            s.max_latency_ms,
            s.sample_count,
            s.error_count,
            h.stale_sec,
            c.ticks_last_min,
            s.oldest_sample
        FROM samples s, heartbeat h, candles c
        """
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(query)
        row = cur.fetchone()
        conn.close()
        if not row:
            return {}
        return {
            "avg_latency_ms":  round(float(row["avg_latency_ms"] or 0), 1),
            "ticks_per_sec":   round(float(row["ticks_per_sec"] or 0), 1),
            "max_latency_ms":  round(float(row["max_latency_ms"] or 0), 1),
            "sample_count":    int(row["sample_count"] or 0),
            "error_count":     int(row["error_count"] or 0),
            "stale_sec":       round(float(row["stale_sec"] or 0), 1),
            "ticks_last_min":  int(row["ticks_last_min"] or 0),
            "valid_pct":        round(
                100 * (1 - (row["error_count"] or 0) / max(row["sample_count"] or 1, 1)), 2
            ),
            "oldest_sample":   row["oldest_sample"],
        }
    except Exception:
        return {}


def fetch_ohlcv_table(symbol: str, limit: int = 20) -> pd.DataFrame:
    """Last N rows of OHLCV for raw data table."""
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT timestamp, open, high, low, close, volume, record_count, error_flag
            FROM gold.gold_crypto_ohlcv
            WHERE symbol = %s AND error_flag = FALSE
            ORDER BY timestamp DESC
            LIMIT %s
        """, (symbol, limit))
        rows = cur.fetchall()
        conn.close()
        df = pd.DataFrame(rows)
        # Convert PostgreSQL NUMERIC (Decimal) -> float to avoid pandas arithmetic issues
        for col in ["open", "high", "low", "close", "volume", "record_count"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(float)
        df = to_local(df, ["timestamp"])
        return df
    except Exception:
        return pd.DataFrame()


# ============================================================================
# UI HELPER COMPONENTS
# ============================================================================

def metric_card(label, value, delta=None, color=None, key=None):
    """Render a styled Binance metric card."""
    color_cls = ""
    if color == "bull":
        color_cls = "bull"
    elif color == "bear":
        color_cls = "bear"
    elif color == "neutral":
        color_cls = "neutral"
    delta_html = f"<div class='metric-delta'>{delta}</div>" if delta else ""
    st.markdown(f"""
    <div class="metric-card {color_cls}" {'id="'+key+'"' if key else ''}>
        <div class="metric-label">{label}</div>
        <div class="metric-value {color_cls}">{value}</div>
        {delta_html}
    </div>
    """, unsafe_allow_html=True)


def pipeline_health_bar(health: dict):
    """Render the DE pipeline status bar."""
    stale = health.get("stale_sec", 0)
    is_stale = stale > 30
    dot_cls = "stale" if is_stale else ""

    tick_val = health.get("ticks_last_min", 0)
    lat_val  = health.get("avg_latency_ms", 0)
    valid_pct = health.get("valid_pct", 0)
    err_rate = 100 - valid_pct
    is_live = not is_stale and tick_val > 0

    if is_live:
        status_text = f"● LIVE · {tick_val} ticks/min · {lat_val:.0f}ms avg"
        status_color = BULL
    elif is_stale:
        status_text = f"⚠ STALE · {stale:.0f}s since last heartbeat"
        status_color = BEAR
    else:
        status_text = "○ IDLE · No recent data"
        status_color = TEXT_MUTED

    st.markdown(f"""
    <div class="health-bar">
        <div class="health-dot {dot_cls}"></div>
        <div class="health-label" style="color:{status_color};font-weight:600;font-size:12px;">{status_text}</div>
        <div class="health-label">Valid</div>
        <div class="health-value">{valid_pct:.1f}%</div>
        <div class="health-label">DLQ Rate</div>
        <div class="health-value" style="color:{'#F6465D' if err_rate > 1 else TEXT_PRIMARY}">{err_rate:.2f}%</div>
        <div class="health-label">Samples (5m)</div>
        <div class="health-value">{health.get('sample_count', 0)}</div>
    </div>
    """, unsafe_allow_html=True)


def build_candlestick_chart(df: pd.DataFrame, symbol: str, tf: str) -> go.Figure:
    """Build Plotly candlestick + volume subplot chart."""
    if df.empty or "candle_ts" not in df.columns:
        fig = go.Figure()
        fig.update_layout(
            template="plotly_dark",
            paper_bgcolor=BG_DARK,
            plot_bgcolor=BG_CARD,
            font=dict(color=TEXT_PRIMARY),
            height=520,
        )
        return fig

    ts_col = "candle_ts" if "candle_ts" in df.columns else "timestamp"
    df = df.sort_values(ts_col)

    colors = [BULL if df["close"].iloc[i] >= df["open"].iloc[i] else BEAR for i in range(len(df))]

    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.06,
        row_heights=[0.72, 0.28],
        subplot_titles=("", ""),
    )

    # Candlestick
    fig.add_trace(go.Candlestick(
        x=df[ts_col],
        open=df["open"], high=df["high"],
        low=df["low"], close=df["close"],
        increasing_line_color=BULL,
        decreasing_line_color=BEAR,
        increasing_fillcolor=BULL,
        decreasing_fillcolor=BEAR,
        name=symbol,
        showlegend=False,
    ), row=1, col=1)

    # Volume bars
    fig.add_trace(go.Bar(
        x=df[ts_col], y=df["volume"],
        marker_color=colors,
        marker_line_width=0,
        name="Volume",
        showlegend=False,
        opacity=0.7,
    ), row=2, col=1)

    tf_labels = {"15m": "15 Minute", "1h": "1 Hour", "4h": "4 Hour"}
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor=BG_DARK,
        plot_bgcolor=BG_CARD,
        font=dict(color=TEXT_PRIMARY, family=MONO_FONT, size=11),
        height=520,
        margin=dict(l=60, r=20, t=20, b=40),
        xaxis_rangeslider_visible=False,
        xaxis=dict(
            showgrid=True, gridcolor=BG_BORDER,
            color=TEXT_MUTED, tickfont=dict(size=10),
            tickformat="%H:%M",
        ),
        yaxis=dict(
            showgrid=True, gridcolor=BG_BORDER,
            color=TEXT_PRIMARY, tickfont=dict(size=10),
            tickprefix="$",
        ),
        yaxis2=dict(
            showgrid=False, gridcolor=BG_BORDER,
            color=TEXT_MUTED, tickfont=dict(size=10),
        ),
        hovermode="x unified",
        legend=dict(orientation="h", y=1.02, x=1, xanchor="right"),
    )
    fig.update_xaxes(title_text="", row=1, col=1)
    fig.update_xaxes(title_text=tf_labels.get(tf, tf), row=2, col=1)
    fig.update_yaxes(title_text="Price (USD)", row=1, col=1)
    fig.update_yaxes(title_text="Volume", row=2, col=1)

    return fig


def build_ml_placeholder_chart(df: pd.DataFrame, symbol: str) -> go.Figure:
    """Simulated price vs ML prediction line chart."""
    if df.empty or "close" not in df.columns:
        fig = go.Figure()
        fig.update_layout(
            template="plotly_dark", paper_bgcolor=BG_DARK, plot_bgcolor=BG_CARD,
            font=dict(color=TEXT_PRIMARY), height=300,
        )
        return fig

    ts_col = "candle_ts" if "candle_ts" in df.columns else "timestamp"
    df = df.sort_values(ts_col).tail(60).copy()

    # Simulate LSTM prediction (smoothed close + random noise)
    noise = pd.Series([random.gauss(0, df["close"].std() * 0.003) for _ in range(len(df))])
    pred = df["close"] + noise
    mae = (abs(df["close"] - pred)).mean()

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df[ts_col], y=df["close"],
        mode="lines", name="Actual",
        line=dict(color=BULL, width=1.8),
    ))
    fig.add_trace(go.Scatter(
        x=df[ts_col], y=pred,
        mode="lines", name="LSTM Pred",
        line=dict(color=NEON_CYAN, width=1.5, dash="dot"),
    ))
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor=BG_DARK, plot_bgcolor=BG_CARD,
        font=dict(color=TEXT_PRIMARY, family=MONO_FONT, size=11),
        height=300,
        margin=dict(l=60, r=20, t=20, b=40),
        xaxis=dict(showgrid=True, gridcolor=BG_BORDER, color=TEXT_MUTED, tickformat="%H:%M"),
        yaxis=dict(showgrid=True, gridcolor=BG_BORDER, color=TEXT_PRIMARY, tickprefix="$"),
        hovermode="x unified",
        legend=dict(orientation="h", y=1.02, x=1, xanchor="right",
                    font=dict(color=TEXT_MUTED, size=10)),
    )
    return fig


# ============================================================================
# BUILD UI
# ============================================================================

# ── Header Bar ────────────────────────────────────────────────────────────
st.markdown("""
<div style="display:flex; align-items:center; justify-content:space-between;
            padding: 8px 0 12px; border-bottom: 1px solid #1E222D; margin-bottom:14px;">
    <div style="display:flex;align-items:center;gap:12px;">
        <div style="font-size:18px;font-weight:700;color:#00D4FF;letter-spacing:0.05em;">⬡</div>
        <div>
            <div style="font-size:16px;font-weight:700;color:#D1D4DC;">CryptoPipeline Pro</div>
            <div style="font-size:10px;color:#787B86;letter-spacing:0.08em;">REAL-TIME DATA ENGINE</div>
        </div>
    </div>
    <div style="font-family:JetBrains Mono,monospace;font-size:11px;color:#787B86;">
        Vietnam (UTC+7) · Live
    </div>
</div>
""", unsafe_allow_html=True)

# ── Top Control Bar ────────────────────────────────────────────────────────
col_sym, col_tf = st.columns([1, 1])
with col_sym:
    st.session_state.symbol = st.selectbox(
        "Symbol", ["BTC-USD", "ETH-USD"],
        index=0 if st.session_state.symbol == "BTC-USD" else 1,
        label_visibility="collapsed",
    )
with col_tf:
    st.session_state.tf = st.selectbox(
        "Timeframe", ["15m", "1h", "4h"],
        index={"15m": 0, "1h": 1, "4h": 2}.get(st.session_state.tf, 0),
        label_visibility="collapsed",
    )

symbol = st.session_state.symbol
tf = st.session_state.tf

# ── Data Fetch ─────────────────────────────────────────────────────────────
ticker   = fetch_ticker(symbol)
health   = fetch_pipeline_health()
rt_tick  = fetch_realtime_tick(symbol)
df_chart = fetch_candles(symbol, tf, limit=300)
df_table = fetch_ohlcv_table(symbol, limit=20)

# ============================================================================
# TAB 1: MARKET & PIPELINE STATS
# ============================================================================
tab1, tab2 = st.tabs(["📊 Market & Pipeline Stats", "🤖 AI & ML Hub"])

with tab1:
    # ── DE Pipeline Health ──────────────────────────────────────────────
    st.markdown("<div class='section-header'><span class='section-title'>DE Pipeline Health</span><div class='section-line'></div></div>", unsafe_allow_html=True)
    pipeline_health_bar(health)

    # ── Binance Ticker Banner ────────────────────────────────────────────
    st.markdown("<div class='section-header'><span class='section-title'>Market Overview</span><div class='section-line'></div></div>", unsafe_allow_html=True)

    _ticker = ticker
    if _ticker and _ticker.get("close"):
        price   = _ticker["close"]
        open_p  = _ticker.get("open") or 0
        chg_24h = _ticker.get("chg_24h")
        is_up   = price >= open_p

        price_color = "bull" if is_up else "bear"
        chg_color  = "bull" if is_up else "bear"
        arrow = "▲" if is_up else "▼"
        chg_str = f"{arrow} {abs(chg_24h):.3f}%" if chg_24h is not None else "—"

        st.markdown(f"""
        <div class="ticker-banner">
            <div>
                <div class="ticker-symbol">{symbol}</div>
                <div class="ticker-price {price_color}">${price:,.2f}</div>
                <div style="margin-top:6px;">
                    <span class="ticker-change {chg_color}">{chg_str} (24h)</span>
                </div>
            </div>
            <div class="ticker-stat">
                <div class="ticker-stat-label">Open</div>
                <div class="ticker-stat-value">${open_p:,.2f}</div>
            </div>
            <div class="ticker-stat">
                <div class="ticker-stat-label">24h High</div>
                <div class="ticker-stat-value" style="color:{BULL}">${_ticker.get('high', 0):,.2f}</div>
            </div>
            <div class="ticker-stat">
                <div class="ticker-stat-label">24h Low</div>
                <div class="ticker-stat-value" style="color:{BEAR}">${_ticker.get('low', 0):,.2f}</div>
            </div>
            <div class="ticker-stat">
                <div class="ticker-stat-label">24h Volume</div>
                <div class="ticker-stat-value">{_ticker.get('volume', 0):,.4f}</div>
            </div>
            <div class="ticker-stat">
                <div class="ticker-stat-label">Ticks in Last Candle</div>
                <div class="ticker-stat-value">{_ticker.get('record_count', 0)}</div>
            </div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.info("⏳ Waiting for market data...")

    # ── Key Pipeline Metrics ────────────────────────────────────────────
    st.markdown("<div class='section-header'><span class='section-title'>Pipeline Metrics</span><div class='section-line'></div></div>", unsafe_allow_html=True)
    m1, m2, m3, m4 = st.columns(4)
    with m1:
        metric_card(
            "E2E Latency (avg)",
            f"{health.get('avg_latency_ms', 0):.0f} ms",
            delta=f"max {health.get('max_latency_ms', 0):.0f} ms",
        )
    with m2:
        ticks = health.get("ticks_last_min", 0)
        metric_card(
            "Throughput",
            f"{ticks} ticks/min",
            delta=f"{health.get('ticks_per_sec', 0):.1f} ticks/s",
            color="bull" if ticks > 0 else None,
        )
    with m3:
        vp = health.get("valid_pct", 0)
        metric_card(
            "Valid Records",
            f"{vp:.1f}%",
            delta=f"{health.get('sample_count', 0)} samples (5m)",
            color="bull" if vp > 99 else ("bear" if vp < 95 else "neutral"),
        )
    with m4:
        err = 100 - health.get("valid_pct", 0)
        metric_card(
            "DLQ / Error Rate",
            f"{err:.2f}%",
            delta=f"{health.get('error_count', 0)} errors",
            color="bear" if err > 1 else "neutral",
        )

    # ── Main Candlestick Chart ──────────────────────────────────────────
    st.markdown("<div class='section-header'><span class='section-title'>Candlestick Chart ({symbol})</span><div class='section-line'></div></div>", unsafe_allow_html=True)
    fig = build_candlestick_chart(df_chart, symbol, tf)
    st.plotly_chart(fig, use_container_width=True, config={
        "displayModeBar": True,
        "modeBarButtonsToRemove": ["lasso2d", "select2d"],
        "template": "plotly_dark",
    })

    # ── Raw OHLCV Table ────────────────────────────────────────────────
    st.markdown("<div class='section-header'><span class='section-title'>Last 20 OHLCV Candles</span><div class='section-line'></div></div>", unsafe_allow_html=True)

    if not df_table.empty:
        display = df_table.copy()
        # Defensive: ensure all numeric cols are float (handles cached Decimal types)
        for col in ["open", "high", "low", "close", "volume", "record_count"]:
            if col in display.columns:
                display[col] = pd.to_numeric(display[col], errors="coerce").fillna(0).astype(float)
        display["chg"] = display["close"] - display["open"]
        display["chg_pct"] = (((display["close"] - display["open"]) / display["open"] * 100).round(3))
        display["▲▼"] = display["chg"].apply(lambda x: "▲" if x >= 0 else "▼")
        display["chg_str"] = display.apply(
            lambda r: f"{'▲' if r['chg'] >= 0 else '▼'} {abs(r['chg']):.2f} ({'+' if r['chg_pct'] >= 0 else ''}{r['chg_pct']:.3f}%)",
            axis=1
        )
        display["price_color"] = display["chg"].apply(
            lambda x: BULL if x >= 0 else BEAR
        )
        display["close_str"] = display.apply(
            lambda r: f"<span style='color:{r['price_color']}'>${r['close']:,.2f}</span>",
            axis=1
        )

        cols_show = ["timestamp", "open", "high", "low", "close_str", "volume", "record_count", "chg_str"]
        cols_renamed = {
            "timestamp": "Time (UTC+7)",
            "open": "Open",
            "high": "High",
            "low": "Low",
            "close_str": "Close",
            "volume": "Volume",
            "record_count": "Ticks",
            "chg_str": "Change",
        }

        # Build HTML table manually for color-coded display
        rows_html = ""
        for _, r in display.iterrows():
            chg_cls = BULL if r["chg"] >= 0 else BEAR
            rows_html += f"""
            <tr>
                <td>{r['timestamp'].strftime('%H:%M:%S')}</td>
                <td>${r['open']:,.2f}</td>
                <td>${r['high']:,.2f}</td>
                <td>${r['low']:,.2f}</td>
                <td style="color:{chg_cls}">${r['close']:,.2f}</td>
                <td>{r['volume']:.4f}</td>
                <td>{r['record_count']}</td>
                <td style="color:{chg_cls}">{r['chg_str']}</td>
            </tr>"""

        st.markdown(f"""
        <div class="data-table">
        <table>
            <thead>
                <tr>
                    <th>Time (UTC+7)</th><th>Open</th><th>High</th><th>Low</th>
                    <th>Close</th><th>Volume</th><th>Ticks</th><th>Change</th>
                </tr>
            </thead>
            <tbody>{rows_html}</tbody>
        </table>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.info("No OHLCV data yet.")

# ============================================================================
# TAB 2: AI & ML HUB
# ============================================================================
with tab2:
    # ── Training Banner ────────────────────────────────────────────────
    st.markdown("""
    <div style="background:rgba(0,212,255,0.08);border:1px solid rgba(0,212,255,0.3);
                border-radius:8px;padding:12px 18px;margin-bottom:18px;display:flex;
                align-items:center;gap:12px;">
        <div style="width:10px;height:10px;border-radius:50%;background:#00D4FF;
                    box-shadow:0 0 8px #00D4FF;animation:pulse 2s infinite;"></div>
        <div>
            <div style="font-size:13px;font-weight:600;color:#00D4FF;">AI/ML Hub — Placeholder UI</div>
            <div style="font-size:11px;color:#787B86;margin-top:2px;">
                Models are being trained. Connect real ML inference endpoints to activate each panel.
            </div>
        </div>
    </div>
    <style>@keyframes pulse {{
        0%,100%{{opacity:1;}}50%{{opacity:0.3;}}
    }}</style>
    """, unsafe_allow_html=True)

    # ── 3 Engine Cards ────────────────────────────────────────────────
    st.markdown("<div class='section-header'><span class='section-title'>AI Prediction Engines</span><div class='section-line'></div></div>", unsafe_allow_html=True)

    e1, e2, e3 = st.columns(3)

    # ── Engine 1: XGBoost Volatility ────────────────────────────────
    with e1:
        # Simulate volatility value
        vol_sim = round(random.uniform(0.5, 3.5), 2)
        vol_color = BULL if vol_sim < 2 else (BEAR if vol_sim > 3 else TEXT_PRIMARY)

        # Build gauge chart
        fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number",
            value=vol_sim,
            number={"suffix": "%", "font": {"size": 28, "color": TEXT_PRIMARY, "family": MONO_FONT}},
            gauge={
                "axis": {"range": [0, 5], "tickcolor": BG_BORDER, "tickfont": {"size": 9, "color": TEXT_MUTED}},
                "bar": {"color": vol_color, "thickness": 0.6},
                "bgcolor": BG_CARD,
                "bordercolor": BG_BORDER,
                "borderwidth": 1,
                "steps": [
                    {"range": [0, 2],   "color": "rgba(14,203,129,0.2)"},
                    {"range": [2, 3.5], "color": "rgba(240,185,11,0.2)"},
                    {"range": [3.5, 5], "color": "rgba(246,70,93,0.2)"},
                ],
                "threshold": {
                    "line": {"color": TEXT_MUTED, "width": 0},
                    "thickness": 0,
                },
            },
        ))
        fig_gauge.update_layout(
            template="plotly_dark",
            paper_bgcolor=BG_DARK,
            font=dict(color=TEXT_PRIMARY, family=MONO_FONT),
            height=200, margin=dict(l=20, r=20, t=30, b=20),
        )

        st.markdown(f"""
        <div class="metric-card neutral" style="margin-bottom:8px;">
            <div class="metric-label">XGBoost Volatility Predictor</div>
            <div style="font-size:11px;color:#787B86;margin-top:2px;">Predicted 30m Realized Vol</div>
        </div>
        """, unsafe_allow_html=True)
        st.plotly_chart(fig_gauge, use_container_width=True)

        col_vol_label, col_vol_val = st.columns(2)
        with col_vol_label:
            st.markdown("<div class='metric-label' style='text-align:center;'>Status</div>", unsafe_allow_html=True)
            st.markdown("<div style='text-align:center;'><span class='badge badge-normal'>Normal Range</span></div>", unsafe_allow_html=True)
        with col_vol_val:
            st.markdown("<div class='metric-label' style='text-align:center;'>Confidence</div>", unsafe_allow_html=True)
            conf = random.randint(78, 94)
            st.markdown(f"<div style='text-align:center;'><span style='font-family:{MONO_FONT};font-size:18px;font-weight:700;color:{TEXT_PRIMARY};'>{conf}%</span></div>", unsafe_allow_html=True)

    # ── Engine 2: LSTM Trend ──────────────────────────────────────────
    with e2:
        signals = ["BUY", "SELL", "HOLD"]
        weights = [0.35, 0.35, 0.30]
        sig = random.choices(signals, weights)[0]
        sig_color = {"BUY": "bull", "SELL": "bear", "HOLD": "neutral"}.get(sig, "neutral")
        sig_bg    = {"BUY": f"rgba(14,203,129,0.15)", "SELL": f"rgba(246,70,93,0.15)", "HOLD": f"rgba(0,212,255,0.15)"}.get(sig, BG_CARD)
        sig_text  = {"BUY": BULL, "SELL": BEAR, "HOLD": NEON_CYAN}.get(sig, TEXT_PRIMARY)
        confidence = random.randint(70, 96)

        # Signal gauge (horizontal bar)
        fig_signal = go.Figure(go.Indicator(
            mode="gauge+number",
            value=confidence,
            number={"suffix": "%", "font": {"size": 28, "color": sig_text, "family": MONO_FONT}},
            gauge={
                "axis": {"range": [0, 100], "tickcolor": BG_BORDER, "tickfont": {"size": 9, "color": TEXT_MUTED}},
                "bar": {"color": sig_text, "thickness": 0.6},
                "bgcolor": BG_CARD,
                "bordercolor": BG_BORDER,
                "borderwidth": 1,
                "steps": [
                    {"range": [0, 40],   "color": "rgba(246,70,93,0.2)"},
                    {"range": [40, 70],  "color": "rgba(240,185,11,0.2)"},
                    {"range": [70, 100], "color": "rgba(14,203,129,0.2)"},
                ],
            },
        ))
        fig_signal.update_layout(
            template="plotly_dark",
            paper_bgcolor=BG_DARK,
            font=dict(color=TEXT_PRIMARY, family=MONO_FONT),
            height=200, margin=dict(l=20, r=20, t=30, b=20),
        )

        st.markdown(f"""
        <div class="metric-card neutral" style="margin-bottom:8px;">
            <div class="metric-label">LSTM Trend Classifier</div>
            <div style="font-size:11px;color:#787B86;margin-top:2px;">Price Direction Signal</div>
        </div>
        """, unsafe_allow_html=True)
        st.plotly_chart(fig_signal, use_container_width=True)

        col_lstm_sig, col_lstm_conf = st.columns(2)
        with col_lstm_sig:
            st.markdown("<div class='metric-label' style='text-align:center;'>Signal</div>", unsafe_allow_html=True)
            st.markdown(f"<div style='text-align:center;'><span class='badge' style='background:{sig_bg};color:{sig_text};font-size:16px;padding:4px 16px;'>{sig}</span></div>", unsafe_allow_html=True)
        with col_lstm_conf:
            st.markdown("<div class='metric-label' style='text-align:center;'>Confidence</div>", unsafe_allow_html=True)
            st.markdown(f"<div style='text-align:center;'><span style='font-family:{MONO_FONT};font-size:18px;font-weight:700;color:{sig_text};'>{confidence}%</span></div>", unsafe_allow_html=True)

    # ── Engine 3: Isolation Forest ──────────────────────────────────────
    with e3:
        is_normal = random.random() > 0.15  # 85% chance normal
        status_label = "SYSTEM NORMAL" if is_normal else "ANOMALY DETECTED"
        status_cls   = "badge-normal" if is_normal else "badge-alert"
        status_color = BULL if is_normal else BEAR
        score = round(random.uniform(0.0, 0.3) if is_normal else random.uniform(0.6, 1.0), 3)

        fig_iforest = go.Figure(go.Indicator(
            mode="gauge+number",
            value=score,
            number={"suffix": "", "font": {"size": 28, "color": status_color, "family": MONO_FONT}},
            gauge={
                "axis": {"range": [0, 1], "tickcolor": BG_BORDER, "tickfont": {"size": 9, "color": TEXT_MUTED}},
                "bar": {"color": status_color, "thickness": 0.6},
                "bgcolor": BG_CARD,
                "bordercolor": BG_BORDER,
                "borderwidth": 1,
                "steps": [
                    {"range": [0, 0.5],   "color": "rgba(14,203,129,0.2)"},
                    {"range": [0.5, 0.7], "color": "rgba(240,185,11,0.2)"},
                    {"range": [0.7, 1.0], "color": "rgba(246,70,93,0.2)"},
                ],
            },
        ))
        fig_iforest.update_layout(
            template="plotly_dark",
            paper_bgcolor=BG_DARK,
            font=dict(color=TEXT_PRIMARY, family=MONO_FONT),
            height=200, margin=dict(l=20, r=20, t=30, b=20),
        )

        st.markdown(f"""
        <div class="metric-card neutral" style="margin-bottom:8px;">
            <div class="metric-label">Isolation Forest Monitor</div>
            <div style="font-size:11px;color:#787B86;margin-top:2px;">Anomaly Score (0=safe, 1=critical)</div>
        </div>
        """, unsafe_allow_html=True)
        st.plotly_chart(fig_iforest, use_container_width=True)

        col_iforest_stat, col_iforest_score = st.columns(2)
        with col_iforest_stat:
            st.markdown("<div class='metric-label' style='text-align:center;'>Status</div>", unsafe_allow_html=True)
            st.markdown(f"<div style='text-align:center;'><span class='badge {status_cls}'>{status_label}</span></div>", unsafe_allow_html=True)
        with col_iforest_score:
            st.markdown("<div class='metric-label' style='text-align:center;'>Anomaly Score</div>", unsafe_allow_html=True)
            st.markdown(f"<div style='text-align:center;'><span style='font-family:{MONO_FONT};font-size:18px;font-weight:700;color:{status_color};'>{score:.3f}</span></div>", unsafe_allow_html=True)

    # ── ML Prediction Chart ───────────────────────────────────────────
    st.markdown("<div class='section-header'><span class='section-title'>Real-time Price vs. LSTM Prediction (Simulated)</span><div class='section-line'></div></div>", unsafe_allow_html=True)
    fig_ml = build_ml_placeholder_chart(df_chart, symbol)
    st.plotly_chart(fig_ml, use_container_width=True)

    # ── Model Info ────────────────────────────────────────────────────
    col_info1, col_info2, col_info3 = st.columns(3)
    with col_info1:
        st.markdown(f"""
        <div class="metric-card neutral">
            <div class="metric-label">XGBoost Model</div>
            <div class="metric-value" style="font-size:14px;">v2.1.0</div>
            <div class="metric-delta">Training: 72h window · Features: price, volume, RSI</div>
        </div>
        """, unsafe_allow_html=True)
    with col_info2:
        st.markdown(f"""
        <div class="metric-card neutral">
            <div class="metric-label">LSTM Model</div>
            <div class="metric-value" style="font-size:14px;">v1.8.3</div>
            <div class="metric-delta">Seq len: 60 · Hidden: 128 · Dropout: 0.2</div>
        </div>
        """, unsafe_allow_html=True)
    with col_info3:
        st.markdown(f"""
        <div class="metric-card neutral">
            <div class="metric-label">Isolation Forest</div>
            <div class="metric-value" style="font-size:14px;">v1.5.1</div>
            <div class="metric-delta">Contamination: 0.1% · Trees: 100 · Max samples: 256</div>
        </div>
        """, unsafe_allow_html=True)

# ============================================================================
# FOOTER
# ============================================================================
st.markdown(f"""
<div style="text-align:center;color:#444;font-size:11px;margin-top:24px;
            font-family:JetBrains Mono,monospace;">
    Last refresh: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} (UTC+7) ·
    CryptoPipeline Pro · Gold Layer Control Center
</div>
""", unsafe_allow_html=True)
