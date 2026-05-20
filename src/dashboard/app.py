"""
Crypto Lakehouse Dashboard — Binance Pro Style
Query engine: Apache Trino (Nessie Catalog → Iceberg → MinIO)
"""

import time
from datetime import datetime, timezone

import streamlit as st
import trino
from trino.exceptions import TrinoQueryError

# ── Page Configuration ────────────────────────────────────────────────────────
st.set_page_config(
    page_title="CryptoLake Pro",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Trino Connection ─────────────────────────────────────────────────────────
@st.cache_resource(ttl=30, show_spinner=False)
def get_trino_connection():
    return trino.connect(
        host="localhost",
        port=8080,
        user="streamlit",
        catalog="nessie",
        schema="gold",
        http_scheme="http",
    )


def run_query(sql: str, params: dict | None = None) -> list[dict]:
    try:
        conn = get_trino_connection()
        cur = conn.cursor()
        cur.execute(sql, params=params)
        cols = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchall()
        return [dict(zip(cols, row)) for row in rows]
    except TrinoQueryError as e:
        st.error(f"Trino query error: {e}")
        return []


# ── Sidebar Controls ─────────────────────────────────────────────────────────
st.sidebar.header("🔧 Live Controls")

refresh_interval = st.sidebar.slider("Auto-refresh (seconds)", 5, 60, 10)
symbol_filter    = st.sidebar.text_input("Symbol filter", value="BTC-USD").strip().upper()
st.sidebar.markdown("---")
st.sidebar.caption(f"⏱ Updated: {datetime.now(timezone.utc):%H:%M:%S} UTC")

# ── Header ────────────────────────────────────────────────────────────────────
st.title("📊 CryptoLake Pro — Live OHLCV + ML Signals")
st.caption("Powered by Apache Trino · Iceberg · MinIO")

# ── Key Metrics Row ──────────────────────────────────────────────────────────
def render_metrics(symbol: str):
    rows = run_query(
        """
        SELECT
            symbol,
            close,
            open,
            high,
            low,
            volume,
            log_return,
            volatility_30m,
            z_score,
            window_start
        FROM nessie.gold.crypto_ohlcv
        WHERE symbol = '%s'
        ORDER BY window_start DESC
        LIMIT 1
        """
        % symbol
    )

    if not rows:
        st.warning(f"No data found for `{symbol}`. Is the stream running?")
        return

    row = rows[0]

    m_close   = row.get("close")           or 0.0
    m_open    = row.get("open")            or 0.0
    m_high    = row.get("high")            or 0.0
    m_low     = row.get("low")             or 0.0
    m_vol     = row.get("volume")          or 0.0
    m_ret     = row.get("log_return")      or 0.0
    m_vol30   = row.get("volatility_30m")  or 0.0
    m_z       = row.get("z_score")         or 0.0

    # Colour helpers
    def colour_delta(val, suffix="", pos="🟢", neg="🔴"):
        arrow = pos if val >= 0 else neg
        return f"{arrow} {val:+.6f}{suffix}"

    mc1, mc2, mc3, mc4 = st.columns(4)
    mc1.metric("Close (USD)",      f"${m_close:,.4f}",  colour_delta(m_ret, " ret"))
    mc2.metric("24h Range",        f"${m_high:,.4f} / ${m_low:,.4f}")
    mc3.metric("Volume",           f"{m_vol:,.2f}")
    mc4.metric("VWAP",             f"${(m_open + m_close) / 2:,.4f}")

    ml1, ml2, ml3, ml4 = st.columns(4)
    ml1.metric("Log Return",       f"{m_ret:+.6f}")
    ml2.metric("Volatility (30m)", f"{m_vol30:+.6f}")
    ml3.metric("Z-Score",         f"{m_z:+.4f}",       colour_delta(m_z))
    ml4.metric("Symbol",           symbol)

    return row


# ── OHLCV + Volume Chart ─────────────────────────────────────────────────────
def render_ohlcv_chart(symbol: str):
    rows = run_query(
        """
        SELECT window_start, open, high, low, close, volume
        FROM nessie.gold.crypto_ohlcv
        WHERE symbol = '%s'
        ORDER BY window_start DESC
        LIMIT 100
        """
        % symbol
    )

    if len(rows) < 2:
        st.info("Not enough data points to render chart yet.")
        return

    rows = list(reversed(rows))

    try:
        import plotly.graph_objs as go
        from plotly.subplots import make_subplots

        fig = make_subplots(
            rows=2, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.08,
            row_heights=[0.7, 0.3],
            subplot_titles=("Price (OHLC)", "Volume"),
        )

        times  = [r["window_start"] for r in rows]
        opens  = [r["open"]  for r in rows]
        highs  = [r["high"]  for r in rows]
        lows   = [r["low"]   for r in rows]
        closes = [r["close"] for r in rows]
        vols   = [r["volume"] for r in rows]

        # Candlestick
        fig.add_trace(
            go.Candlestick(
                x=times, open=opens, high=highs, low=lows, close=closes,
                increasing_line_color="#00c853",
                decreasing_line_color="#ff1744",
                name="OHLC",
            ),
            row=1, col=1,
        )

        # Volume bars
        fig.add_trace(
            go.Bar(
                x=times, y=vols,
                marker_color=["#00c853" if c >= o else "#ff1744" for c, o in zip(closes, opens)],
                name="Volume",
                opacity=0.7,
            ),
            row=2, col=1,
        )

        fig.update_layout(
            height=600,
            template="plotly_dark",
            showlegend=False,
            xaxis_rangeslider_visible=False,
            title=f"{symbol} — OHLCV (Last 100 Candles)",
        )
        fig.update_xaxes(title_text="Time (UTC)", row=2, col=1)
        fig.update_yaxes(title_text="Price (USD)", row=1, col=1)
        fig.update_yaxes(title_text="Volume",      row=2, col=1)

        st.plotly_chart(fig, use_container_width=True)

    except ImportError:
        st.line_chart(
            data=[
                {"time": r["window_start"], "close": r["close"]}
                for r in rows
            ],
            x="time",
            y="close",
            height=400,
        )


# ── ML Features Table ────────────────────────────────────────────────────────
def render_ml_table(symbol: str):
    rows = run_query(
        """
        SELECT
            window_start,
            log_return,
            volatility_30m,
            z_score,
            close,
            volume,
            trade_count
        FROM nessie.gold.crypto_ohlcv
        WHERE symbol = '%s'
        ORDER BY window_start DESC
        LIMIT 50
        """
        % symbol
    )

    if not rows:
        return

    st.subheader("📈 ML Feature Stream")

    import pandas as pd

    df = pd.DataFrame(rows)
    df["window_start"] = pd.to_datetime(df["window_start"]).dt.strftime("%H:%M:%S")

    for col in ["log_return", "volatility_30m", "z_score", "close", "volume"]:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: f"{float(x):+.6f}" if x is not None else "—")

    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        height=320,
    )


# ── Auto-refresh ──────────────────────────────────────────────────────────────
if "refresh_key" not in st.session_state:
    st.session_state.refresh_key = 0

placeholder = st.empty()

with placeholder.container():
    render_metrics(symbol_filter)
    render_ohlcv_chart(symbol_filter)
    render_ml_table(symbol_filter)

time.sleep(refresh_interval)
st.session_state.refresh_key += 1
st.rerun()
