#!/usr/bin/env python3
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import psycopg2
from datetime import datetime, timedelta

st.set_page_config(page_title="Crypto Data Pipeline", layout="wide")

@st.cache_resource
def get_db_connection():
    return psycopg2.connect(
        host="postgres",
        port=5432,
        database="cryptopipeline",
        user="crypto_user",
        password="crypto_password"
    )

def fetch_latest_ohlcv(symbol="BTC-USD", limit=100):
    try:
        conn = get_db_connection()
        query = f"""
        SELECT timestamp, symbol, open, high, low, close, volume 
        FROM gold.gold_crypto_ohlcv 
        WHERE symbol = '{symbol}' 
        ORDER BY timestamp DESC 
        LIMIT {limit}
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df.sort_values("timestamp")
    except Exception as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()

def main():
    st.title("🚀 Real-time Crypto Data Pipeline")
    st.markdown("Trực quan hóa dữ liệu từ Kafka → Spark → PostgreSQL/HDFS")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        symbol = st.selectbox("Chọn tài sản:", ["BTC-USD", "ETH-USD"])
    with col2:
        timeframe = st.selectbox("Khung thời gian:", ["1m", "5m", "1h"])
    with col3:
        st.metric("Trạng thái", "🟢 Active")
    
    st.markdown("---")
    
    df = fetch_latest_ohlcv(symbol, limit=100)
    
    if not df.empty:
        # OHLCV Chart
        fig = go.Figure(data=[
            go.Candlestick(
                x=df['timestamp'],
                open=df['open'],
                high=df['high'],
                low=df['low'],
                close=df['close']
            )
        ])
        fig.update_layout(title=f"{symbol} - OHLCV", xaxis_title="Thời gian", yaxis_title="Giá (USD)")
        st.plotly_chart(fig, use_container_width=True)
        
        # Statistics
        st.markdown("### 📊 Thống kê")
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Giá hiện tại", f"${df['close'].iloc[-1]:.2f}")
        with col2:
            st.metric("Cao nhất", f"${df['high'].max():.2f}")
        with col3:
            st.metric("Thấp nhất", f"${df['low'].min():.2f}")
        with col4:
            st.metric("Volume", f"{df['volume'].sum():.2f}")
        
        st.markdown("### 📈 Dữ liệu")
        st.dataframe(df.tail(20), use_container_width=True)
    else:
        st.warning("Không có dữ liệu để hiển thị. Kiểm tra xem Kafka & Spark có chạy không.")

if __name__ == "__main__":
    main()
