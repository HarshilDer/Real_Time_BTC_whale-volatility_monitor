import streamlit as st
import pandas as pd
import psycopg2
import time
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ==========================================
# 1. PAGE SETUP & BLOOMBERG CSS
# ==========================================
st.set_page_config(page_title="BTC Terminal", page_icon="📈", layout="wide")

st.markdown("""
    <style>
    .stApp { background-color: #000000; }
    * { font-family: 'Courier New', Courier, monospace !important; }
    h1, h2, h3, p, span, .stMetric label { color: #FFB000 !important; }
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    div[data-testid="metric-container"] {
        background-color: #111111; border: 1px solid #333333; padding: 10px;
    }
    .dataframe { color: white !important; }
    div.row-widget.stRadio > div { flex-direction: row; }
    </style>
    """, unsafe_allow_html=True)


@st.cache_resource
def init_connection():
    return psycopg2.connect(
        host="timescaledb", port="5432", user="postgres", password="admin", dbname="crypto_db"
    )


conn = init_connection()


# ==========================================
# 2. DATA FETCHING
# ==========================================
def fetch_trade_data():
    query = "SELECT time, price, size, side, is_whale FROM btc_trades ORDER BY time DESC LIMIT 5000;"
    df = pd.read_sql_query(query, conn)
    if not df.empty:
        df['time'] = pd.to_datetime(df['time'])
        df = df.sort_values('time', ascending=True)
    return df


def fetch_volatility_data():
    # Re-adding this so we can see the alerts table
    query = "SELECT time, swing_percentage, time_window_seconds FROM volatility_alerts ORDER BY time DESC LIMIT 10;"
    df = pd.read_sql_query(query, conn)
    if not df.empty:
        df['time'] = pd.to_datetime(df['time'])
    return df


# ==========================================
# 3. UI LAYOUT & RENDERING
# ==========================================
st.title("⚡ BTC/USD INSTITUTIONAL TERMINAL")

trades_df = fetch_trade_data()
vol_df = fetch_volatility_data()

if not trades_df.empty:
    current_price = trades_df.iloc[-1]['price']
    total_volume = trades_df['size'].sum()
    whales_df = trades_df[trades_df['is_whale'] == True].sort_values('time', ascending=False)

    # --- TOP METRICS ---
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("LATEST TICK", f"${current_price:,.2f}")
    col2.metric("VOL (5K TRADES)", f"{total_volume:.2f} BTC")
    col3.metric("WHALE ALERTS", len(whales_df))
    col4.metric("VOLATILITY EVENTS", len(vol_df))

    st.markdown("---")

    # --- ROW 1: MAIN TERMINAL (Chart & Tape) ---
    chart_col, tape_col = st.columns([3, 1])

    with chart_col:
        tf_selection = st.radio("RESOLUTION:", ["10 Seconds", "1 Minute", "5 Minutes"], horizontal=True,
                                label_visibility="collapsed")
        tf_map = {"10 Seconds": "10s", "1 Minute": "1min", "5 Minutes": "5min"}

        # Resample logic
        df_resampled = trades_df.copy().set_index('time')
        ohlcv = df_resampled.resample(tf_map[tf_selection]).agg({
            'price': ['first', 'max', 'min', 'last'],
            'size': 'sum'
        }).dropna()
        ohlcv.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        ohlcv.reset_index(inplace=True)
        ohlcv = ohlcv.sort_values('time', ascending=True).tail(60)

        # Plotly Candlestick
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.03, row_heights=[0.7, 0.3])
        fig.add_trace(go.Candlestick(x=ohlcv['time'], open=ohlcv['Open'], high=ohlcv['High'], low=ohlcv['Low'],
                                     close=ohlcv['Close'], name='Price'), row=1, col=1)
        v_colors = ['#00FF00' if r['Close'] >= r['Open'] else '#FF0000' for i, r in ohlcv.iterrows()]
        fig.add_trace(go.Bar(x=ohlcv['time'], y=ohlcv['Volume'], marker_color=v_colors, name='Volume'), row=2, col=1)

        fig.update_layout(plot_bgcolor='#000000', paper_bgcolor='#000000', font=dict(color='#FFB000'),
                          margin=dict(l=0, r=0, t=0, b=0), xaxis_rangeslider_visible=False, height=500,
                          showlegend=False)
        fig.update_xaxes(showgrid=True, gridcolor='#222222')
        fig.update_yaxes(showgrid=True, gridcolor='#222222', side='right')
        st.plotly_chart(fig, use_container_width=True)

    with tape_col:
        st.subheader("▶ THE TAPE")
        tape_show = trades_df.tail(30).sort_values('time', ascending=False).copy()
        tape_show['time'] = tape_show['time'].dt.strftime('%H:%M:%S')


        def color_tape(row):
            color = '#00FF00' if row['side'] == 'buy' else '#FF0000'
            return [f'color: {color}; font-weight: bold; background-color: black;'] * len(row)


        st.dataframe(tape_show[['time', 'price', 'size', 'side']].style.apply(color_tape, axis=1),
                     use_container_width=True, hide_index=True, height=500)

    st.markdown("---")

    # --- ROW 2: ALERTS CONSOLE (Whales & Volatility) ---
    alert_col1, alert_col2 = st.columns(2)

    with alert_col1:
        st.subheader("🐋 WHALE WATCH (>0.5 BTC)")
        if not whales_df.empty:
            whales_display = whales_df.head(10).copy()
            whales_display['time'] = whales_display['time'].dt.strftime('%H:%M:%S')
            st.dataframe(whales_display[['time', 'price', 'size', 'side']], use_container_width=True, hide_index=True)
        else:
            st.info("Searching for whales in the deep...")

    with alert_col2:
        st.subheader("⚡ VOLATILITY ALERTS")
        if not vol_df.empty:
            vol_display = vol_df.copy()
            vol_display['time'] = vol_display['time'].dt.strftime('%H:%M:%S')
            vol_display['swing_percentage'] = vol_display['swing_percentage'].apply(lambda x: f"{x:.3f}%")
            st.dataframe(vol_display, use_container_width=True, hide_index=True)
        else:
            st.info("Market is currently stable.")

else:
    st.warning("⏳ CONNECTING TO DATASTREAM...")

time.sleep(1)
st.rerun()