import streamlit as st
from config import Config

st.set_page_config(page_title="Trading Dashboard", layout="wide")

# Title
st.markdown("# Trading Dashboard - Phase 1 MVP")
st.markdown("---")

# Test config
st.success("Configuration loaded successfully!")

# Show paths
st.markdown("### System Paths")
col1, col2 = st.columns(2)

with col1:
    st.info(f"Data Directory: {Config.DATA_DIR}")
    st.info(f"NSE Raw: {Config.NSE_RAW_DIR}")
    st.info(f"BSE Raw: {Config.BSE_RAW_DIR}")

with col2:
    st.info(f"Watchlist: {Config.WATCHLIST_DIR}")
    st.info(f"Logs: {Config.LOGS_DIR}")

# Test data
st.markdown("---")
st.markdown("### Sample Signal")

import pandas as pd
sample_data = pd.DataFrame({
    'Symbol': ['TCS', 'INFY', 'RELIANCE'],
    'Price': [3450, 1520, 2450],
    'Delivery %': [78.2, 82.5, 75.8],
    'Momentum': [15.3, 12.1, 18.7]
})

st.dataframe(sample_data, use_container_width=True)

st.markdown("---")
st.success("Dashboard is working! Ready for full implementation.")
