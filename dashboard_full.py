import streamlit as st
import pandas as pd
import os
from datetime import datetime
from config import Config

st.set_page_config(page_title="Trading Dashboard", layout="wide")

# FZ STANDARD THEME WITH HOVER EFFECTS
st.markdown('''<style>
    :root { --card: rgba(255,255,255,.08); --border: rgba(255,255,255,.15); --accent:#8ea2ff; }
    body { background: linear-gradient(135deg,#0f0c29,#302b63,#24243e); color:#fff; font-family: 'Segoe UI', sans-serif; }
    .section { margin: 24px 0 12px 0; font-weight: 800; font-size: 24px; 
        background: linear-gradient(135deg,#8ea2ff,#0b7cff); -webkit-background-clip:text; 
        -webkit-text-fill-color:transparent; }
    .subsection { margin: 16px 0 8px 0; font-weight: 700; font-size: 16px; color: var(--accent); }
    .card { padding:20px;border-radius:16px;background:var(--card);border:1px solid var(--border);text-align:center; transition: all 0.3s ease; }
    .card:hover { transform:translateY(-5px); box-shadow:0 8px 25px rgba(142,162,255,0.4); border-color:#8ea2ff; }
    .metric .label { font-size:13px;opacity:.75;text-transform:uppercase;letter-spacing:1px }
    .metric .value { font-size:32px;font-weight:900;color:var(--accent);margin-top:8px }
    .hero { background: linear-gradient(135deg,#667eea,#764ba2); padding:32px;border-radius:24px; margin:12px 0 24px 0; box-shadow:0 16px 48px rgba(102,126,234,.4);text-align:center}
    .hero h1 { margin:0;font-weight:900;font-size:48px }
    .hero p { margin:8px 0 0;font-size:18px;opacity:.95 }
    
    /* Dataframe hover */
    .stDataFrame tbody tr { transition: all 0.3s ease; }
    .stDataFrame tbody tr:hover { background: rgba(142,162,255,0.2) !important; transform: scale(1.02); cursor: pointer; }
    
    /* Button hover */
    .stButton button { transition: all 0.3s ease; background: linear-gradient(135deg,#667eea,#764ba2); }
    .stButton button:hover { background: linear-gradient(135deg,#764ba2,#667eea); transform: translateY(-2px); box-shadow: 0 6px 20px rgba(142,162,255,0.5); }
    
    /* Input hover */
    .stTextInput input:hover, .stNumberInput input:hover { border-color: #8ea2ff; box-shadow: 0 0 15px rgba(142,162,255,0.3); }
    
    /* Tab hover */
    .stTabs [data-baseweb="tab"]:hover { background: rgba(142,162,255,0.2); transform: scale(1.05); transition: all 0.2s ease; }
    
    /* Success box hover */
    .stSuccess:hover { transform: scale(1.02); box-shadow: 0 4px 15px rgba(72,187,120,0.2); transition: all 0.3s ease; }
    
</style>''', unsafe_allow_html=True)

def log_signal_to_history(symbol, exchange, close, deliv_per, momentum_score):
    """Automatically log signal to history file"""
    history_file = "data/signal_history.csv"
    today = datetime.now().strftime("%Y-%m-%d")
    
    if not os.path.exists(history_file):
        with open(history_file, 'w') as f:
            f.write("Date,Symbol,Exchange,Price,Delivery_Percent,Momentum_Score\n")
    
    try:
        df_history = pd.read_csv(history_file)
        if ((df_history["Date"] == today) & (df_history["Symbol"] == symbol)).any():
            return
    except:
        df_history = pd.DataFrame()
    
    with open(history_file, 'a') as f:
        f.write(f"{today},{symbol},{exchange},{close},{deliv_per:.2f},{momentum_score:.1f}\n")


def metric(label, value):
    st.markdown(f"<div class='card metric'><div class='label'>{label}</div><div class='value'>{value}</div></div>", unsafe_allow_html=True)

page = st.sidebar.radio("Navigation", ["Dashboard", "Data Health", "Signals", "Verify Conditions", "Watchlist", "Win Rate"])

# DASHBOARD
if page == "Dashboard":
    st.markdown("<div class='hero'><h1>Trading Dashboard</h1><p>Progressive Spike Strategy • Phase 1 MVP</p></div>", unsafe_allow_html=True)
    
    try:
        from progressive_screener import ProgressiveSpiker
        LIVE_FILE = "data/dashboard_cloud.csv"

        
        if os.path.exists(LIVE_FILE):
            df = pd.read_csv(LIVE_FILE)
            
            # Validate DATE column
            if "DATE" not in df.columns or df["DATE"].isna().all():
                st.error("⚠️ DATE column missing - run auto_update_smart.py")
                st.stop()
            
            df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
            latest_date = df["DATE"].max()
            
            # Exchange split
            exch_counts = df["EXCHANGE"].value_counts()
            nse_count = exch_counts.get("NSE", 0)
            bse_count = exch_counts.get("BSE", 0)
            
            c1, c2, c3, c4 = st.columns(4)
            with c1: metric("Total Stocks", f"{len(df):,}")
            with c2: metric("NSE Stocks", f"{nse_count:,}")
            with c3: metric("BSE Stocks", f"{bse_count:,}")
            with c4: metric("Data as of", latest_date.strftime("%d %b %Y"))
            
            st.markdown("<div class='section'>12-Condition Signals</div>", unsafe_allow_html=True)
            sig = ProgressiveSpiker(df).get_signals()
            if len(sig) > 0:
                st.success(f"✅ Found {len(sig)} signals passing all 12 conditions")
                cols = [c for c in ["SYMBOL","EXCHANGE","CLOSE","DELIV_PER","DELIVERY_TURNOVER","ATW"] if c in sig.columns]
                st.dataframe(sig[cols].head(30), use_container_width=True, height=500)
                
                st.markdown("<div class='subsection'>Add Signal to Watchlist</div>", unsafe_allow_html=True)
                col1, col2 = st.columns([2, 1])
                with col1:
                    selected_stock = st.selectbox("📌 Select Stock", sig["SYMBOL"].tolist())
                with col2:
                    if st.button("➕ Add", use_container_width=True):
                        row = sig[sig["SYMBOL"] == selected_stock].iloc[0]
                        try:
                            from watchlist_manager import WatchlistManager
                            wm = WatchlistManager()
                            added, msg = wm.add_stock(selected_stock, row.get("CLOSE", 0), row.get("DELIV_PER", 0), 0)
                            if added:
                                st.success(f"✅ {msg}")
                            else:
                                st.warning(f"⚠️ {msg}")
                        except Exception as e:
                            st.error(f"Error: {e}")
            else:
                st.info("📭 No signals found")
        else:
            st.warning("⚠️ No data file. Run auto_update_smart.py first")
    except Exception as e:
        st.error(f"Error: {e}")

# DATA HEALTH
elif page == "Data Health":
    st.markdown("<div class='section'>Data Health & Download</div>", unsafe_allow_html=True)
    
    tabs = st.tabs(["📥 Download", "📊 Primary File", "📈 NSE Bhav", "📦 NSE Delivery", "🏢 BSE Data"])
    
    with tabs[0]:
        st.markdown("<div class='subsection'>Download NSE & BSE Data</div>", unsafe_allow_html=True)
        
        if st.button("🚀 Download All", use_container_width=True):
            from data_downloader_improved import DataDownloaderImproved
            with st.spinner("⏳ Downloading..."):
                res = DataDownloaderImproved().download_all()
            
            st.markdown("<div class='subsection'>Download Results</div>", unsafe_allow_html=True)
            
            col1, col2 = st.columns(2)
            with col1:
                st.write("**NSE India**")
                if res["nse_bhav"][0]:
                    st.success("✅ NSE Bhav")
                else:
                    st.error(f"❌ NSE Bhav: {res['nse_bhav'][1]}")
                
                if res["nse_delivery"][0]:
                    st.success("✅ NSE Delivery")
                else:
                    st.warning(f"⚠️ NSE Delivery: {res['nse_delivery'][1]}")
            
            with col2:
                st.write("**BSE India**")
                if res["bse"][0]:
                    st.success("✅ BSE Data")
                    if isinstance(res["bse"][1], dict):
                        st.caption(f"Source: {res['bse'][1].get('source', 'Direct')}")
                else:
                    st.warning(f"⚠️ BSE: {res['bse'][1]}")
    
    with tabs[1]:
        st.markdown("<div class='subsection'>Primary Combined File</div>", unsafe_allow_html=True)
        LIVE_FILE = "data/combined_dashboard_live.csv"

        if os.path.exists(LIVE_FILE):
            df = pd.read_csv(LIVE_FILE)
            
            if "DATE" in df.columns:
                df["DATE"] = pd.to_datetime(df["DATE"], errors='coerce')
                latest_date = df["DATE"].max()
                oldest_date = df["DATE"].min()
                date_info = f"{oldest_date.strftime('%d %b %Y')} to {latest_date.strftime('%d %b %Y')}"
            else:
                date_info = "Unknown"
            
            exch_counts = df["EXCHANGE"].value_counts() if "EXCHANGE" in df.columns else {}
            nse_count = exch_counts.get("NSE", 0)
            bse_count = exch_counts.get("BSE", 0)
            
            c1, c2, c3, c4 = st.columns(4)
            with c1: metric("Total Stocks", f"{len(df):,}")
            with c2: metric("NSE", f"{nse_count:,}")
            with c3: metric("BSE", f"{bse_count:,}")
            with c4: metric("Date Range", date_info)
            
            st.dataframe(df.head(30), use_container_width=True)
        else:
            st.info("No combined file")
    
    with tabs[2]:
        st.markdown("<div class='subsection'>NSE Bhav Copy</div>", unsafe_allow_html=True)
        if os.path.exists(Config.NSE_RAW_DIR):
            import re
            files = sorted([f for f in os.listdir(Config.NSE_RAW_DIR) if "bhav" in f.lower()])
            if files:
                latest = files[-1]
                date_match = re.search(r'(\d{8})', latest)
                file_date = date_match.group(1) if date_match else "Unknown"
                
                df = pd.read_csv(os.path.join(Config.NSE_RAW_DIR, latest))
                
                c1, c2, c3, c4 = st.columns(4)
                with c1: metric("Latest File", latest[-20:] if len(latest) > 20 else latest)
                with c2: metric("Data Date", f"{file_date[:4]}-{file_date[4:6]}-{file_date[6:]}")
                with c3: metric("Records", f"{len(df):,}")
                with c4: metric("Symbols", f"{df['SYMBOL'].nunique() if 'SYMBOL' in df.columns else 0:,}")
                
                st.dataframe(df.head(20), use_container_width=True)
    
    with tabs[3]:
        st.markdown("<div class='subsection'>NSE Delivery (MTO)</div>", unsafe_allow_html=True)
        if os.path.exists(Config.NSE_RAW_DIR):
            import re
            files = sorted([f for f in os.listdir(Config.NSE_RAW_DIR) if "delivery" in f.lower()])
            if files:
                latest = files[-1]
                date_match = re.search(r'(\d{8})', latest)
                file_date = date_match.group(1) if date_match else "Unknown"
                
                df = pd.read_csv(os.path.join(Config.NSE_RAW_DIR, latest))
                
                c1, c2, c3, c4 = st.columns(4)
                with c1: metric("Latest File", latest[-20:] if len(latest) > 20 else latest)
                with c2: metric("Data Date", f"{file_date[:4]}-{file_date[4:6]}-{file_date[6:]}")
                with c3: metric("Records", f"{len(df):,}")
                with c4: metric("Columns", len(df.columns))
                
                st.dataframe(df.head(20), use_container_width=True)
    
    with tabs[4]:
        st.markdown("<div class='subsection'>BSE Data</div>", unsafe_allow_html=True)
        if os.path.exists(Config.BSE_RAW_DIR):
            import re
            files = sorted([f for f in os.listdir(Config.BSE_RAW_DIR)])
            if files:
                latest = files[-1]
                date_match = re.search(r'(\d{8})', latest)
                file_date = date_match.group(1) if date_match else "Unknown"
                
                df = pd.read_csv(os.path.join(Config.BSE_RAW_DIR, latest))
                
                c1, c2, c3, c4 = st.columns(4)
                with c1: metric("Latest File", latest[-20:] if len(latest) > 20 else latest)
                with c2: metric("Data Date", f"{file_date[:4]}-{file_date[4:6]}-{file_date[6:]}")
                with c3: metric("Records", f"{len(df):,}")
                with c4: metric("Symbols", f"{df.get('SC_CODE', df.iloc[:, 0]).nunique()}")
                
                st.dataframe(df.head(20), use_container_width=True)
            else:
                st.info("No BSE files - Try downloading")


# SIGNALS PAGE
elif page == "Signals":
    st.markdown("<div class='section'>12-Condition Signals</div>", unsafe_allow_html=True)
    
    LIVE_FILE = "data/combined_dashboard_live.csv"

    if os.path.exists(LIVE_FILE):
        df = pd.read_csv(LIVE_FILE)
        
        # Validate DATE
        if "DATE" not in df.columns or df["DATE"].isna().all():
            st.error("⚠️ DATE column missing - run auto_update_smart.py")
            st.stop()
        
        df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
        latest_date = df["DATE"].max()
        
        # Exchange split
        exch_counts = df["EXCHANGE"].value_counts()
        nse_count = exch_counts.get("NSE", 0)
        bse_count = exch_counts.get("BSE", 0)
        
        c1, c2, c3, c4 = st.columns(4)
        with c1: metric("Total Stocks", f"{len(df):,}")
        with c2: metric("NSE", f"{nse_count:,}")
        with c3: metric("BSE", f"{bse_count:,}")
        with c4: metric("Data as of", latest_date.strftime("%d %b %Y"))
        
        st.markdown("<div class='subsection'>12-Condition Signals</div>", unsafe_allow_html=True)
        
        # Use ProgressiveSpiker instead of duplicate logic
        from progressive_screener import ProgressiveSpiker
        signals = ProgressiveSpiker(df).get_signals()
        
        if len(signals) > 0:
            # Calculate Momentum Score
            deliv_momentum = ((signals["DELIV_PER"] - signals["DELIV_PER_1W"]) / signals["DELIV_PER_1W"] * 100).clip(0, 33)
            turnover_momentum = ((signals["DELIVERY_TURNOVER"] - signals["DELIVERY_TURNOVER_1W"]) / signals["DELIVERY_TURNOVER_1W"] * 100).clip(0, 33)
            atw_momentum = ((signals["ATW"] - signals["ATW_1W"]) / signals["ATW_1W"] * 100).clip(0, 34)
            
            signals["MOMENTUM_SCORE"] = (deliv_momentum + turnover_momentum + atw_momentum).round(1)
            signals = signals.sort_values("MOMENTUM_SCORE", ascending=False)
            
            # Auto-log signals to history
            for idx, row in signals.iterrows():
                log_signal_to_history(
                    row["SYMBOL"], 
                    row.get("EXCHANGE", "NSE"),
                    row["CLOSE"], 
                    row["DELIV_PER"], 
                    row["MOMENTUM_SCORE"]
                )
            
            st.success(f"✅ Found {len(signals)} signals passing all 12 conditions (auto-logged to history)")
            
            # Display signals
            display_cols = ["SYMBOL", "EXCHANGE", "CLOSE", "DELIV_PER", "DELIVERY_TURNOVER", "ATW", "MOMENTUM_SCORE"]
            available_cols = [col for col in display_cols if col in signals.columns]
            st.dataframe(signals[available_cols], use_container_width=True)
            
            # Signal History Section
            st.markdown("<div class='subsection'>Signal History</div>", unsafe_allow_html=True)
            
            if os.path.exists("data/signal_history.csv"):
                df_history = pd.read_csv("data/signal_history.csv")
                
                # Show stats
                c1, c2, c3 = st.columns(3)
                with c1: metric("Total Signals Logged", f"{len(df_history):,}")
                with c2: metric("Unique Stocks", f"{df_history['Symbol'].nunique():,}")
                with c3: metric("Date Range", f"{df_history['Date'].min()} to {df_history['Date'].max()}")
                
                # Show last 50 signals
                st.dataframe(df_history.tail(50), use_container_width=True)
                
                # Download button
                csv = df_history.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Download Full Signal History CSV",
                    data=csv,
                    file_name=f"signal_history_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                    mime="text/csv",
                    key="download_signals"
                )
            else:
                st.info("No signal history yet. Signals will be auto-logged when they appear.")
            
        else:
            st.warning("No signals found today")
    else:
        st.info("No data available. Run auto_update_smart.py first.")

# VERIFY CONDITIONS
elif page == "Verify Conditions":
    st.markdown("<div class='section'>Verify 12-Condition Compliance</div>", unsafe_allow_html=True)
    
    try:
        from progressive_screener import ProgressiveSpiker
        LIVE_FILE = "data/combined_dashboard_live.csv"

        if os.path.exists(LIVE_FILE):
            df = pd.read_csv(LIVE_FILE)
            sig = ProgressiveSpiker(df).get_signals()
            if len(sig) > 0:
                st.success(f"Found {len(sig)} signals - Checking first 5...")
                
                for idx, row in sig.head(5).iterrows():
                    with st.expander(f"🔍 {row.get('SYMBOL', 'N/A')}"):
                        st.markdown("<div class='subsection'>Baseline (3 Conditions)</div>", unsafe_allow_html=True)
                        st.write(f"1. Delivery % ≥ 50: {row.get('DELIV_PER', 0):.2f} {'✅' if row.get('DELIV_PER', 0) >= 50 else '❌'}")
                        st.write(f"2. Turnover ≥ 5M: {row.get('DELIVERY_TURNOVER', 0):,.0f} {'✅' if row.get('DELIVERY_TURNOVER', 0) >= 5000000 else '❌'}")
                        st.write(f"3. ATW ≥ 20K: {row.get('ATW', 0):,.0f} {'✅' if row.get('ATW', 0) >= 20000 else '❌'}")
                        
                        st.markdown("<div class='subsection'>Progression (9 Conditions)</div>", unsafe_allow_html=True)
                        d = row.get('DELIV_PER',0); d1w = row.get('DELIV_PER_1W',0); d1m = row.get('DELIV_PER_1M',0); d3m = row.get('DELIV_PER_3M',0)
                        st.write(f"4-6. Delivery: {d:.2f} > {d1w:.2f} > {d1m:.2f} > {d3m:.2f} {'✅' if d > d1w > d1m > d3m else '❌'}")
                        
                        t = row.get('DELIVERY_TURNOVER',0); t1w = row.get('DELIVERY_TURNOVER_1W',0); t1m = row.get('DELIVERY_TURNOVER_1M',0); t3m = row.get('DELIVERY_TURNOVER_3M',0)
                        st.write(f"7-9. Turnover: {t:,.0f} > {t1w:,.0f} > {t1m:,.0f} > {t3m:,.0f} {'✅' if t > t1w > t1m > t3m else '❌'}")
                        
                        a = row.get('ATW',0); a1w = row.get('ATW_1W',0); a1m = row.get('ATW_1M',0); a3m = row.get('ATW_3M',0)
                        st.write(f"10-12. ATW: {a:,.0f} > {a1w:,.0f} > {a1m:,.0f} > {a3m:,.0f} {'✅' if a > a1w > a1m > a3m else '❌'}")
            else:
                st.info("No signals")
        else:
            st.warning("No data")
    except Exception as e:
        st.error(f"Error: {e}")

# WATCHLIST
elif page == "Watchlist":
    st.markdown("<div class='section'>Active Watchlist</div>", unsafe_allow_html=True)
    try:
        from watchlist_manager import WatchlistManager
        wm = WatchlistManager()
        
        if len(wm.active) > 0:
            # Update prices first
            LIVE_FILE = "data/combined_dashboard_live.csv"

            if os.path.exists(LIVE_FILE):
                df = pd.read_csv(LIVE_FILE)
                wm.auto_update_prices(df)
                wm = WatchlistManager()  # Reload after update
            
            st.markdown("<div class='subsection'>Manage Positions</div>", unsafe_allow_html=True)
            
            # Use data_editor for inline editing
            display_cols = ["symbol", "entry_price", "current_price", "tp", "sl", "entry_date"]
            display_df = wm.active[display_cols].copy()
            
            st.dataframe(display_df, use_container_width=True, height=400)
            
            # Delete section with explicit stock selection
            st.markdown("<div class='subsection'>Close Position (Record Trade)</div>", unsafe_allow_html=True)
            
            # Create a unique list of stocks
            stock_list = wm.active["symbol"].unique().tolist()
            
            col1, col2 = st.columns([3, 1])
            with col1:
                # Store selection in session state
                if "close_stock_idx" not in st.session_state:
                    st.session_state.close_stock_idx = 0
                
                selected_idx = st.selectbox(
                    "Select stock to close",
                    range(len(stock_list)),
                    format_func=lambda x: f"{stock_list[x]} - Entry: ₹{wm.active[wm.active['symbol']==stock_list[x]]['entry_price'].values[0]:.2f}",
                    key="close_selector"
                )
                selected_stock = stock_list[selected_idx]
                
            with col2:
                exit_price = st.number_input("Exit Price", value=0.0, min_value=0.0, key="exit_input")
            
            col3, col4, col5 = st.columns([1, 1, 2])
            with col3:
                if st.button("✅ Close & Record", key="close_btn", type="primary"):
                    if exit_price > 0:
                        success = wm.close_position(selected_stock, exit_price, "manual")
                        if success:
                            st.success(f"✅ Closed {selected_stock} at ₹{exit_price}")
                            st.info("👉 Refresh page to see updated list")
                        else:
                            st.error("Failed to close position")
                    else:
                        st.error("⚠️ Enter exit price > 0")
            
            with col4:
                if st.button("🗑️ Remove Only", key="delete_btn", type="secondary"):
                    success = wm.delete_stock(selected_stock)
                    if success:
                        st.warning(f"🗑️ Removed {selected_stock} (not recorded)")
                        st.info("👉 Refresh page")
                    else:
                        st.error("Failed to remove")
            
            with col5:
                st.caption("Close = Record trade | Remove = Delete without tracking")
            
            st.divider()
            st.markdown("<div class='subsection'>Export Data</div>", unsafe_allow_html=True)
            csv = wm.active.to_csv(index=False)
            st.download_button(
                "📥 Download Watchlist CSV",
                csv,
                f"watchlist_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                "text/csv",
                key="download_btn"
            )
            
            # Show summary
            st.markdown("<div class='subsection'>Quick Stats</div>", unsafe_allow_html=True)
            total_value = (wm.active["current_price"] * 100).sum()
            total_pl = ((wm.active["current_price"] - wm.active["entry_price"]) * 100).sum()
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Active Positions", len(wm.active))
            with col2:
                st.metric("Approx Value", f"₹{total_value:,.0f}")
            with col3:
                st.metric("Unrealized P&L", f"₹{total_pl:,.0f}", delta=f"{(total_pl/total_value*100):.1f}%")
        
        else:
            st.info("📭 No active positions - Add signals from Dashboard")
            
    except Exception as e:
        st.error(f"Error: {str(e)}")
        st.exception(e)

# WIN RATE
elif page == "Win Rate":
    st.markdown("<div class='section'>Performance Report</div>", unsafe_allow_html=True)
    try:
        from watchlist_manager import WatchlistManager
        wm = WatchlistManager()
        stats = wm.get_win_rate()
        
        st.markdown("<div class='subsection'>Trading Statistics</div>", unsafe_allow_html=True)
        c1, c2, c3, c4 = st.columns(4)
        with c1: metric("Total", stats["total"])
        with c2: metric("Winners", stats["winners"])
        with c3: metric("Win Rate %", f"{stats['win_rate']:.1f}%")
        with c4: metric("Avg Return %", f"{stats['avg_return']:.1f}%")
    except Exception as e:
        st.error(f"Error: {e}")
