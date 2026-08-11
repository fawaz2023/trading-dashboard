import streamlit as st
import pandas as pd
import os
from datetime import datetime
from config import Config

st.set_page_config(page_title="Trading Dashboard", layout="wide")

@st.cache_data(ttl=3600)
def load_live_data(filepath, file_mtime=0):
    if os.path.exists(filepath):
        df = pd.read_csv(filepath)
        if "DATE" in df.columns:
            df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
        return df
    return None

@st.cache_data(ttl=3600)
def get_cached_signals(df_filt):
    from progressive_screener import ProgressiveSpiker
    return ProgressiveSpiker(df_filt).get_signals()

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
    
    /* Premium Button */
    div.stButton > button:first-child { background: linear-gradient(135deg, #6c5ce7 0%, #8e7bff 100%); color: white; border: none; padding: 10px 24px; border-radius: 8px; font-weight: bold; letter-spacing: 0.5px; width: 100%; transition: all 0.3s ease; }
    div.stButton > button:first-child:hover { background: linear-gradient(135deg, #5a4bd1 0%, #7a6bf2 100%); transform: translateY(-1px); box-shadow: 0 4px 12px rgba(108, 92, 231, 0.3); }    
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


def style_actionable_band(val):
    try:
        v = float(val)
        if 60 <= v <= 80:
            return 'background-color: rgba(72, 187, 120, 0.3); color: #fff; font-weight: bold;'
        elif v > 80:
            return 'color: rgba(255, 255, 255, 0.3);'
        return ''
    except:
        return ''

def metric(label, value):
    st.markdown(f"<div class='card metric'><div class='label'>{label}</div><div class='value'>{value}</div></div>", unsafe_allow_html=True)

page = st.sidebar.radio("Navigation", ["Dashboard", "Signals", "Institutional Signals", "Verify Conditions", "Watchlist", "Win Rate", "Data Health"])
st.sidebar.divider()
if st.sidebar.button("🔄 Force Data Refresh", help="Clear cache and reload latest data from disk", use_container_width=True):
    st.cache_data.clear()
    st.rerun()
exclude_t2t = st.sidebar.checkbox("🚫 Hide 100% Delivery (T2T)", value=True)


# DASHBOARD
if page == "Dashboard":
    st.markdown("<div class='hero'><h1>Trading Dashboard</h1><p>Progressive Spike Strategy • Phase 1 MVP</p></div>", unsafe_allow_html=True)
    
    try:
        from progressive_screener import ProgressiveSpiker
        LIVE_FILE = "data/dashboard_cloud.csv"

        
        if os.path.exists(LIVE_FILE):
            df = load_live_data(LIVE_FILE, os.path.getmtime(LIVE_FILE))
            
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
            if exclude_t2t and "EVER_100_DELIV" in df.columns:
                df_filt = df[df["EVER_100_DELIV"] == False]
            else:
                df_filt = df
            sig = get_cached_signals(df_filt)
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
    st.markdown("<div class='section'>Data Health & Sync Status</div>", unsafe_allow_html=True)
    
    import json
    status_file = "data/data_status.json"
    status_data = {}
    if os.path.exists(status_file):
        try:
            with open(status_file, 'r', encoding='utf-8') as f:
                status_data = json.load(f)
        except:
            pass
            
    # Load total counts from cloud file if available
    LIVE_FILE = "data/dashboard_cloud.csv"
    total_count, nse_count, bse_count = 0, 0, 0
    df = None
    if os.path.exists(LIVE_FILE):
        df = load_live_data(LIVE_FILE, os.path.getmtime(LIVE_FILE))
        if df is not None and not df.empty:
            total_count = len(df)
            exch_counts = df["EXCHANGE"].value_counts() if "EXCHANGE" in df.columns else {}
            nse_count = exch_counts.get("NSE", 0)
            bse_count = exch_counts.get("BSE", 0)
    
    st.subheader("System Data Status")
    
    col1, col2, col3, col4 = st.columns(4)
    
    def get_status_ui(date_str):
        if date_str and date_str != "Missing":
            return "🟢 Synced"
        return "🔴 Missing"

    nse_bhav_date = status_data.get("nse_bhav_date", "Unknown")
    nse_deliv_date = status_data.get("nse_deliv_date", "Unknown")
    bse_bhav_date = status_data.get("bse_bhav_date", "Unknown")
    bse_deliv_date = status_data.get("bse_deliv_date", "Unknown")

    with col1:
        st.metric(label="📈 NSE Bhavcopy", value=f"{nse_bhav_date}", delta=get_status_ui(nse_bhav_date), delta_color="off")
    with col2:
        st.metric(label="📦 NSE Delivery", value=f"{nse_deliv_date}", delta=get_status_ui(nse_deliv_date), delta_color="off")
    with col3:
        st.metric(label="🏦 BSE Bhavcopy", value=f"{bse_bhav_date}", delta=get_status_ui(bse_bhav_date), delta_color="off")
    with col4:
        st.metric(label="🚚 BSE Delivery", value=f"{bse_deliv_date}", delta=get_status_ui(bse_deliv_date), delta_color="off")
        
    st.markdown("---")
    
    # 2. Action Center (Middle)
    st.subheader("Data Downloader & Sync")
    
    col_dl1, col_dl2 = st.columns([3, 1])
    with col_dl1:
        st.write(f"🌐 **Total Universe**: {total_count:,} Stocks ({nse_count:,} NSE / {bse_count:,} BSE)")
        last_run = status_data.get('last_run', 'Unknown')
        st.info(f"💡 The background sync last ran on: **{last_run}**.")
    
    with col_dl2:
        if st.button("🚀 Run Local Downloader", use_container_width=True):
            try:
                from data_downloader_improved import DataDownloaderImproved
                with st.spinner("⏳ Downloading raw files (Local Only)..."):
                    res = DataDownloaderImproved().download_all()
                st.success("Download Complete!")
            except Exception as e:
                st.error(f"Cannot download on Cloud: {e}")
            
    with st.expander("🔍 Inspect Synced Cloud File", expanded=False):
        if total_count > 0 and df is not None:
            st.dataframe(df.head(50), use_container_width=True)
        else:
            st.warning("Cloud file missing or empty.")

    # SIGNALS PAGE
elif page == "Signals":
    st.markdown("<div class='section'>12-Condition Signals</div>", unsafe_allow_html=True)
    
    LIVE_FILE = "data/dashboard_cloud.csv"


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
        if exclude_t2t and "EVER_100_DELIV" in df.columns:
            df_filt = df[df["EVER_100_DELIV"] == False]
        else:
            df_filt = df
        signals = get_cached_signals(df_filt)
        
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

# INSTITUTIONAL SIGNALS (now powered by survivors_archive.csv)
elif page == "Institutional Signals":
    st.markdown("<div class='section'>Institutional Signals (Rolling 30 Days)</div>", unsafe_allow_html=True)
    
    try:
        live_df = pd.read_csv("data/dashboard_cloud.csv", usecols=["DATE"])
        calc_date = pd.to_datetime(live_df["DATE"].iloc[0], errors="coerce").strftime("%d %b %Y")
        st.markdown(f"<p style='color: #a0aec0; margin-top: -10px; margin-bottom: 20px; font-weight: bold;'>⚡ All metrics actively recalculated using market data as of: <span style='color: #00ffcc;'>{calc_date}</span></p>", unsafe_allow_html=True)
    except:
        pass
        
    with st.expander("🤖 How to read these scores (Machine Learning Insights)"):
        st.markdown("""
        **The ML Edge (Based on past 30 days of performance):**
        * **STABILITY_RAW:** Measures block order size vs the 3-month average. **> 3.16 is the golden threshold.** 
        * **TRIGGER_COUNT_30D:** **1 is good** (Fresh Accumulation). 3+ is bad (Distribution Trap).
        * **AI_SCORE:** A percentile score (0.0 to 1.0) that heavily rewards massive block orders on fresh triggers.
        * *Tip: If a stock has a high COMBINED_SCORE but a low AI_SCORE, it means it has high momentum but no real institutional block orders. Avoid it.*
        """)
    
    HIST_FILE = "data/active_signals_ranked.csv"
    if os.path.exists(HIST_FILE):
        df_hist = pd.read_csv(HIST_FILE)
        df_hist["DATE"] = pd.to_datetime(df_hist["DATE"], errors="coerce")
        
        # Summary metrics
        c1, c2, c3 = st.columns(3)
        with c1: metric("Total Survivors", f"{len(df_hist)}")
        with c2: metric("Trading Days", f"{df_hist['DATE'].nunique()}")
        with c3: metric("Unique Symbols", f"{df_hist['SYMBOL'].nunique()}")
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            date_options = ["ALL"] + sorted([d.strftime("%Y-%m-%d") for d in df_hist["DATE"].dt.date.unique()], reverse=True)
            date_filter = st.selectbox("Trading Date", date_options)
        with col2:
            min_score = st.slider("Min Combined Score", 0.0, 1.0, 0.0, 0.05, help="Filter by minimum COMBINED_SCORE (0-1 percentile)")
        with col3:
            search_query = st.text_input("Search Symbol", "").upper()
        with col4:
            exchange_filter = st.selectbox("Exchange", ["ALL", "NSE", "BSE"])
            
        # Apply filters
        if date_filter != "ALL":
            df_hist = df_hist[df_hist["DATE"].dt.strftime("%Y-%m-%d") == date_filter]
        if "COMBINED_SCORE" in df_hist.columns:
            df_hist = df_hist[df_hist["COMBINED_SCORE"] >= min_score]
        if search_query:
            df_hist = df_hist[df_hist["SYMBOL"].str.contains(search_query, na=False)]
        if exchange_filter != "ALL":
            df_hist = df_hist[df_hist["EXCHANGE"] == exchange_filter]
            
        if len(df_hist) > 0:
            if "AI_SCORE" in df_hist.columns:
                df_hist = df_hist.sort_values(by=["AI_SCORE", "STABILITY_RAW"], ascending=[False, False])
            else:
                df_hist = df_hist.sort_values(by=["COMBINED_SCORE"], ascending=[False])
            
            # Create FIRST_TRIGGERED from DATE (the day the stock first entered the scanner)
            df_hist["FIRST_TRIGGERED"] = df_hist["DATE"].dt.strftime("%d %b %Y")
            
            # Determine today's date for highlighting
            today_date = df_hist["DATE"].max()
            is_today = df_hist["DATE"] == today_date
            
            # Add a "🆕" tag to today's signals in the SYMBOL column
            # Add repeat flags
            if "REPEAT_FLAG" in df_hist.columns and "TRIGGER_COUNT_30D" in df_hist.columns:
                mask = df_hist["REPEAT_FLAG"] == True
                df_hist.loc[mask, "SYMBOL"] = df_hist.loc[mask, "SYMBOL"] + " 🔥(" + df_hist.loc[mask, "TRIGGER_COUNT_30D"].astype(str) + ")"
            
            # Tag today's new signals
            df_hist.loc[is_today, "SYMBOL"] = "🆕 " + df_hist.loc[is_today, "SYMBOL"]
                
            if date_filter == "ALL":
                st.success(f"Displaying {len(df_hist)} survivors across all dates")
            else:
                st.success(f"Displaying {len(df_hist)} survivors for {date_filter}")

            # FIRST_TRIGGERED goes at the end (right side)
            display_cols = ["SYMBOL", "EXCHANGE", "CLOSE", 
                            "AI_SCORE", "COMBINED_SCORE", 
                            "TRIGGER_COUNT_30D", "STABILITY_RAW", 
                            "DELIV_PER", "DELIVERY_TURNOVER", "ATW", "FIRST_TRIGGERED"]
            avail_cols = [c for c in display_cols if c in df_hist.columns]
            
            # Reset index for clean sequential numbering
            df_hist = df_hist.reset_index(drop=True)
            
            # Build the is_today mask aligned to new index
            today_mask = is_today.values
            
            # Apply ML rules and today's highlighting
            def apply_ml_styles(df_style):
                styles = pd.DataFrame('', index=df_style.index, columns=df_style.columns)
                for i, idx in enumerate(df_hist.index):
                    # Default: today's signals get a light green tint
                    if i < len(today_mask) and today_mask[i]:
                        styles.iloc[i] = 'background-color: rgba(72, 187, 120, 0.15);'
                    
                    # ML TRAP (Overrides): High triggers = Distribution
                    if df_hist.iloc[i]['TRIGGER_COUNT_30D'] > 2:
                        styles.iloc[i] = 'background-color: rgba(255, 76, 76, 0.15); color: #ff4c4c;'
                    
                    # ML EDGE (Overrides): Fresh trigger + High Stability
                    elif df_hist.iloc[i]['TRIGGER_COUNT_30D'] == 1 and df_hist.iloc[i]['STABILITY_RAW'] > 3.16:
                        styles.iloc[i] = 'background-color: rgba(0, 255, 204, 0.15); color: #00ffcc; font-weight: bold;'
                        
                return styles
            
            # Apply styling
            if "DELIV_PER" in avail_cols:
                styled_df = df_hist[avail_cols].style.map(style_actionable_band, subset=["DELIV_PER"])
                styled_df = styled_df.apply(lambda _: apply_ml_styles(df_hist[avail_cols]), axis=None)
                
                format_dict = {}
                for raw_col in ["MOMENTUM_RAW", "FOOTPRINT_RAW", "STABILITY_RAW"]:
                    if raw_col in avail_cols:
                        format_dict[raw_col] = "{:.2f}"
                for pct_col in ["COMBINED_SCORE", "AI_SCORE", "MOMENTUM_SCORE", "FOOTPRINT_SCORE", "STABILITY_SCORE"]:
                    if pct_col in avail_cols:
                        format_dict[pct_col] = "{:.2f}"
                if format_dict:
                    styled_df = styled_df.format(format_dict)
                    
                st.dataframe(styled_df, use_container_width=True, height=600, hide_index=True)
            else:
                st.dataframe(df_hist[avail_cols], use_container_width=True, height=600, hide_index=True)
        else:
            st.info(f"No signals match the selected filters.")
    else:
        st.warning("⚠️ No survivor archive found. Run calculate_active_signals.py.")

# VERIFY CONDITIONS
elif page == "Verify Conditions":
    st.markdown("<div class='section'>Verify 12-Condition Compliance</div>", unsafe_allow_html=True)
    
    try:
        from progressive_screener import ProgressiveSpiker
        LIVE_FILE = "data/dashboard_cloud.csv"


        if os.path.exists(LIVE_FILE):
            df = load_live_data(LIVE_FILE, os.path.getmtime(LIVE_FILE))
            if exclude_t2t and "EVER_100_DELIV" in df.columns:
                df_filt = df[df["EVER_100_DELIV"] == False]
            else:
                df_filt = df
            sig = get_cached_signals(df_filt)
            if len(sig) > 0:
                st.success(f"Found {len(sig)} signals - Checking first 5...")
                
                for idx, row in sig.head(5).iterrows():
                    with st.expander(f"🔍 {row.get('SYMBOL', 'N/A')}"):
                        st.markdown("<div class='subsection'>Baseline (3 Conditions)</div>", unsafe_allow_html=True)
                        deliv_min = Config.PROGRESSIVE_SPIKE.get("delivery_pct_min", 50)
                        turnover_min = Config.PROGRESSIVE_SPIKE.get("delivery_turnover_min", 5000000)
                        atw_min = Config.PROGRESSIVE_SPIKE.get("atw_min", 25000)
                        st.write(f"1. Delivery % ≥ {deliv_min}: {row.get('DELIV_PER', 0):.2f} {'✅' if row.get('DELIV_PER', 0) >= deliv_min else '❌'}")
                        st.write(f"2. Turnover ≥ {turnover_min/1000000:g}M: {row.get('DELIVERY_TURNOVER', 0):,.0f} {'✅' if row.get('DELIVERY_TURNOVER', 0) >= turnover_min else '❌'}")
                        st.write(f"3. ATW ≥ {atw_min/1000:g}K: {row.get('ATW', 0):,.0f} {'✅' if row.get('ATW', 0) >= atw_min else '❌'}")
                        
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
            LIVE_FILE = "data/dashboard_cloud.csv"


            if os.path.exists(LIVE_FILE):
                df = load_live_data(LIVE_FILE, os.path.getmtime(LIVE_FILE))
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
