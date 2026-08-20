import streamlit as st
import pandas as pd
import os
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
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

# FZ STANDARD THEME WITH HOVER EFFECTS & 2026 AESTHETIC
st.markdown('''<style>
    :root { --card: rgba(255,255,255,.08); --border: rgba(255,255,255,.15); --accent:#00E5FF; }
    
    /* Deep space background */
    .stApp {
        background-color: #06080F;
        background-image: radial-gradient(circle at 20% 20%, rgba(0, 229, 255, 0.05) 0%, transparent 40%),
                          radial-gradient(circle at 80% 80%, rgba(245, 0, 87, 0.05) 0%, transparent 40%);
        color:#fff; font-family: 'Inter', 'Segoe UI', sans-serif;
    }

    /* Remove Streamlit containers & borders */
    .stPlotlyChart, .element-container {
        background: transparent !important;
        border: none !important;
        box-shadow: none !important;
    }

    /* Premium Typography */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&display=swap');

    .section { margin: 24px 0 12px 0; font-weight: 800; font-size: 24px;
        background: linear-gradient(135deg,#00E5FF,#F50057); -webkit-background-clip:text;
        -webkit-text-fill-color:transparent; font-family: 'Inter', sans-serif;}
    .subsection { margin: 16px 0 8px 0; font-weight: 700; font-size: 16px; color: var(--accent); font-family: 'Inter', sans-serif;}

    /* Original Cards */
    .card { padding:20px;border-radius:16px;background:var(--card);border:1px solid var(--border);text-align:center; transition: all 0.3s ease; }
    .card:hover { transform:translateY(-5px); box-shadow:0 8px 25px rgba(0,229,255,0.2); border-color:#00E5FF; }
    .metric .label { font-size:13px;opacity:.75;text-transform:uppercase;letter-spacing:1px; font-family: 'Inter', sans-serif;}
    .metric .value { font-size:32px;font-weight:900;color:var(--accent);margin-top:8px; font-family: 'Inter', sans-serif;}
    .hero { background: linear-gradient(135deg,#1e2a38,#1a233a); border: 1px solid rgba(0,229,255,0.2); padding:32px;border-radius:24px; margin:12px 0 24px 0; box-shadow:0 16px 48px rgba(0,0,0,.4);text-align:center}

    /* Fintech Cards */
    .fintech-card {
        background: rgba(20, 25, 40, 0.6);
        backdrop-filter: blur(16px);
        -webkit-backdrop-filter: blur(16px);
        border: 1px solid rgba(255, 255, 255, 0.07);
        border-radius: 12px;
        padding: 20px;
        position: relative;
        overflow: hidden;
        transition: all 0.3s ease;
    }
    .fintech-card:hover {
        border: 1px solid rgba(255, 255, 255, 0.2);
        box-shadow: 0 8px 30px rgba(0, 0, 0, 0.3);
    }
    
    .cap-label {
        font-size: 11px; color: #8b9bb4; 
        text-transform: uppercase; letter-spacing: 1.5px;
        font-family: 'Inter', sans-serif; font-weight: 600;
    }
    .cap-value {
        font-size: 36px; font-weight: 800; 
        font-family: 'Inter', sans-serif;
        line-height: 1.2; margin-top: 5px;
    }
    
    /* Glowing Progress Bar */
    .progress-bg {
        height: 4px; background: rgba(255,255,255,0.1); 
        border-radius: 2px; margin-top: 15px; overflow: hidden;
    }
    .progress-fill {
        height: 100%; border-radius: 2px;
        box-shadow: 0 0 10px currentColor; /* The glow effect */
    }

    /* Section Headers */
    .terminal-header {
        font-size: 14px; font-weight: 700; color: #ffffff;
        text-transform: uppercase; letter-spacing: 1px;
        border-left: 3px solid #00E5FF; padding-left: 12px; 
        margin-bottom: 15px; margin-top: 10px;
        font-family: 'Inter', sans-serif;
    }
    
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

# URL Routing state management (Cross-version compatible)
PAGES = ["Dashboard", "Signals", "SBIA Institutional Engine", "Verify Conditions", "Watchlist", "Win Rate", "Data Health"]
default_idx = 0
try:
    # Modern Streamlit (Local - v1.30+)
    params = st.query_params
    if "page" in params and params["page"] in PAGES:
        default_idx = PAGES.index(params["page"])
    page = st.sidebar.radio("Navigation", PAGES, index=default_idx)
    st.query_params["page"] = page
except AttributeError:
    # Legacy Streamlit (Cloud - v1.28)
    params = st.experimental_get_query_params()
    if "page" in params and params["page"][0] in PAGES:
        default_idx = PAGES.index(params["page"][0])
    page = st.sidebar.radio("Navigation", PAGES, index=default_idx)
    st.experimental_set_query_params(page=page)
st.sidebar.divider()
if st.sidebar.button("Force Data Refresh", help="Clear cache and reload latest data from disk", use_container_width=True):
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
            else:
                st.info("📭 No signals found")
                
            if True: # Always render visualization regardless of daily signal count
                # --- 2026 COMMAND CENTER VISUALIZATION ---
                st.markdown("<br>", unsafe_allow_html=True)
                LEGACY_FILE_MAIN = "data/legacy_watchlist.csv"
                if os.path.exists(LEGACY_FILE_MAIN):
                    try:
                        df_legacy_main = pd.read_csv(LEGACY_FILE_MAIN)
                        if len(df_legacy_main) > 0:
                            # Mock data generation for aesthetic purposes
                            np.random.seed(42)
                            sectors = ['Banks', 'IT', 'Pharma', 'Auto', 'Metals', 'FMCG', 'Realty', 'Energy', 'Capital Goods', 'Consumer']
                            caps = ['Large Cap', 'Mid Cap', 'Small Cap']

                            df_viz = df_legacy_main.copy()
                            df_viz['SECTOR'] = np.random.choice(sectors, len(df_viz))
                            df_viz['MARKET_CAP'] = np.random.choice(caps, len(df_viz), p=[0.2, 0.4, 0.4])

                            col_left, col_right = st.columns([1.5, 1])

                            with col_left:
                                st.markdown('<div class="terminal-header">Sector Displacement Radar</div>', unsafe_allow_html=True)
                                
                                sector_df = df_viz.groupby('SECTOR').size().reset_index(name='COUNT').sort_values('COUNT', ascending=True)
                                total_signals = len(df_viz)
                                                       # --- COLOR LOGIC ---
                                def get_color(count):
                                    if count >= 6: return '#F50057'  # Magenta for highest
                                    elif count >= 4: return '#FFB300' # Amber for mid
                                    else: return '#00E5FF'           # Cyan for lowest

                                colors = [get_color(c) for c in sector_df['COUNT']]

                                # --- BUILD THE CHART ---
                                fig = go.Figure()

                                # 1. THE STEMS (Thick, visible lines)
                                fig.add_trace(go.Bar(
                                    x=sector_df['COUNT'],
                                    y=sector_df['SECTOR'],
                                    orientation='h',
                                    marker=dict(
                                        color=colors, 
                                        line=dict(width=0) # No border on the bar itself
                                    ),
                                    width=0.05, # This makes the thick bar act like a thick stem
                                    hoverinfo='skip'
                                ))

                                # 2. THE DOTS AND LABELS (Heavy, bold, glowing)
                                fig.add_trace(go.Scatter(
                                    x=sector_df['COUNT'],
                                    y=sector_df['SECTOR'],
                                    mode='markers+text',
                                    marker=dict(
                                        size=18, # Much larger dots
                                        color=colors,
                                        line=dict(color='#FFFFFF', width=2), # White border for pop
                                        symbol='circle'
                                    ),
                                    text=sector_df['COUNT'],
                                    textposition='middle right',
                                    textfont=dict(
                                        color='#FFFFFF', # Pure white text
                                        size=16,         # Larger text
                                        family='Inter, sans-serif'
                                    ),
                                    hovertemplate='<b>%{y}</b><br>Signals: %{x}<extra></extra>'
                                ))

                                # 3. LAYOUT & ZERO-CHROME
                                fig.update_layout(
                                    margin=dict(t=40, l=120, r=40, b=20),
                                    paper_bgcolor='rgba(0,0,0,0)',
                                    plot_bgcolor='rgba(0,0,0,0)',
                                    showlegend=False,
                                    xaxis=dict(
                                        visible=False, # Hide X-axis numbers, the dots have the numbers
                                        showgrid=False,
                                        zeroline=False
                                    ),
                                    yaxis=dict(
                                        showgrid=False,
                                        zeroline=False,
                                        tickfont=dict(
                                            color='#FFFFFF', # Pure white sector names
                                            size=14,
                                            family='Inter, sans-serif'
                                        )
                                    ),
                                    height=380
                                )

                                # Add total signals as a sleek subtitle annotation since donut is gone
                                fig.add_annotation(
                                    text=f"TOTAL SIGNALS: <b style='color:#ffffff; font-size: 16px;'>{total_signals}</b>",
                                    x=0.01, y=1.15,
                                    xref="paper", yref="paper",
                                    showarrow=False,
                                    xanchor="left",
                                    font=dict(family='Inter, sans-serif', size=12, color='#8b9bb4')
                                )

                                st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})

                            with col_right:
                                st.markdown('<div class="terminal-header">Smart Money Flow</div>', unsafe_allow_html=True)
                                
                                cap_counts = df_viz['MARKET_CAP'].value_counts().to_dict()
                                total_viz = len(df_viz)
                                cap_colors = {'Large Cap': '#00E5FF', 'Mid Cap': '#FFB300', 'Small Cap': '#F50057'}
                                
                                # Render Premium Cards with Trend Indicators
                                for cap_type in ['Large Cap', 'Mid Cap', 'Small Cap']:
                                    count = cap_counts.get(cap_type, 0)
                                    pct = (count / total_viz) * 100 if total_viz > 0 else 0
                                    color = cap_colors[cap_type]
                                    
                                    # Mock 24h trend (replace with actual logic comparing today vs yesterday)
                                    trend_val = np.random.uniform(-5, 10) 
                                    trend_arrow = "▲" if trend_val >= 0 else "▼"
                                    trend_color = "#00E5FF" if trend_val >= 0 else "#F50057"
                                    
                                    style1 = "margin-bottom: 15px;"
                                    style2 = "display: flex; justify-content: space-between; align-items: flex-end;"
                                    style3 = f"color:{color};"
                                    style4 = "text-align: right; padding-bottom: 5px;"
                                    style5 = "font-size: 11px; color: #8b9bb4; text-transform: uppercase; letter-spacing: 1px;"
                                    style6 = f"font-size: 16px; font-weight: 600; color: {trend_color}; margin-top: 2px;"
                                    style7 = f"width: {pct}%; background-color: {color}; color: {color};"

                                    html_str = f"""
                                    <div class="fintech-card" style="{style1}">
                                        <div style="{style2}">
                                            <div>
                                                <div class="cap-label">{cap_type}</div>
                                                <div class="cap-value" style="{style3}">{count}</div>
                                            </div>
                                            <div style="{style4}">
                                                <div style="{style5}">24h Trend</div>
                                                <div style="{style6}">{trend_arrow} {abs(trend_val):.1f}%</div>
                                            </div>
                                        </div>
                                        <div class="progress-bg">
                                            <div class="progress-fill" style="{style7}"></div>
                                        </div>
                                    </div>
                                    """
                                    st.markdown(html_str, unsafe_allow_html=True)
                    except Exception as e:
                        st.error(f"Could not load heatmap: {e}")
                # -----------------------------------------------------------------
                
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

    # Calculate dates dynamically from the actual cloud data to permanently fix the sync issue
    nse_bhav_date, nse_deliv_date, bse_bhav_date, bse_deliv_date = "Missing", "Missing", "Missing", "Missing"
    
    if df is not None and not df.empty and "DATE" in df.columns:
        # Working with a copy to avoid SettingWithCopyWarning if df was a slice
        temp_df = df.copy()
        temp_df["_parsed_date"] = pd.to_datetime(temp_df["DATE"], errors='coerce')
        
        # 1. NSE Bhavcopy
        nse_mask = temp_df["EXCHANGE"] == "NSE"
        if nse_mask.any():
            nse_bhav_date = temp_df[nse_mask]["_parsed_date"].max().strftime("%d %b %Y")
            
        # 2. NSE Delivery (Requires DELIV_PER > 0)
        nse_deliv_mask = nse_mask & temp_df["DELIV_PER"].notna() & (temp_df["DELIV_PER"] > 0)
        if nse_deliv_mask.any():
            nse_deliv_date = temp_df[nse_deliv_mask]["_parsed_date"].max().strftime("%d %b %Y")
            
        # 3. BSE Bhavcopy
        bse_mask = temp_df["EXCHANGE"] == "BSE"
        if bse_mask.any():
            bse_bhav_date = temp_df[bse_mask]["_parsed_date"].max().strftime("%d %b %Y")
            
        # 4. BSE Delivery (Requires DELIV_PER > 0)
        bse_deliv_mask = bse_mask & temp_df["DELIV_PER"].notna() & (temp_df["DELIV_PER"] > 0)
        if bse_deliv_mask.any():
            bse_deliv_date = temp_df[bse_deliv_mask]["_parsed_date"].max().strftime("%d %b %Y")

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

# MULTI-STRATEGY EXECUTION ENGINE
elif page == "SBIA Institutional Engine":
    st.markdown("<div class='section'>Multi-Strategy Execution Engine</div>", unsafe_allow_html=True)
    
    st.markdown("""
    <div style='background: rgba(255, 255, 255, 0.03); border: 1px solid rgba(255, 255, 255, 0.1); border-radius: 12px; padding: 15px; margin-bottom: 25px;'>
    This dual-engine system isolates distinct institutional profiles: High-Velocity Alpha Markups (Path A) and Quiet Base-Loading Breakouts (Path B).
    </div>
    """, unsafe_allow_html=True)
    
    tab1, tab2, tab3 = st.tabs(["🔬 Legacy Screener", "🏆 SBIA Alpha Engine (High-Velocity)", "🔭 SBIA FlexGate Engine (Base-Loading)"])
    
    with tab1:
        st.markdown("<div class='subsection'>🔬 Phase 1: Institutional Screener (Raw ATW Unverified)</div>", unsafe_allow_html=True)

        LEGACY_FILE = "data/legacy_watchlist.csv"
        if os.path.exists(LEGACY_FILE):
            try:
                df_legacy = pd.read_csv(LEGACY_FILE)
                # ------------------------------------
                
            except pd.errors.EmptyDataError:
                df_legacy = pd.DataFrame()
                
            if len(df_legacy) > 0:
                df_legacy["DATE_RAW"] = pd.to_datetime(df_legacy["DATE"], errors="coerce")
                df_legacy["DATE"] = df_legacy["DATE_RAW"].dt.strftime("%d %b %Y")
                today_date = df_legacy["DATE_RAW"].max()
                is_today = df_legacy["DATE_RAW"] == today_date
                
                # Tag today's new signals
                df_legacy.loc[is_today, "SYMBOL"] = "🆕 " + df_legacy.loc[is_today, "SYMBOL"]
                if "REPEAT_FLAG" in df_legacy.columns and "TRIGGER_COUNT_30D" in df_legacy.columns:
                    mask = (df_legacy["REPEAT_FLAG"] == True) & df_legacy["TRIGGER_COUNT_30D"].notna()
                    df_legacy.loc[mask, "SYMBOL"] = df_legacy.loc[mask, "SYMBOL"] + " 🔥(" + df_legacy.loc[mask, "TRIGGER_COUNT_30D"].astype(str) + ")"
                
                if "COMBINED_SCORE" in df_legacy.columns:
                    df_legacy = df_legacy.drop(columns=["COMBINED_SCORE"])
                
                
                legacy_cols = ["DATE", "SYMBOL", "EXCHANGE", "CLOSE", "AI_SCORE", "SIS", "Whale_Density", "Implied_Trades", "STABILITY_RAW", "TRIGGER_COUNT_30D", "DELIV_PER", "DELIVERY_TURNOVER", "ATW"]
                avail_leg_cols = [c for c in legacy_cols if c in df_legacy.columns]
                
                df_legacy = df_legacy.reset_index(drop=True)
                today_mask = is_today.values
                
                def apply_ml_styles(df_style):
                    styles = pd.DataFrame('', index=df_style.index, columns=df_style.columns)
                    for i, _ in enumerate(df_legacy.index):
                        if i < len(today_mask) and today_mask[i]:
                            styles.iloc[i] = 'background-color: rgba(72, 187, 120, 0.15);'
                        if 'TRIGGER_COUNT_30D' in df_legacy.columns and df_legacy.iloc[i]['TRIGGER_COUNT_30D'] > 2:
                            styles.iloc[i] = 'background-color: rgba(255, 76, 76, 0.15); color: #ff4c4c;'
                        elif 'TRIGGER_COUNT_30D' in df_legacy.columns and 'STABILITY_RAW' in df_legacy.columns and df_legacy.iloc[i]['TRIGGER_COUNT_30D'] == 1 and df_legacy.iloc[i]['STABILITY_RAW'] > 3.16:
                            styles.iloc[i] = 'background-color: rgba(0, 255, 204, 0.15); color: #00ffcc; font-weight: bold;'
                    return styles
                    
                format_dict_leg = {
                    "CLOSE": "₹{:.2f}",
                    "AI_SCORE": "{:.2f}",
                    "SIS": "{:.2f}",
                    "Whale_Density": "{:.2f}",
                    "Implied_Trades": "{:,.0f}",
                    "STABILITY_RAW": "{:.2f}",
                    "DELIV_PER": "{:.2f}%",
                    "DELIVERY_TURNOVER": "₹{:,.0f}",
                    "ATW": "₹{:,.0f}"
                }
                
                styled_leg = df_legacy[avail_leg_cols].style.format(format_dict_leg).background_gradient(subset=["AI_SCORE"], cmap="YlOrRd")
                if "DELIV_PER" in avail_leg_cols:
                    styled_leg = styled_leg.map(style_actionable_band, subset=["DELIV_PER"])
                styled_leg = styled_leg.apply(lambda _: apply_ml_styles(df_legacy[avail_leg_cols]), axis=None)
                
                st.dataframe(styled_leg, use_container_width=True, hide_index=True)
            else:
                st.info("No legacy signals found.")
        else:
            st.info("Legacy watchlist not found.")
            
    with tab2:
        st.markdown("""
        <div style='background: linear-gradient(135deg, rgba(46, 204, 113, 0.1), rgba(39, 174, 96, 0.05)); border: 1px solid rgba(46, 204, 113, 0.4); border-radius: 12px; padding: 20px; margin-bottom: 20px;'>
            <h3 style='margin-top: 0; color: #2ecc71; display: flex; align-items: center;'><span style='font-size: 1.5rem; margin-right: 10px;'>🏆</span> Path A: Alpha Markups</h3>
            <p style='margin-bottom: 0; opacity: 0.9;'>These signals survived the 12-Condition cascade and the AI Bouncer. They are primed for immediate, high-velocity markups.</p>
        </div>
        """, unsafe_allow_html=True)
        
        ALPHA_FILE = "data/sbia_alpha_watchlist.csv"
        if os.path.exists(ALPHA_FILE):
            try:
                df_alpha = pd.read_csv(ALPHA_FILE)
            except pd.errors.EmptyDataError:
                df_alpha = pd.DataFrame()
                
            if len(df_alpha) > 0:
                if "DATE" in df_alpha.columns:
                    df_alpha["DATE"] = pd.to_datetime(df_alpha["DATE"], errors="coerce").dt.strftime("%d %b %Y")
                
                if "ENTRY_PRICE" in df_alpha.columns:
                    if "STOP_LOSS" in df_alpha.columns:
                        df_alpha["STOP_LOSS"] = df_alpha.apply(
                            lambda r: f"₹{r['STOP_LOSS']:.2f} ({((r['STOP_LOSS'] - r['ENTRY_PRICE']) / r['ENTRY_PRICE']) * 100:.1f}%)" if pd.notna(r['STOP_LOSS']) and pd.notna(r['ENTRY_PRICE']) and r['ENTRY_PRICE'] > 0 else (f"₹{r['STOP_LOSS']:.2f}" if pd.notna(r['STOP_LOSS']) else "N/A"),
                            axis=1
                        )
                    if "TAKE_PROFIT" in df_alpha.columns:
                        df_alpha["TAKE_PROFIT"] = df_alpha.apply(
                            lambda r: f"₹{r['TAKE_PROFIT']:.2f} (+{((r['TAKE_PROFIT'] - r['ENTRY_PRICE']) / r['ENTRY_PRICE']) * 100:.1f}%)" if pd.notna(r['TAKE_PROFIT']) and pd.notna(r['ENTRY_PRICE']) and r['ENTRY_PRICE'] > 0 else (f"₹{r['TAKE_PROFIT']:.2f}" if pd.notna(r['TAKE_PROFIT']) else "N/A"),
                            axis=1
                        )
                
                format_dict = {
                    "ENTRY_PRICE": "₹{:.2f}",
                    "CLOSE": "₹{:.2f}",
                    "ATR14": "₹{:.2f}",
                    "REC_POS_SIZE_INR": "₹{:,.0f}",
                    "AI_WIN_PROBABILITY": "{:.1f}%",
                    "SIS": "{:.2f}",
                    "Whale_Density": "{:.2f}",
                    "Implied_Trades": "{:,.0f}"
                }
                
                display_cols = ["DATE", "SYMBOL", "EXCHANGE", "ENTRY_PRICE", "CLOSE", "AI_WIN_PROBABILITY", "SIS", "Whale_Density", "Implied_Trades", "STOP_LOSS", "TAKE_PROFIT", "REC_POS_SIZE_INR", "ATR14"]
                avail_cols = [c for c in display_cols if c in df_alpha.columns]
                
                styled_alpha = df_alpha[avail_cols].style.format(format_dict).background_gradient(subset=["AI_WIN_PROBABILITY"], cmap="Greens")
                st.dataframe(styled_alpha, use_container_width=True, hide_index=True)
                
                st.markdown("<br>", unsafe_allow_html=True)
                with st.expander("🕰️ Historical / Completed Trades"):
                    if os.path.exists("data/sbia_ledger.csv"):
                        try:
                            ledger_df = pd.read_csv("data/sbia_ledger.csv")
                            completed_df = ledger_df[ledger_df["STATUS"] != "ACTIVE"].copy()
                            
                            if len(completed_df) > 0:
                                completed_df["ENTRY_DATE"] = pd.to_datetime(completed_df["ENTRY_DATE"], errors="coerce").dt.strftime("%d %b %Y")
                                completed_df["EXIT_DATE"] = pd.to_datetime(completed_df["EXIT_DATE"], errors="coerce").dt.strftime("%d %b %Y")
                                
                                def color_status(val):
                                    if val == "HIT_TP": return "color: #2ecc71; font-weight: bold;"
                                    if val == "HIT_SL": return "color: #e74c3c; font-weight: bold;"
                                    if val == "SUSPENDED": return "color: #f39c12;"
                                    return "color: #95a5a6;"
                                    
                                if "ENTRY_PRICE" in completed_df.columns:
                                    if "STOP_LOSS" in completed_df.columns:
                                        completed_df["STOP_LOSS"] = completed_df.apply(
                                            lambda r: f"₹{r['STOP_LOSS']:.2f} ({((r['STOP_LOSS'] - r['ENTRY_PRICE']) / r['ENTRY_PRICE']) * 100:.1f}%)" if pd.notna(r['STOP_LOSS']) and pd.notna(r['ENTRY_PRICE']) and r['ENTRY_PRICE'] > 0 else (f"₹{r['STOP_LOSS']:.2f}" if pd.notna(r['STOP_LOSS']) else "N/A"),
                                            axis=1
                                        )
                                    if "TAKE_PROFIT" in completed_df.columns:
                                        completed_df["TAKE_PROFIT"] = completed_df.apply(
                                            lambda r: f"₹{r['TAKE_PROFIT']:.2f} (+{((r['TAKE_PROFIT'] - r['ENTRY_PRICE']) / r['ENTRY_PRICE']) * 100:.1f}%)" if pd.notna(r['TAKE_PROFIT']) and pd.notna(r['ENTRY_PRICE']) and r['ENTRY_PRICE'] > 0 else (f"₹{r['TAKE_PROFIT']:.2f}" if pd.notna(r['TAKE_PROFIT']) else "N/A"),
                                            axis=1
                                        )
                                    if "EXIT_PRICE" in completed_df.columns:
                                        completed_df["EXIT_PRICE"] = completed_df.apply(
                                            lambda r: f"₹{r['EXIT_PRICE']:.2f} ({((r['EXIT_PRICE'] - r['ENTRY_PRICE']) / r['ENTRY_PRICE']) * 100:+.1f}%)" if pd.notna(r['EXIT_PRICE']) and pd.notna(r['ENTRY_PRICE']) and r['ENTRY_PRICE'] > 0 else (f"₹{r['EXIT_PRICE']:.2f}" if pd.notna(r['EXIT_PRICE']) else "N/A"),
                                            axis=1
                                        )

                                format_ledger = {
                                    "ENTRY_PRICE": "₹{:.2f}",
                                    "ENTRY_AI_PROB": "{:.1f}%",
                                    "ENTRY_WHALE_DENSITY": "{:.2f}"
                                }
                                
                                disp_cols = ["ENTRY_DATE", "SYMBOL", "STATUS", "ENTRY_AI_PROB", "ENTRY_WHALE_DENSITY", "ENTRY_PRICE", "EXIT_PRICE", "EXIT_DATE", "STOP_LOSS", "TAKE_PROFIT"]
                                avail_ledger = [c for c in disp_cols if c in completed_df.columns]
                                
                                st.dataframe(
                                    completed_df[avail_ledger].style.map(color_status, subset=["STATUS"]).format(format_ledger),
                                    use_container_width=True, hide_index=True
                                )
                            else:
                                st.info("No completed trades recorded yet.")
                        except Exception as e:
                            st.error(f"Error loading ledger: {e}")

                st.markdown("<br>", unsafe_allow_html=True)
                
                # --- SBIA VELOCITY ₹10L SIMULATION ---
                if os.path.exists("data/sbia_ledger.csv"):
                    try:
                        ledger_full = pd.read_csv("data/sbia_ledger.csv")
                        if len(ledger_full) > 0:
                            st.markdown("""
                            <div style='background: linear-gradient(135deg, rgba(46, 204, 113, 0.1), rgba(39, 174, 96, 0.05)); border: 1px solid rgba(46, 204, 113, 0.4); border-radius: 12px; padding: 20px; margin-bottom: 20px;'>
                                <h3 style='margin-top: 0; color: #2ecc71; display: flex; align-items: center;'><span style='font-size: 1.5rem; margin-right: 10px;'>💰</span> ₹10L Velocity Simulation</h3>
                                <p style='margin-bottom: 0; opacity: 0.9;'>Tracking total Realized and Unrealized PnL assuming a ₹1,000,000 base capital, risking exactly 1.5% (₹15,000) per trade based on the exact Stop Loss distance on entry day.</p>
                            </div>
                            """, unsafe_allow_html=True)
                            
                            capital = 1000000.0
                            risk_per_trade = capital * 0.015 # 15000
                            
                            sim_records = []
                            total_realized = 0.0
                            total_unrealized = 0.0
                            wins = 0
                            losses = 0
                            
                            for idx, row in ledger_full.iterrows():
                                sym = row['SYMBOL']
                                entry = row['ENTRY_PRICE']
                                sl = row['STOP_LOSS']
                                status = row['STATUS']
                                
                                if pd.isna(entry) or pd.isna(sl) or entry <= sl:
                                    # Fallback if SL is invalid
                                    invested = capital * 0.10
                                    shares = invested / entry if entry > 0 else 0
                                else:
                                    sl_dist = entry - sl
                                    shares = risk_per_trade / sl_dist
                                    invested = shares * entry
                                    
                                    # Cap max investment at 10% of equity like the main engine
                                    if invested > capital * 0.10:
                                        invested = capital * 0.10
                                        shares = invested / entry
                                
                                # Realized PnL
                                r_pnl = 0.0
                                u_pnl = 0.0
                                current_value = invested
                                
                                if status != 'ACTIVE':
                                    exit_px = row.get('EXIT_PRICE', entry)
                                    if pd.isna(exit_px): exit_px = entry
                                    r_pnl = shares * (exit_px - entry)
                                    total_realized += r_pnl
                                    current_value = invested + r_pnl
                                    
                                    if status in ['HIT_TP', 'MOMENTUM_LOST'] and r_pnl > 0: wins += 1
                                    elif status == 'HIT_SL' or r_pnl < 0: losses += 1
                                    
                                else:
                                    # Active trade, get current price from df_alpha
                                    current_px = entry
                                    if sym in df_alpha['SYMBOL'].values:
                                        current_px = df_alpha[df_alpha['SYMBOL'] == sym]['CLOSE'].iloc[0]
                                    u_pnl = shares * (current_px - entry)
                                    total_unrealized += u_pnl
                                    current_value = invested + u_pnl
                                    
                                sim_records.append({
                                    "DATE": row["ENTRY_DATE"],
                                    "SYMBOL": sym,
                                    "STATUS": status,
                                    "INVESTED": invested,
                                    "CURR_VALUE": current_value,
                                    "REALIZED_PNL": r_pnl if status != 'ACTIVE' else np.nan,
                                    "UNREALIZED_PNL": u_pnl if status == 'ACTIVE' else np.nan,
                                    "TOTAL_PNL": r_pnl + u_pnl,
                                    "PNL_%": ((r_pnl + u_pnl) / invested * 100) if invested > 0 else 0
                                })
                                
                            sim_df = pd.DataFrame(sim_records)
                            sim_df = sim_df.sort_values(by="DATE", ascending=False)
                            
                            total_trades = wins + losses
                            win_rate = (wins / total_trades * 100) if total_trades > 0 else 0.0
                            current_equity = capital + total_realized + total_unrealized
                            
                            c1, c2, c3, c4 = st.columns(4)
                            c1.metric("Current Portfolio Value", f"₹{current_equity:,.0f}", f"{((current_equity - capital) / capital) * 100:+.2f}%")
                            c2.metric("Total Realized PnL", f"₹{total_realized:,.0f}")
                            c3.metric("Running Unrealized PnL", f"₹{total_unrealized:,.0f}")
                            c4.metric("Strategy Win Rate", f"{win_rate:.1f}%", f"{wins}W / {losses}L")
                            
                            st.markdown("<br>", unsafe_allow_html=True)
                            
                            with st.expander("📊 View Trade-by-Trade Simulation Ledger"):
                                sim_df["DATE"] = pd.to_datetime(sim_df["DATE"], errors="coerce").dt.strftime("%d %b %Y")
                                
                                format_sim = {
                                    "INVESTED": "₹{:,.0f}",
                                    "CURR_VALUE": "₹{:,.0f}",
                                    "REALIZED_PNL": "₹{:,.0f}",
                                    "UNREALIZED_PNL": "₹{:,.0f}",
                                    "TOTAL_PNL": "₹{:,.0f}",
                                    "PNL_%": "{:+.1f}%"
                                }
                                
                                def color_pnl(val):
                                    if pd.isna(val): return ""
                                    if val > 0: return "color: #2ecc71; font-weight: bold;"
                                    if val < 0: return "color: #e74c3c; font-weight: bold;"
                                    return "color: #95a5a6;"
                                    
                                styled_sim = sim_df.style.format(format_sim).map(color_pnl, subset=["REALIZED_PNL", "UNREALIZED_PNL", "TOTAL_PNL", "PNL_%"]).map(color_status, subset=["STATUS"])
                                st.dataframe(styled_sim, use_container_width=True, hide_index=True)
                                
                    except Exception as e:
                        st.error(f"Error loading simulation: {e}")
            else:
                st.warning("⚠️ No stocks passed the Path A ML Gate today.")
        else:
            st.warning("Run calculate_active_signals.py to generate the Alpha Watchlist.")
            
    with tab3:
        st.markdown("""
        <div style='background: linear-gradient(135deg, rgba(142, 162, 255, 0.1), rgba(11, 124, 255, 0.05)); border: 1px solid rgba(142, 162, 255, 0.4); border-radius: 12px; padding: 20px; margin-bottom: 20px;'>
            <h3 style='margin-top: 0; color: #8ea2ff; display: flex; align-items: center;'><span style='font-size: 1.5rem; margin-right: 10px;'>🔭</span> Path B: Base-Loading (FlexGate)</h3>
            <p style='margin-bottom: 0; opacity: 0.9;'>These signals survived the ICT Box anomalies (exactly 2 alerts in <strong>21 days</strong>). <strong>Trend-Following Notice: No Fixed Profit Target. Use the Chandelier Exit.</strong></p>
        </div>
        """, unsafe_allow_html=True)
        
        FLEXGATE_FILE = "data/sbia_flexgate_watchlist.csv"
        if os.path.exists(FLEXGATE_FILE):
            try:
                df_flex = pd.read_csv(FLEXGATE_FILE)
            except pd.errors.EmptyDataError:
                df_flex = pd.DataFrame()
                
            if len(df_flex) > 0:
                if "DATE" in df_flex.columns:
                    df_flex["DATE"] = pd.to_datetime(df_flex["DATE"], errors="coerce").dt.strftime("%d %b %Y")
                
                if "AI_WIN_PROBABILITY" in df_flex.columns and "AI_APPROVED" in df_flex.columns:
                    df_flex["AI_STATUS"] = df_flex.apply(
                        lambda r: f"✅ {r['AI_WIN_PROBABILITY']:.1f}%" if r["AI_APPROVED"] else f"❌ {r['AI_WIN_PROBABILITY']:.1f}%",
                        axis=1
                    )
                
                format_dict = {
                    "CLOSE": "₹{:.2f}",
                    "ATR14": "₹{:.2f}",
                    "CHANDELIER_EXIT": "₹{:.2f}",
                    "REC_POS_SIZE_INR": "₹{:,.0f}",
                    "SIS": "{:.2f}",
                    "Whale_Density": "{:.2f}",
                    "Implied_Trades": "{:,.0f}"
                }
                
                display_cols = ["DATE", "SYMBOL", "EXCHANGE", "CLOSE", "AI_STATUS", "SIS", "Whale_Density", "Implied_Trades", "CHANDELIER_EXIT", "REC_POS_SIZE_INR", "ATR14"]
                avail_cols = [c for c in display_cols if c in df_flex.columns]

                def color_ai_status(val):
                    if isinstance(val, str) and val.startswith("✅"):
                        return "color: #2ecc71; font-weight: bold; background-color: rgba(46, 204, 113, 0.1);"
                    if isinstance(val, str) and val.startswith("❌"):
                        return "color: #e74c3c; font-style: italic; background-color: rgba(231, 76, 60, 0.05);"
                    return ""

                if "AI_STATUS" in avail_cols:
                    styled_flex = df_flex[avail_cols].style.format(format_dict).map(color_ai_status, subset=["AI_STATUS"])
                else:
                    styled_flex = df_flex[avail_cols].style.format(format_dict)
                    
                st.dataframe(styled_flex, use_container_width=True, hide_index=True)
                
                st.markdown("<br>", unsafe_allow_html=True)
                
                # --- SBIA FLEXGATE ₹10L SIMULATION ---
                st.markdown("""
                <div style='background: linear-gradient(135deg, rgba(142, 162, 255, 0.1), rgba(11, 124, 255, 0.05)); border: 1px solid rgba(142, 162, 255, 0.4); border-radius: 12px; padding: 20px; margin-bottom: 20px;'>
                    <h3 style='margin-top: 0; color: #8ea2ff; display: flex; align-items: center;'><span style='font-size: 1.5rem; margin-right: 10px;'>💰</span> ₹10L FlexGate Simulation</h3>
                    <p style='margin-bottom: 0; opacity: 0.9;'>Capital allocation based on a ₹1,000,000 base, risking exactly 1.5% (₹15,000) per trade based on the <strong>Chandelier Exit</strong> distance.</p>
                </div>
                """, unsafe_allow_html=True)
                
                capital = 1000000.0
                risk_per_trade = capital * 0.015 # 15000
                
                sim_records = []
                total_invested = 0.0
                
                for idx, row in df_flex.iterrows():
                    sym = row['SYMBOL']
                    entry = row['CLOSE']
                    sl = row.get('CHANDELIER_EXIT', pd.NA)
                    
                    if pd.isna(entry) or pd.isna(sl) or entry <= sl:
                        # Fallback if SL is invalid or missing
                        invested = capital * 0.10
                        shares = invested / entry if entry > 0 else 0
                    else:
                        sl_dist = entry - sl
                        shares = risk_per_trade / sl_dist
                        invested = shares * entry
                        
                        # Cap max investment at 10% of equity
                        if invested > capital * 0.10:
                            invested = capital * 0.10
                            shares = invested / entry
                            
                    total_invested += invested
                        
                    sim_records.append({
                        "DATE": row["DATE"],
                        "SYMBOL": sym,
                        "STATUS": "ACTIVE",
                        "INVESTED": invested,
                        "ENTRY_PRICE": entry,
                        "CHANDELIER_EXIT": sl,
                        "SHARES": shares
                    })
                    
                sim_df = pd.DataFrame(sim_records)
                
                c1, c2, c3 = st.columns(3)
                c1.metric("Total Capital", f"₹{capital:,.0f}")
                c2.metric("Total Allocated", f"₹{total_invested:,.0f}", f"{(total_invested / capital) * 100:.1f}% Deployed")
                c3.metric("Available Cash", f"₹{(capital - total_invested):,.0f}")
                
                st.markdown("<br>", unsafe_allow_html=True)
                
                with st.expander("📊 View Current Portfolio Allocation"):
                    format_sim = {
                        "INVESTED": "₹{:,.0f}",
                        "ENTRY_PRICE": "₹{:.2f}",
                        "CHANDELIER_EXIT": "₹{:.2f}",
                        "SHARES": "{:,.0f}"
                    }
                    
                    styled_sim = sim_df.style.format(format_sim)
                    st.dataframe(styled_sim, use_container_width=True, hide_index=True)
            else:
                st.warning("⚠️ No stocks passed the strict FlexGate logic today.")
        else:
            st.warning("Run calculate_active_signals.py to generate the FlexGate Watchlist.")

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
