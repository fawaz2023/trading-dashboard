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
    
    /* 1. KILL DEFAULT STREAMLIT UI */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    
    /* Make the top header transparent so the sidebar toggle button (>>/<<) still works! */
    header {
        background-color: transparent !important;
    }
    header:hover {
        background-color: rgba(0, 229, 255, 0.05) !important;
    }
    
    /* Hide the ugly 'Running...' status dots but keep the sidebar button */
    [data-testid="stStatusWidget"] {
        display: none !important;
    }
    
    /* 2. DARK GLASSMORPHISM SIDEBAR */
    section[data-testid="stSidebar"] {
        background-color: #0A0C12;
        border-right: 1px solid rgba(255,255,255,0.05);
    }
    section[data-testid="stSidebar"] .stRadio > label {
        font-size: 12px; text-transform: uppercase; letter-spacing: 1.5px; color: #5f6b7c;
        font-family: 'Inter', sans-serif; font-weight: 600;
    }
    
    /* 3. CUSTOM NAVIGATION BUTTONS */
    section[data-testid="stSidebar"] .stRadio > div[role="radiogroup"] > label {
        background-color: transparent;
        border: 1px solid transparent;
        border-radius: 8px;
        padding: 10px 12px;
        transition: all 0.2s ease;
        margin-bottom: 5px;
        color: #8b9bb4;
        font-weight: 500;
    }
    section[data-testid="stSidebar"] .stRadio > div[role="radiogroup"] > label:hover {
        background-color: rgba(255,255,255,0.03);
        color: #ffffff;
        border-color: rgba(255,255,255,0.1);
    }
    section[data-testid="stSidebar"] .stRadio > div[role="radiogroup"] > label[data-checked="true"] {
        background-color: rgba(0, 229, 255, 0.05) !important;
        border-left: 3px solid #00E5FF !important;
        border-radius: 0 8px 8px 0;
        color: #00E5FF !important;
        font-weight: 700;
    }
    
    /* 5. DENSE, PREMIUM DATA TABLE STYLING */
    .stDataFrame {
        background: rgba(20, 25, 40, 0.4) !important;
        border: 1px solid rgba(255,255,255,0.07) !important;
        border-radius: 12px !important;
        padding: 0px 15px !important; /* Remove inner padding so table spans width */
    }
    /* Style Table Headers */
    .stDataFrame th {
        background-color: transparent !important;
        color: #8b9bb4 !important;
        font-family: 'Inter', sans-serif !important;
        font-weight: 600 !important;
        text-transform: uppercase !important;
        font-size: 11px !important;
        letter-spacing: 1px !important;
        border-bottom: 1px solid rgba(255,255,255,0.1) !important;
    }
    /* Style Table Data Rows */
    .stDataFrame td {
        font-family: 'Inter', sans-serif !important;
        font-weight: 600 !important;
        color: #e0e6ed !important;
        font-size: 14px !important;
        border-bottom: 1px solid rgba(255,255,255,0.03) !important;
        padding-top: 14px !important; /* Dense rows */
        padding-bottom: 14px !important;
    }
    /* Neon Cyan Checkboxes */
    .stDataFrame input[type="checkbox"] {
        accent-color: #00E5FF !important;
        cursor: pointer;
    }
    /* Remove default streamlit button hover grey */
    .stDataFrame tr:hover {
        background-color: rgba(0, 229, 255, 0.03) !important;
    }
    /* 6. PREMIUM SEARCH BAR (Command Line Style) */
    div[data-baseweb="select"] > div {
        background-color: rgba(255, 255, 255, 0.03) !important;
        border: 1px solid rgba(255, 255, 255, 0.1) !important;
        border-radius: 8px !important;
        box-shadow: none !important;
        transition: all 0.2s ease;
    }
    div[data-baseweb="select"] > div:hover {
        border: 1px solid rgba(0, 229, 255, 0.5) !important;
        background-color: rgba(0, 229, 255, 0.05) !important;
    }
    div[data-baseweb="select"] > div > div {
        font-family: 'Inter', sans-serif !important;
        font-weight: 600 !important;
        color: #FFFFFF !important;
        font-size: 16px !important;
    }
    /* Style the dropdown options when opened */
    ul[data-baseweb="menu"] {
        background-color: #0e1117 !important;
        border: 1px solid rgba(0, 229, 255, 0.2) !important;
        border-radius: 8px !important;
    }
    li[data-baseweb="option"] {
        color: #e0e6ed !important;
        font-family: 'Inter', sans-serif !important;
    }
    li[data-baseweb="option"]:hover {
        background-color: rgba(0, 229, 255, 0.1) !important;
        color: #FFFFFF !important;
    }

    /* 7. FIX THE PRIMARY BUTTON (Terminal Magenta) */
    .stButton > button[kind="primary"] {
        background-color: #F50057 !important;
        color: #ffffff !important;
        border: none !important;
        border-radius: 8px !important;
        font-family: 'Inter', sans-serif !important;
        font-weight: 700 !important;
        letter-spacing: 0.5px !important;
        text-transform: uppercase !important;
        transition: all 0.2s ease !important;
        box-shadow: 0 0 15px rgba(245, 0, 87, 0.3) !important; /* Magenta glow */
    }
    .stButton > button[kind="primary"]:hover {
        background-color: #FF1A6B !important;
        box-shadow: 0 0 25px rgba(245, 0, 87, 0.6) !important;
        transform: translateY(-1px) !important;
    }
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
                                # --- COLOR LOGIC: Solid Neon and Glowing RGBA counterpart ---
                                def get_colors(count):
                                    if count >= 6:
                                        return '#F50057', 'rgba(245, 0, 87, 0.35)'  # Magenta
                                    elif count >= 4:
                                        return '#FFB300', 'rgba(255, 179, 0, 0.35)' # Amber
                                    else:
                                        return '#00E5FF', 'rgba(0, 229, 255, 0.35)' # Cyan

                                # --- BUILD THE NEON TUBE CHART ---
                                fig = go.Figure()

                                for index, row in sector_df.iterrows():
                                    neon, glow = get_colors(row['COUNT'])
                                    
                                    # Layer 1: The Glow / Halation
                                    fig.add_trace(go.Bar(
                                        x=[row['COUNT']],
                                        y=[row['SECTOR']],
                                        orientation='h',
                                        marker=dict(color=glow, line=dict(width=0)),
                                        width=0.85,
                                        hoverinfo='skip',
                                        showlegend=False
                                    ))
                                    
                                    # Layer 2: The Main Solid Neon Bar
                                    fig.add_trace(go.Bar(
                                        x=[row['COUNT']],
                                        y=[row['SECTOR']],
                                        orientation='h',
                                        marker=dict(
                                            color=neon,
                                            line=dict(color='rgba(255,255,255,0.3)', width=1)
                                        ),
                                        width=0.55,
                                        text=[row['COUNT']],
                                        textposition='outside',
                                        textfont=dict(color='#FFFFFF', size=16, family='Arial Black, sans-serif'),
                                        hovertemplate=f'<b>{row["SECTOR"]}</b><br>Signals: {row["COUNT"]}<extra></extra>',
                                        showlegend=False
                                    ))

                                # 3. LAYOUT & ZERO-CHROME
                                fig.update_layout(
                                    margin=dict(t=40, l=120, r=40, b=20),
                                    paper_bgcolor='rgba(0,0,0,0)',
                                    plot_bgcolor='rgba(0,0,0,0)',
                                    barmode='overlay', # CRITICAL: stack the glow and the core
                                    xaxis=dict(
                                        visible=False,
                                        showgrid=False,
                                        zeroline=False
                                    ),
                                    yaxis=dict(
                                        showgrid=False,
                                        zeroline=False,
                                        tickfont=dict(
                                            color='#FFFFFF',
                                            size=15,
                                            family='Arial Black, Inter, sans-serif'
                                        )
                                    ),
                                    height=380
                                )

                                # Add total signals as a sleek subtitle annotation
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
    
    tab1, tab2, tab3, tab4 = st.tabs(["🔬 Legacy Screener", "🏆 SBIA Alpha Engine (High-Velocity)", "🔭 SBIA FlexGate Engine (Base-Loading)", "🤖 FlexGate 2.0 (ML Engine)"])
    
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
                                <p style='margin-bottom: 0; opacity: 0.9;'>Tracking total Realized and Unrealized PnL assuming a ₹1,000,000 base capital, risking exactly 3.0% (₹30,000) per trade based on the exact Stop Loss distance on entry day.</p>
                            </div>
                            """, unsafe_allow_html=True)
                            
                            capital = 1000000.0
                            risk_per_trade = capital * 0.030 # 30000
                            
                            sim_records = []
                            total_realized = 0.0
                            total_unrealized = 0.0
                            wins = 0
                            losses = 0
                            
                            # Load latest market prices for accurate Unrealized PnL
                            latest_prices = {}
                            if os.path.exists("data/dashboard_cloud.csv"):
                                try:
                                    live_df = pd.read_csv("data/dashboard_cloud.csv", usecols=["SYMBOL", "CLOSE"])
                                    latest_prices = live_df.drop_duplicates(subset=["SYMBOL"]).set_index("SYMBOL")["CLOSE"].to_dict()
                                except Exception:
                                    pass
                            
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
                                    # Active trade, get current price from live market data
                                    current_px = latest_prices.get(sym, entry)
                                    u_pnl = shares * (current_px - entry)
                                    total_unrealized += u_pnl
                                    current_value = invested + u_pnl
                                    
                                sim_records.append({
                                    "DATE": row["ENTRY_DATE"],
                                    "SYMBOL": sym,
                                    "STATUS": status,
                                    "INVESTED": invested,
                                    "CURR_VALUE": current_value,
                                    "REALIZED_PNL": r_pnl,
                                    "UNREALIZED_PNL": u_pnl,
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
            <p style='margin-bottom: 0; opacity: 0.9;'>These signals survived the ICT Box anomalies (exactly 2 alerts in <strong>10 days</strong>). <strong>Trend-Following Notice: No Fixed Profit Target. Use the Chandelier Exit.</strong></p>
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
                    <p style='margin-bottom: 0; opacity: 0.9;'>Capital allocation based on a ₹1,000,000 base, risking exactly 3.0% (₹30,000) per trade based on the <strong>Chandelier Exit</strong> distance.</p>
                </div>
                """, unsafe_allow_html=True)
                
                capital = 1000000.0
                risk_per_trade = capital * 0.030 # 30000
                
                sim_records = []
                total_invested = 0.0
                total_unrealized = 0.0
                
                # Load latest market prices for accurate Unrealized PnL
                latest_prices = {}
                if os.path.exists("data/dashboard_cloud.csv"):
                    try:
                        live_df = pd.read_csv("data/dashboard_cloud.csv", usecols=["SYMBOL", "CLOSE"])
                        latest_prices = live_df.drop_duplicates(subset=["SYMBOL"]).set_index("SYMBOL")["CLOSE"].to_dict()
                    except Exception:
                        pass
                
                for idx, row in df_flex.iterrows():
                    # Only simulate trades for AI APPROVED signals
                    if row.get('AI_APPROVED', False) == False:
                        continue
                        
                    # Stop allocating if we are out of cash
                    if total_invested >= capital:
                        continue
                        
                    sym = row['SYMBOL']
                    entry = row['CLOSE'] # This is the historical close (entry)
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
                            
                    # Hard cap so we don't exceed remaining capital
                    if total_invested + invested > capital:
                        invested = capital - total_invested
                        shares = invested / entry if entry > 0 else 0
                            
                    total_invested += invested
                    
                    curr_px = latest_prices.get(sym, entry)
                    u_pnl = shares * (curr_px - entry)
                    total_unrealized += u_pnl
                        
                    sim_records.append({
                        "DATE": row["DATE"],
                        "SYMBOL": sym,
                        "STATUS": "ACTIVE",
                        "INVESTED": invested,
                        "ENTRY_PRICE": entry,
                        "CURRENT_PRICE": curr_px,
                        "UNREALIZED_PNL": u_pnl,
                        "PNL_%": (u_pnl / invested * 100) if invested > 0 else 0,
                        "CHANDELIER_EXIT": sl,
                        "SHARES": shares
                    })
                    
                sim_df = pd.DataFrame(sim_records)
                
                c1, c2, c3 = st.columns(3)
                c1.metric("Total Capital", f"₹{capital:,.0f}")
                c2.metric("Total Allocated", f"₹{total_invested:,.0f}", f"{(total_invested / capital) * 100:.1f}% Deployed")
                c3.metric("Running Unrealized PnL", f"₹{total_unrealized:,.0f}", f"{(total_unrealized / total_invested) * 100 if total_invested > 0 else 0:+.2f}%")
                
                st.markdown("<br>", unsafe_allow_html=True)
                
                with st.expander("📊 View Current Portfolio Allocation"):
                    format_sim = {
                        "INVESTED": "₹{:,.0f}",
                        "ENTRY_PRICE": "₹{:.2f}",
                        "CURRENT_PRICE": "₹{:.2f}",
                        "UNREALIZED_PNL": "₹{:,.0f}",
                        "PNL_%": "{:+.1f}%",
                        "CHANDELIER_EXIT": "₹{:.2f}",
                        "SHARES": "{:,.0f}"
                    }
                    
                    def color_pnl(val):
                        if pd.isna(val): return ""
                        if val > 0: return "color: #2ecc71; font-weight: bold;"
                        if val < 0: return "color: #e74c3c; font-weight: bold;"
                        return "color: #95a5a6;"
                    
                    if not sim_df.empty:
                        styled_sim = sim_df.style.format(format_sim).map(color_pnl, subset=["UNREALIZED_PNL", "PNL_%"])
                    else:
                        styled_sim = sim_df.style.format(format_sim)
                        
                    st.dataframe(styled_sim, use_container_width=True, hide_index=True)
            else:
                st.warning("⚠️ No stocks passed the strict FlexGate logic today.")
        else:
            st.warning("Run calculate_active_signals.py to generate the FlexGate Watchlist.")
            
    with tab4:
        st.markdown("""
        <div style='background: linear-gradient(135deg, rgba(231, 76, 60, 0.1), rgba(192, 57, 43, 0.05)); border: 1px solid rgba(231, 76, 60, 0.4); border-radius: 12px; padding: 20px; margin-bottom: 20px;'>
            <h3 style='margin-top: 0; color: #e74c3c; display: flex; align-items: center;'><span style='font-size: 1.5rem; margin-right: 10px;'>🤖</span> FlexGate 2.0 (ML Engine)</h3>
            <p style='margin-bottom: 0; opacity: 0.9;'>These signals survived the ML Heuristic Bouncer (ATR > 3.5%) and scored &ge; 60% on the Random Forest engine.</p>
        </div>
        """, unsafe_allow_html=True)
        
        FLEXGATE2_FILE = "data/sbia_flexgate2_watchlist.csv"
        if os.path.exists(FLEXGATE2_FILE):
            try:
                df_flex2 = pd.read_csv(FLEXGATE2_FILE)
            except pd.errors.EmptyDataError:
                df_flex2 = pd.DataFrame()
                
            if len(df_flex2) > 0:
                if "DATE" in df_flex2.columns:
                    df_flex2["DATE"] = pd.to_datetime(df_flex2["DATE"], errors="coerce").dt.strftime("%d %b %Y")
                
                if "AI_WIN_PROBABILITY" in df_flex2.columns and "AI_APPROVED" in df_flex2.columns:
                    df_flex2["AI_STATUS"] = df_flex2.apply(
                        lambda r: f"✅ {r['AI_WIN_PROBABILITY']:.1f}%" if r["AI_APPROVED"] else f"❌ {r['AI_WIN_PROBABILITY']:.1f}%",
                        axis=1
                    )
                
                format_dict_f2 = {
                    "CLOSE": "₹{:.2f}",
                    "ATR14": "₹{:.2f}",
                    "CHANDELIER_EXIT": "₹{:.2f}",
                    "REC_POS_SIZE_INR": "₹{:,.0f}",
                    "SIS": "{:.2f}",
                    "Whale_Density": "{:.2f}",
                    "Implied_Trades": "{:,.0f}"
                }
                
                display_cols_f2 = ["DATE", "SYMBOL", "EXCHANGE", "CLOSE", "AI_STATUS", "SIS", "Whale_Density", "Implied_Trades", "CHANDELIER_EXIT", "REC_POS_SIZE_INR", "ATR14"]
                avail_cols_f2 = [c for c in display_cols_f2 if c in df_flex2.columns]

                def color_ai_status2(val):
                    if isinstance(val, str) and val.startswith("✅"):
                        return "color: #2ecc71; font-weight: bold; background-color: rgba(46, 204, 113, 0.1);"
                    if isinstance(val, str) and val.startswith("❌"):
                        return "color: #e74c3c; font-style: italic; background-color: rgba(231, 76, 60, 0.05);"
                    return ""

                if "AI_STATUS" in avail_cols_f2:
                    styled_flex2 = df_flex2[avail_cols_f2].style.format(format_dict_f2).map(color_ai_status2, subset=["AI_STATUS"])
                else:
                    styled_flex2 = df_flex2[avail_cols_f2].style.format(format_dict_f2)
                    
                st.dataframe(styled_flex2, use_container_width=True, hide_index=True)
            else:
                st.warning("⚠️ No stocks passed the strict FlexGate 2.0 ML logic today.")
        else:
            st.warning("Run flexgate_2_scanner.py to generate the FlexGate 2.0 Watchlist.")

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
    st.markdown('<div class="terminal-header" style="font-size: 24px; border-left: 4px solid #00E5FF; margin-bottom: 20px;">Active Watchlist</div>', unsafe_allow_html=True)
    try:
        from watchlist_manager import WatchlistManager
        wm = WatchlistManager()
        
        # --- 1. LOAD SCANNER UNIVERSE ---
        @st.cache_data(ttl=3600)
        def load_scanner_universe():
            universe_symbols = set()
            symbol_to_scanner = {}
            
            master_df = pd.DataFrame()
            LIVE_FILE = "data/dashboard_cloud.csv"
            if os.path.exists(LIVE_FILE):
                master_df = pd.read_csv(LIVE_FILE)
                try:
                    # 12-Condition Signals
                    sig_df = get_cached_signals(master_df)
                    for sym in sig_df['SYMBOL'].dropna().unique():
                        universe_symbols.add(sym)
                        symbol_to_scanner[sym] = "12-Condition Scanner"
                except: pass
            
            if os.path.exists("data/sbia_alpha_watchlist.csv"):
                df_alpha = pd.read_csv("data/sbia_alpha_watchlist.csv")
                for sym in df_alpha['SYMBOL'].dropna().unique():
                    universe_symbols.add(sym)
                    symbol_to_scanner[sym] = "SBIA Alpha Engine"
                    
            if os.path.exists("data/sbia_flexgate_watchlist.csv"):
                df_legacy = pd.read_csv("data/sbia_flexgate_watchlist.csv")
                for sym in df_legacy['SYMBOL'].dropna().unique():
                    universe_symbols.add(sym)
                    if sym not in symbol_to_scanner or symbol_to_scanner[sym] == "12-Condition Scanner":
                        symbol_to_scanner[sym] = "Legacy FlexGate"
                        
            if os.path.exists("data/sbia_flexgate2_watchlist.csv"):
                df_fg2 = pd.read_csv("data/sbia_flexgate2_watchlist.csv")
                if 'AI_APPROVED' in df_fg2.columns:
                    df_fg2 = df_fg2[df_fg2['AI_APPROVED'] == True]
                for sym in df_fg2['SYMBOL'].dropna().unique():
                    universe_symbols.add(sym)
                    symbol_to_scanner[sym] = "FlexGate 2.0 ML Engine"
                    
            records = [{"SYMBOL": sym, "SCANNER_NAME": symbol_to_scanner[sym]} for sym in universe_symbols]
            return master_df, pd.DataFrame(records)

        master_df, universe_df = load_scanner_universe()

        # --- 2. WATCHLIST SEARCH UI ---
        st.markdown('<div style="font-size: 13px; color: #8b9bb4; text-transform: uppercase; letter-spacing: 1.5px; font-weight: 700; margin-bottom: 10px;">Add Scanner Stock to Watchlist</div>', unsafe_allow_html=True)
        
        if not universe_df.empty:
            col1, col2 = st.columns([3, 1])
            with col1:
                universe_df['DISPLAY'] = universe_df['SYMBOL'] + "  |  " + universe_df['SCANNER_NAME'].fillna('Signal')
                selected_option = st.selectbox(
                    "Search for a stock that recently triggered a scanner:",
                    options=universe_df['DISPLAY'].unique(),
                    index=None,
                    placeholder="Type symbol or scanner name..."
                )

            # --- 3. LIVE METRIC PREVIEW & ADD LOGIC ---
            if selected_option:
                selected_symbol = selected_option.split("  |  ")[0]
                
                stock_data_match = master_df[master_df['SYMBOL'] == selected_symbol]
                stock_data = stock_data_match.iloc[0] if not stock_data_match.empty else {}
                
                try:
                    import yfinance as yf
                    ticker = yf.Ticker(selected_symbol + ".NS")
                    live_price = ticker.fast_info['last_price']
                except:
                    live_price = stock_data.get('CLOSE', 0.0)
                    
                st.markdown(f"""
                <div style="background: rgba(20, 25, 40, 0.6); border: 1px solid rgba(0, 229, 255, 0.3); border-radius: 10px; padding: 15px; margin-top: 10px; margin-bottom: 20px;">
                    <h3 style="color: #00E5FF; margin-bottom: 10px; margin-top: 0;">{selected_symbol} Preview</h3>
                    <div style="display: flex; justify-content: space-between; color: white; font-family: 'Inter', sans-serif;">
                        <div>
                            <div style="font-size: 12px; color: #8b9bb4;">LIVE PRICE</div>
                            <div style="font-size: 24px; font-weight: 800;">₹{live_price:.2f}</div>
                        </div>
                        <div>
                            <div style="font-size: 12px; color: #8b9bb4;">DELIVERY %</div>
                            <div style="font-size: 24px; font-weight: 800;">{stock_data.get('DELIV_PER', 0):.2f}%</div>
                        </div>
                        <div>
                            <div style="font-size: 12px; color: #8b9bb4;">SCANNER</div>
                            <div style="font-size: 24px; font-weight: 800;">{universe_df[universe_df['SYMBOL'] == selected_symbol]['SCANNER_NAME'].values[0]}</div>
                        </div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

                with st.form("add_to_watchlist_form"):
                    st.markdown("**Set Position Parameters**")
                    c1, c2, c3 = st.columns(3)
                    with c1:
                        entry_price = st.number_input("Entry Price", value=float(live_price), step=0.05)
                    with c2:
                        target_price = st.number_input("Target Price", value=float(live_price * 1.10), step=0.05)
                    with c3:
                        stop_loss = st.number_input("Stop Loss", value=float(live_price * 0.95), step=0.05)
                        
                    submitted = st.form_submit_button("➕ Add to Active Watchlist", type="primary")
                    
                    if submitted:
                        success, msg = wm.add_stock(
                            symbol=selected_symbol,
                            entry_price=entry_price,
                            delivery_pct=stock_data.get('DELIV_PER', 0),
                            momentum=0
                        )
                        if success:
                            st.success(f"{selected_symbol} added to Watchlist! Tracking PnL active.")
                            st.rerun()
                        else:
                            st.error(msg)
        else:
            st.info("No scanner signals currently active.")
        
        st.markdown("<hr>", unsafe_allow_html=True)

        if len(wm.active) > 0:
            LIVE_FILE = "data/dashboard_cloud.csv"
            if os.path.exists(LIVE_FILE):
                df = load_live_data(LIVE_FILE, os.path.getmtime(LIVE_FILE))
                wm.auto_update_prices(df)
                wm = WatchlistManager()  # Reload after update
            
            st.markdown('<div class="terminal-header" style="font-size: 16px; border-left: 4px solid #FFB300; margin-top: 30px; margin-bottom: 15px;">Manage Positions</div>', unsafe_allow_html=True)
            
            display_cols = ["symbol", "entry_price", "current_price", "tp", "sl", "entry_date"]
            display_df = wm.active[display_cols].copy()
            
            # Add PnL % Column
            display_df['PnL_%'] = ((display_df['current_price'] - display_df['entry_price']) / display_df['entry_price']) * 100
            
            # Add checkbox column
            display_df['Select to Close'] = False
            
            # Configure columns
            column_config = {
                "symbol": "Symbol",
                "entry_price": st.column_config.NumberColumn("Entry Price", format="₹%.2f"),
                "current_price": st.column_config.NumberColumn("Current Price", format="₹%.2f"),
                "tp": st.column_config.NumberColumn("Target", format="₹%.2f"),
                "sl": st.column_config.NumberColumn("Stop Loss", format="₹%.2f"),
                "entry_date": "Entry Date",
                "PnL_%": st.column_config.NumberColumn(
                    "PnL %",
                    format="%.2f%%"
                ),
                "Select to Close": st.column_config.CheckboxColumn(
                    "Select to Close",
                    default=False,
                    help="Check this box and click 'Close Selected Positions' below"
                )
            }
            
            # Render Data Editor
            edited_df = st.data_editor(
                display_df,
                column_config=column_config,
                use_container_width=True,
                hide_index=True,
                disabled=["symbol", "entry_price", "current_price", "tp", "sl", "entry_date", "PnL_%"]
            )
            
            # --- INLINE CLOSE POSITION LOGIC & FOOTER ---
            selected_to_close = edited_df[edited_df["Select to Close"] == True]
            
            # Calculate Total Portfolio PnL
            total_pnl = display_df['PnL_%'].sum()
            pnl_color = "#00E5FF" if total_pnl >= 0 else "#F50057"
        
            # Create a 2-column footer layout
            col_metrics, col_btn = st.columns([2, 1])
            
            with col_metrics:
                st.markdown(f"""
                <div style="background: rgba(20, 25, 40, 0.4); border: 1px solid rgba(255,255,255,0.07); border-radius: 12px; padding: 15px 20px; height: 100%; display: flex; align-items: center; justify-content: space-between;">
                    <div>
                        <div style="font-size: 11px; color: #8b9bb4; text-transform: uppercase; letter-spacing: 1px;">Total Open PnL</div>
                        <div style="font-size: 24px; font-weight: 800; color: {pnl_color};">{total_pnl:.2f}%</div>
                    </div>
                    <div style="text-align: right;">
                        <div style="font-size: 11px; color: #8b9bb4; text-transform: uppercase; letter-spacing: 1px;">Active Positions</div>
                        <div style="font-size: 24px; font-weight: 800; color: #fff;">{len(display_df)}</div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
        
            with col_btn:
                # Add margin top to align with the card
                st.markdown("<div style='margin-top: 8px;'></div>", unsafe_allow_html=True)
                if st.button("🔴 Close Selected", type="primary", use_container_width=True):
                    if not selected_to_close.empty:
                        for symbol, exit_price in zip(selected_to_close['symbol'], selected_to_close['current_price']):
                            success = wm.close_position(symbol, exit_price, "manual")
                            if success:
                                st.toast(f"Closed position for {symbol}", icon="✅")
                        st.rerun()
                    else:
                        st.warning("Select a position to close.")
            
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
