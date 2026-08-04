with open('dashboard_full.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

new_lines = []
in_data_health = False

for line in lines:
    if line.startswith('elif page == "Data Health":'):
        in_data_health = True
        
        # Inject the new Data Health code
        new_health_code = """elif page == "Data Health":
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
        df = load_live_data(LIVE_FILE)
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

"""
        new_lines.append(new_health_code)
        continue
    
    if in_data_health and line.strip() == 'elif page == "Signals":':
        in_data_health = False
        new_lines.append("    # SIGNALS PAGE\n")
        new_lines.append(line)
        continue
        
    if in_data_health:
        # Skip all the old lines
        pass
    else:
        new_lines.append(line)

with open('dashboard_full.py', 'w', encoding='utf-8') as f:
    f.writelines(new_lines)

print('Dashboard updated to read data_status.json.')
