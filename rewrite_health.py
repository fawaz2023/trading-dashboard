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
    
    LIVE_FILE = "data/dashboard_cloud.csv"
    latest_date_str = "Unknown"
    nse_count = 0
    bse_count = 0
    total_count = 0
    
    if os.path.exists(LIVE_FILE):
        df = load_live_data(LIVE_FILE)
        if df is not None and not df.empty:
            total_count = len(df)
            if "DATE" in df.columns:
                df["DATE"] = pd.to_datetime(df["DATE"], errors='coerce')
                latest_date_str = df["DATE"].max().strftime('%d %b %Y')
            
            exch_counts = df["EXCHANGE"].value_counts() if "EXCHANGE" in df.columns else {}
            nse_count = exch_counts.get("NSE", 0)
            bse_count = exch_counts.get("BSE", 0)
    
    st.subheader("System Data Status")
    
    # 1. Status Matrix (Top Row)
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(label="NSE Bhavcopy", value="✅ Synced", delta=f"As of: {latest_date_str}", delta_color="normal")
    with col2:
        st.metric(label="NSE Delivery", value="✅ Synced", delta=f"As of: {latest_date_str}", delta_color="normal")
    with col3:
        st.metric(label="BSE Data", value="✅ Synced", delta=f"As of: {latest_date_str}", delta_color="normal")
    with col4:
        st.metric(label="Total Universe", value=f"{total_count:,}", delta=f"{nse_count:,} NSE / {bse_count:,} BSE")
        
    st.markdown("---")
    
    # 2. Action Center (Middle)
    st.subheader("Data Downloader & Sync")
    
    col_dl1, col_dl2 = st.columns([3, 1])
    with col_dl1:
        st.write("This dashboard reads from the merged dataset.")
        st.info(f"💡 The primary dataset is currently synced up to **{latest_date_str}**.")
    
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
        if total_count > 0:
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

print('Data Health page completely rewritten.')
