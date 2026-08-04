with open('calculate_active_signals.py', 'r', encoding='utf-8') as f:
    content = f.read()

import_code = '''
    archive_path = "data/survivors_archive.csv"
    
    # 1. Save new signals to archive
    if all_scored:
        scored_df = pd.concat(all_scored, ignore_index=True)
        print(f"Computed raw ratios for {len(scored_df)} new survivors across {scored_df['DATE'].nunique()} dates")
        
        keep_cols = ["DATE", "SYMBOL", "EXCHANGE", "CLOSE", "DELIV_PER", "ATW", "DELIVERY_TURNOVER",
                     "MOMENTUM_RAW", "FOOTPRINT_RAW", "STABILITY_RAW",
                     "HASMOMENTUMDATA", "HASFOOTPRINTDATA", "HASSTABILITYHISTORY20D"]
                     
        for c in keep_cols:
            if c not in scored_df.columns:
                scored_df[c] = pd.NA
                
        hist = scored_df[keep_cols].copy()
        
        if not BACKFILL_MODE and os.path.exists(archive_path):
            old_hist = pd.read_csv(archive_path)
            old_hist["DATE"] = pd.to_datetime(old_hist["DATE"], errors="coerce")
            hist = pd.concat([old_hist, hist], ignore_index=True)
            
        hist = hist.dropna(subset=["DATE", "SYMBOL", "EXCHANGE"])
        hist = hist.drop_duplicates(subset=["DATE", "SYMBOL", "EXCHANGE"], keep="last")
        hist = hist.sort_values(["DATE", "SYMBOL"], ascending=[False, True])
        
        if not BACKFILL_MODE:
            hist.to_csv(archive_path, index=False)
            print(f"survivors_archive.csv updated: {len(hist)} rows")
    else:
        print("No new signals today. Updating existing active signals.")
        if os.path.exists(archive_path):
            hist = pd.read_csv(archive_path)
            hist["DATE"] = pd.to_datetime(hist["DATE"], errors="coerce")
        else:
            hist = pd.DataFrame()
            
    if hist.empty:
        print("No historical signals to rank.")
        sys.exit(0)
        
    # 2. Filter to 30 days rolling survivors
    max_date = hist["DATE"].max()
    dates_sorted = sorted(hist["DATE"].unique(), reverse=True)
    cutoff_date = dates_sorted[min(30, len(dates_sorted))-1]
    
    recent_pool = hist[hist["DATE"] >= cutoff_date].copy()
    
    # 3. Update active signals with LATEST data from combined_dashboard_live.csv
    # We map the latest fundamental columns from 'df' to 'recent_pool' based on SYMBOL
    update_cols = ["CLOSE", "DELIV_PER", "DELIVERY_TURNOVER", "ATW", 
                   "DELIV_PER_1W", "DELIV_PER_1M", "DELIV_PER_3M", 
                   "DELIVERY_TURNOVER_1W", "DELIVERY_TURNOVER_1M", "DELIVERY_TURNOVER_3M", 
                   "ATW_1W", "ATW_1M", "ATW_3M"]
                   
    df_latest = df.set_index("SYMBOL")
    for col in update_cols:
        if col in df_latest.columns:
            recent_pool[col] = recent_pool["SYMBOL"].map(df_latest[col]).fillna(recent_pool.get(col, 0))
            
    # 4. Recalculate RAW metrics using the updated LIVE data
    recent_pool = score_signals(recent_pool)
    
    # 5. Rank percentiles globally across the recent pool
    recent_pool["MOMENTUM_SCORE"] = recent_pool["MOMENTUM_RAW"].rank(pct=True, na_option="bottom")
    recent_pool["FOOTPRINT_SCORE"] = recent_pool["FOOTPRINT_RAW"].rank(pct=True, na_option="bottom")
    recent_pool["STABILITY_SCORE"] = recent_pool["STABILITY_RAW"].rank(pct=True, na_option="bottom")
    
    recent_pool["COMBINED_SCORE"] = (
        0.4 * recent_pool["MOMENTUM_SCORE"] +
        0.4 * recent_pool["FOOTPRINT_SCORE"] +
        0.2 * recent_pool["STABILITY_SCORE"]
    )
    
    # Flag repeat triggers in the last 30 days
    trigger_counts = recent_pool["SYMBOL"].value_counts()
    recent_pool["TRIGGER_COUNT_30D"] = recent_pool["SYMBOL"].map(trigger_counts)
    recent_pool["REPEAT_FLAG"] = recent_pool["TRIGGER_COUNT_30D"] > 1
    
    # Calculate ML-Driven AI Score
    recent_pool["AI_SCORE"] = (
        0.6 * recent_pool["STABILITY_SCORE"] +
        0.1 * recent_pool["MOMENTUM_SCORE"] +
        0.1 * recent_pool["FOOTPRINT_SCORE"] +
        0.2 * (1.0 / recent_pool["TRIGGER_COUNT_30D"])
    )
    
    # Output artifacts for the dashboard
    active_path = "data/active_signals_ranked.csv"
    today_path = "data/signal_scores_today.csv"
    history_path = "data/signal_scores_history.csv"
    
    recent_pool = recent_pool.sort_values(["DATE", "COMBINED_SCORE", "FOOTPRINT_RAW", "SYMBOL"], 
                                          ascending=[False, False, False, True])
                                          
    recent_pool.to_csv(active_path, index=False)
    recent_pool.to_csv(history_path, index=False)
    
    today_scores = recent_pool[recent_pool["DATE"] == max_date].copy()
    today_scores.to_csv(today_path, index=False)
    
    print(f"active_signals_ranked.csv (30-day view) updated and saved: {len(recent_pool)} rows")
    print(f"signal_scores_today.csv saved: {len(today_scores)} rows")
    
    # Process and save T2T rejected signals for ML training
'''

old_code_start = '''    if not all_scored and not all_t2t_scored:
        print(f"Loaded 0 signals from {INPUT_FILE}")
        sys.exit(0)
        
    if all_scored:'''

old_code_end = '''    # Process and save T2T rejected signals for ML training'''

if old_code_start in content and old_code_end in content:
    start_idx = content.find(old_code_start)
    end_idx = content.find(old_code_end) + len(old_code_end)
    new_content = content[:start_idx] + import_code + content[end_idx:]
    with open('calculate_active_signals.py', 'w', encoding='utf-8') as f:
        f.write(new_content)
    print("Patched calculate_active_signals.py to update metrics daily!")
else:
    print("Failed to find target block in calculate_active_signals.py")
