import pandas as pd
import json
import os
import sys
import io
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import sys
from datetime import datetime, timedelta
from config import Config

BACKFILL_MODE = False
INPUT_FILE = Config.COMBINED_FILE if BACKFILL_MODE else "data/combined_dashboard_live.csv"

CONFIG_FILE = "institutional_config.json"
TODAY_OUT = "data/signal_scores_today.csv"
HISTORY_OUT = "data/signal_scores_history.csv"
FEATURE_OUT = "data/signal_feature_store.csv"
RANKED_OUT = "data/active_signals_ranked.csv"

def score_signals(signals):
    signals = signals.copy()
    
    signals["MOMENTUM_RAW"] = signals.apply(lambda x: x["DELIV_PER"] / x["DELIV_PER_3M"] if pd.notna(x.get("DELIV_PER_3M")) and x["DELIV_PER_3M"] > 0 else 0, axis=1)
    signals["FOOTPRINT_RAW"] = signals.apply(lambda x: x["DELIVERY_TURNOVER"] / x["DELIVERY_TURNOVER_3M"] if pd.notna(x.get("DELIVERY_TURNOVER_3M")) and x["DELIVERY_TURNOVER_3M"] > 0 else 0, axis=1)
    signals["STABILITY_RAW"] = signals.apply(lambda x: x["ATW"] / x["ATW_3M"] if pd.notna(x.get("ATW_3M")) and x["ATW_3M"] > 0 else 0, axis=1)
    
    # Set Quality Flags
    signals["HASMOMENTUMDATA"] = signals["DELIV_PER_1W"].notna() & signals["DELIV_PER_1M"].notna()
    signals["HASFOOTPRINTDATA"] = signals.get("DELIVERY_TURNOVER_1W", pd.Series(dtype=float)).notna() & signals.get("DELIVERY_TURNOVER_1M", pd.Series(dtype=float)).notna()
    signals["HASSTABILITYHISTORY20D"] = signals["ATW"].notna() & signals.get("ATW_1M", pd.Series(dtype=float)).notna()
    
    return signals

def run_scoring():
    if not os.path.exists(CONFIG_FILE):
        print(f"ERROR: {CONFIG_FILE} missing. Run validator first.")
        sys.exit(1)
        
    if not os.path.exists(INPUT_FILE):
        print(f"ERROR: {INPUT_FILE} missing.")
        sys.exit(1)
        
    df = pd.read_csv(INPUT_FILE)
    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
    
    from progressive_screener import ProgressiveSpiker
    
    all_scored = []
    all_t2t_scored = []
    
    for dt in sorted(df["DATE"].dropna().unique()):
        day_df = df[df["DATE"] == dt].copy()
        signals = ProgressiveSpiker(day_df).get_signals()
        if signals.empty:
            continue

        scored_df = score_signals(signals)
        if scored_df.empty:
            continue

        scored_df["DATE"] = pd.to_datetime(scored_df["DATE"], errors="coerce")
        
        if "EVER_100_DELIV" in scored_df.columns:
            t2t_mask = scored_df["EVER_100_DELIV"] == True
            t2t_df = scored_df[t2t_mask].copy()
            clean_df = scored_df[~t2t_mask].copy()
            
            if not clean_df.empty:
                all_scored.append(clean_df)
            if not t2t_df.empty:
                all_t2t_scored.append(t2t_df)
        else:
            all_scored.append(scored_df)


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
    
    # --- SAVE GOLDEN ML SIGNALS (GREEN STOCKS) ---
    # The user wants to archive stocks that meet the strict "Green" dashboard criteria 
    # (Fresh Trigger + High Stability) for future ML research.
    golden_signals = today_scores[(today_scores["TRIGGER_COUNT_30D"] == 1) & (today_scores["STABILITY_RAW"] > 3.16)].copy()
    
    if not golden_signals.empty:
        golden_path = "data/ml_golden_signals.csv"
        # We append today's golden signals to the master file
        if os.path.exists(golden_path):
            old_golden = pd.read_csv(golden_path)
            combined_golden = pd.concat([old_golden, golden_signals], ignore_index=True)
            # Remove any accidental duplicates by DATE and SYMBOL
            combined_golden = combined_golden.drop_duplicates(subset=["DATE", "SYMBOL"], keep="last")
        else:
            combined_golden = golden_signals
            
        combined_golden.to_csv(golden_path, index=False)

        print(f"ml_golden_signals.csv appended: {len(golden_signals)} new highly stable institutional footprints today!")
        
    # --- SAVE LATE BLOOMERS ---
    # Stocks that weren't Golden on Day 1, but achieved STABILITY > 3.16 later within the 30-day window
    current_green = recent_pool[(recent_pool["TRIGGER_COUNT_30D"] == 1) & (recent_pool["STABILITY_RAW"] > 3.16)].copy()
    if not current_green.empty:
        golden_path = "data/ml_golden_signals.csv"
        if os.path.exists(golden_path):
            golden_df = pd.read_csv(golden_path)
            # Exclude stocks that are already purely Golden
            current_green = current_green[~current_green["SYMBOL"].isin(golden_df["SYMBOL"])]
            
        if not current_green.empty:
            current_green["LATE_BLOOMER_DATE"] = max_date.strftime("%Y-%m-%d")
            bloomer_path = "data/ml_late_bloomers.csv"
            if os.path.exists(bloomer_path):
                old_bloomers = pd.read_csv(bloomer_path)
                combined_bloomers = pd.concat([old_bloomers, current_green], ignore_index=True)
                # Keep the first time they bloomed
                combined_bloomers = combined_bloomers.drop_duplicates(subset=["SYMBOL"], keep="first")
            else:
                combined_bloomers = current_green
            combined_bloomers.to_csv(bloomer_path, index=False)
            print(f"ml_late_bloomers.csv updated with new late bloomers!")

        
    # Process and save T2T rejected signals for ML training

    if all_t2t_scored:
        t2t_df = pd.concat(all_t2t_scored, ignore_index=True)
        t2t_path = "data/t2t_rejected_signals_history.csv"
        
        t2t_hist = t2t_df[keep_cols].copy()
        if not BACKFILL_MODE and os.path.exists(t2t_path):
            old_t2t = pd.read_csv(t2t_path)
            old_t2t["DATE"] = pd.to_datetime(old_t2t["DATE"], errors="coerce")
            t2t_hist = pd.concat([old_t2t, t2t_hist], ignore_index=True)
            
        t2t_hist = t2t_hist.dropna(subset=["DATE", "SYMBOL", "EXCHANGE"])
        t2t_hist = t2t_hist.drop_duplicates(subset=["DATE", "SYMBOL", "EXCHANGE"], keep="last")
        t2t_hist = t2t_hist.sort_values(["DATE", "COMBINED_SCORE", "SYMBOL"], ascending=[False, False, True])
        t2t_hist.to_csv(t2t_path, index=False)
        print(f"t2t_rejected_signals_history.csv saved: {len(t2t_hist)} rows")

if __name__ == "__main__":
    run_scoring()
