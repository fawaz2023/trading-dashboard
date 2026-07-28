import pandas as pd
import json
import os
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
    signals["MOMENTUM_SCORE"] = (signals["DELIV_PER_1W"] > signals["DELIV_PER_1M"]).astype(int)
    signals["FOOTPRINT_SCORE"] = (signals["DELIVERY_TURNOVER"] > signals["DELIVERY_TURNOVER_1M"]).astype(int)
    signals["STABILITY_SCORE"] = (signals["ATW"] > signals["ATW_1M"]).astype(int)
    
    signals["COMBINED_SCORE"] = signals["MOMENTUM_SCORE"] + signals["FOOTPRINT_SCORE"] + signals["STABILITY_SCORE"]
    
    # Set Quality Flags (Checking actual data presence, NOT if the score passed)
    signals["HASMOMENTUMDATA"] = signals["DELIV_PER_1W"].notna() & signals["DELIV_PER_1M"].notna()
    signals["HASFOOTPRINTDATA"] = signals["DELIVERY_TURNOVER"].notna() & signals["DELIVERY_TURNOVER_1M"].notna()
    signals["HASSTABILITYHISTORY20D"] = signals["ATW"].notna() & signals["ATW_1M"].notna()
    
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
    
    # Filter out 100% delivery (T2T) before processing if needed
    if "EVER_100_DELIV" in df.columns:
        df = df[df["EVER_100_DELIV"] == False]
    
    for dt in sorted(df["DATE"].dropna().unique()):
        day_df = df[df["DATE"] == dt].copy()
        signals = ProgressiveSpiker(day_df).get_signals()
        if signals.empty:
            continue

        scored_df = score_signals(signals)
        if scored_df.empty:
            continue

        scored_df["DATE"] = pd.to_datetime(scored_df["DATE"], errors="coerce")
        all_scored.append(scored_df)

    if not all_scored:
        print(f"Loaded 0 signals from {INPUT_FILE}")
        print("active_signals_ranked.csv saved: 0 rows")
        sys.exit(0)
        
    scored_df = pd.concat(all_scored, ignore_index=True)
    print(f"Scored {len(scored_df)} stocks (binary confluence) across {scored_df['DATE'].nunique()} dates")
    
    # Save feature store (optional backup append)
    if os.path.exists(FEATURE_OUT):
        scored_df.to_csv(FEATURE_OUT, mode='a', header=False, index=False)
    else:
        scored_df.to_csv(FEATURE_OUT, index=False)
    print("signal_feature_store.csv appended")
    
    history_path = "data/signal_scores_history.csv"
    active_path = "data/active_signals_ranked.csv"
    today_path = "data/signal_scores_today.csv"

    keep_cols = ["DATE", "SYMBOL", "EXCHANGE", "CLOSE", "DELIV_PER", "ATW", 
                 "MOMENTUM_SCORE", "FOOTPRINT_SCORE", "STABILITY_SCORE", "COMBINED_SCORE",
                 "HASMOMENTUMDATA", "HASFOOTPRINTDATA", "HASSTABILITYHISTORY20D"]
    
    # Filter columns
    hist = scored_df[keep_cols].copy()
    
    if not BACKFILL_MODE:
        # Standard daily append mode
        if os.path.exists(history_path):
            old_hist = pd.read_csv(history_path)
            old_hist["DATE"] = pd.to_datetime(old_hist["DATE"], errors="coerce")
            hist = pd.concat([old_hist, hist], ignore_index=True)
            
    hist = hist.dropna(subset=["DATE", "SYMBOL", "EXCHANGE"])
    hist = hist.drop_duplicates(subset=["DATE", "SYMBOL", "EXCHANGE"], keep="last")
    hist = hist.sort_values(["DATE", "COMBINED_SCORE", "SYMBOL"], ascending=[False, False, True])
    hist.to_csv(history_path, index=False)
    
    max_date = hist["DATE"].max()
    cutoff = max_date - pd.Timedelta(days=14)
    
    active = hist[hist["DATE"] >= cutoff].copy()
    active = active.drop_duplicates(subset=["DATE", "SYMBOL", "EXCHANGE"], keep="last")
    active = active.sort_values(["COMBINED_SCORE", "DATE", "SYMBOL"], ascending=[False, False, True])
    active.to_csv(active_path, index=False)
    
    today_scores = hist[hist["DATE"] == max_date].copy()
    today_scores = today_scores.sort_values(["COMBINED_SCORE", "SYMBOL"], ascending=[False, True])
    today_scores.to_csv(today_path, index=False)
    
    print(f"signal_scores_history.csv saved: {len(hist)} rows")
    print(f"active_signals_ranked.csv saved: {len(active)} rows")
    print(f"signal_scores_today.csv saved: {len(today_scores)} rows")

if __name__ == "__main__":
    run_scoring()
