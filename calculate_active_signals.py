import pandas as pd
import json
import os
import sys
from datetime import datetime, timedelta

CONFIG_FILE = "institutional_config.json"
INPUT_FILE = "data/combined_dashboard_live.csv"
TODAY_OUT = "data/signal_scores_today.csv"
HISTORY_OUT = "data/signal_scores_history.csv"
FEATURE_OUT = "data/signal_feature_store.csv"
RANKED_OUT = "data/active_signals_ranked.csv"

def run_scoring():
    if not os.path.exists(CONFIG_FILE):
        print(f"ERROR: {CONFIG_FILE} missing. Run validator first.")
        sys.exit(1)
        
    if not os.path.exists(INPUT_FILE):
        print(f"ERROR: {INPUT_FILE} missing.")
        sys.exit(1)
        
    df = pd.read_csv(INPUT_FILE)
    
    # 1. Get filtered signals using the core engine
    try:
        from progressive_screener import ProgressiveSpiker
        # Filter out 100% delivery (T2T) before scoring
        if "EVER_100_DELIV" in df.columns:
            df_filt = df[df["EVER_100_DELIV"] == False]
        else:
            df_filt = df
        signals = ProgressiveSpiker(df_filt).get_signals()
    except Exception as e:
        print(f"ERROR: Failed to run ProgressiveSpiker: {e}")
        sys.exit(1)
        
    if signals.empty:
        print("Loaded 0 signals from combined_dashboard_live.csv")
        print("active_signals_ranked.csv saved: 0 rows")
        sys.exit(0)
        
    print(f"Loaded {len(signals)} signals from combined_dashboard_live.csv")
    
    # 2. Binary Option C Scoring
    # MOMENTUM: 1-Week avg > 1-Month avg (Corrected to track progressive uptrend, not single day)
    # FOOTPRINT (Proxy): Delivery Turnover > 1-Month Delivery Turnover (Since raw commitment ratio isn't in baseline)
    # STABILITY (Proxy): ATW > 1-Month ATW (Since raw inverse volatility isn't in baseline)
    
    signals["MOMENTUM_SCORE"] = (signals["DELIV_PER_1W"] > signals["DELIV_PER_1M"]).astype(int)
    signals["FOOTPRINT_SCORE"] = (signals["DELIVERY_TURNOVER"] > signals["DELIVERY_TURNOVER_1M"]).astype(int)
    signals["STABILITY_SCORE"] = (signals["ATW"] > signals["ATW_1M"]).astype(int)
    
    signals["COMBINED_SCORE"] = signals["MOMENTUM_SCORE"] + signals["FOOTPRINT_SCORE"] + signals["STABILITY_SCORE"]
    
    # Set Quality Flags (Checking actual data presence, NOT if the score passed)
    signals["HASMOMENTUMDATA"] = signals["DELIV_PER_1W"].notna() & signals["DELIV_PER_1M"].notna()
    signals["HASFOOTPRINTDATA"] = signals["DELIVERY_TURNOVER"].notna() & signals["DELIVERY_TURNOVER_1M"].notna()
    signals["HASSTABILITYHISTORY20D"] = signals["ATW"].notna() & signals["ATW_1M"].notna()
    
    print(f"Scored {len(signals)} stocks (binary confluence)")
    
    # Add timestamp
    signals["DATE"] = pd.to_datetime(signals["DATE"], errors='coerce')
    today_date = signals["DATE"].max()
    if pd.isna(today_date):
        today_date = pd.to_datetime(datetime.now().strftime("%Y-%m-%d"))
        signals["DATE"] = today_date
        
    # Format output for TODAY
    signals.to_csv(TODAY_OUT, index=False)
    print(f"signal_scores_today.csv saved: {len(signals)} rows")
    
    # Format output for FEATURE STORE
    if os.path.exists(FEATURE_OUT):
        signals.to_csv(FEATURE_OUT, mode='a', header=False, index=False)
    else:
        signals.to_csv(FEATURE_OUT, index=False)
    print("signal_feature_store.csv appended")
    
    # Handle HISTORY and deduplication
    keep_cols = ["DATE", "SYMBOL", "EXCHANGE", "CLOSE", "DELIV_PER", "ATW", 
                 "MOMENTUM_SCORE", "FOOTPRINT_SCORE", "STABILITY_SCORE", "COMBINED_SCORE",
                 "HASMOMENTUMDATA", "HASFOOTPRINTDATA", "HASSTABILITYHISTORY20D"]
    
    history_df = signals[keep_cols].copy()
    
    if os.path.exists(HISTORY_OUT):
        old_history = pd.read_csv(HISTORY_OUT)
        old_history["DATE"] = pd.to_datetime(old_history["DATE"], errors='coerce')
        combined_history = pd.concat([old_history, history_df], ignore_index=True)
        # Dedup on Date, Symbol, Exchange
        combined_history = combined_history.drop_duplicates(subset=["DATE", "SYMBOL", "EXCHANGE"], keep="last")
    else:
        combined_history = history_df
        
    combined_history.to_csv(HISTORY_OUT, index=False)
    print("signal_scores_history.csv appended")
    
    # Define ACTIVE EXPIRY WINDOW (10 Trading Days -> approx 14 calendar days)
    cutoff_date = today_date - timedelta(days=14)
    active_signals = combined_history[combined_history["DATE"] >= cutoff_date].copy()
    
    # For UI Ranking: Dedup to get only the latest score per symbol in the active window
    active_signals = active_signals.sort_values(by="DATE", ascending=True)
    active_signals = active_signals.drop_duplicates(subset=["SYMBOL", "EXCHANGE"], keep="last")
    
    # Sort by COMBINED_SCORE descending
    active_signals = active_signals.sort_values(by="COMBINED_SCORE", ascending=False)
    
    active_signals.to_csv(RANKED_OUT, index=False)
    print(f"active_signals_ranked.csv saved: {len(active_signals)} rows")

if __name__ == "__main__":
    run_scoring()
