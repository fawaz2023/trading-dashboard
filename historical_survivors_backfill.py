import os
import sys
import argparse
import pandas as pd
from backfill_3m_signals import load_all_raw_data, compute_progressive_averages
from calculate_active_signals import score_signals
from progressive_screener import ProgressiveSpiker

def backfill_survivors(days=5):
    print("=" * 70)
    print(f"HISTORICAL SURVIVORS BACKFILL ({days} days)")
    print("=" * 70)
    
    # 1. Load raw data and compute 3M baselines
    df_all = load_all_raw_data()
    df_all = compute_progressive_averages(df_all)
    
    max_date = df_all["DATE"].max()
    cutoff = max_date - pd.Timedelta(days=days)
    
    all_dates = sorted(df_all["DATE"].dropna().unique())
    
    target_dates = [d for d in all_dates if d >= cutoff]
    
    print(f"Targeting {len(target_dates)} dates from {cutoff.date()} to {max_date.date()}")
    
    all_scored = []
    
    for dt in target_dates:
        day_df = df_all[df_all["DATE"] == dt].copy()
        if day_df.empty:
            continue
            
        signals = ProgressiveSpiker(day_df).get_signals()
        if signals.empty:
            continue
            
        scored = score_signals(signals)
        if scored.empty:
            continue
            
        scored["DATE"] = pd.to_datetime(scored["DATE"], errors="coerce")
        
        # Exclude T2T rejects from the main survivor archive
        if "EVER_100_DELIV" in scored.columns:
            clean = scored[~scored["EVER_100_DELIV"]].copy()
        else:
            clean = scored.copy()
            
        if not clean.empty:
            dt_str = pd.Timestamp(dt).strftime("%Y-%m-%d")
            print(f"  {dt_str}: {len(clean)} clean survivors")
            all_scored.append(clean)
            
    if not all_scored:
        print("No survivors found in the backfill window.")
        return
        
    backfill_df = pd.concat(all_scored, ignore_index=True)
    
    keep_cols = ["DATE", "SYMBOL", "EXCHANGE", "CLOSE", "DELIV_PER", "ATW", "DELIVERY_TURNOVER",
                 "MOMENTUM_RAW", "FOOTPRINT_RAW", "STABILITY_RAW",
                 "HASMOMENTUMDATA", "HASFOOTPRINTDATA", "HASSTABILITYHISTORY20D"]
                 
    for c in keep_cols:
        if c not in backfill_df.columns:
            backfill_df[c] = pd.NA
            
    hist = backfill_df[keep_cols].copy()
    
    archive_path = "data/survivors_archive.csv"
    if os.path.exists(archive_path):
        old_hist = pd.read_csv(archive_path)
        old_hist["DATE"] = pd.to_datetime(old_hist["DATE"], errors="coerce")
        hist = pd.concat([old_hist, hist], ignore_index=True)
        
    # Deduplicate by DATE + SYMBOL
    hist = hist.dropna(subset=["DATE", "SYMBOL", "EXCHANGE"])
    hist = hist.drop_duplicates(subset=["DATE", "SYMBOL", "EXCHANGE"], keep="last")
    hist = hist.sort_values(["DATE", "SYMBOL"], ascending=[False, True])
    
    hist.to_csv(archive_path, index=False)
    print(f"\nSuccessfully appended to {archive_path}")
    print(f"Archive now contains {len(hist)} rows across {hist['DATE'].nunique()} trading days.")
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=5, help="Days to backfill")
    args = parser.parse_args()
    backfill_survivors(args.days)
