import pandas as pd
import numpy as np
import json
import os
import sys
import io
import joblib
import yfinance as yf

if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

from datetime import datetime, timedelta
from config import Config

BACKFILL_MODE = False
INPUT_FILE = Config.COMBINED_FILE if BACKFILL_MODE else "data/combined_dashboard_live.csv"
CONFIG_FILE = "institutional_config.json"

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

def calculate_atr_and_risk(df, equity=1000000, risk_pct=0.015, max_cap_pct=0.10):
    """Fetch ATR14 from yfinance and calculate Risk parameters"""
    if df.empty:
        return df
        
    df = df.copy()
    symbols = df["SYMBOL"].unique().tolist()
    yf_symbols = [f"{s}.NS" for s in symbols]
    
    print(f"Fetching ATR data for {len(symbols)} symbols...")
    try:
        # Download last 20 days to ensure we can calculate 14-day ATR
        data = yf.download(yf_symbols, period="1mo", progress=False, group_by="ticker")
        
        df["ATR14"] = np.nan
        df["STOP_LOSS"] = np.nan
        df["TAKE_PROFIT"] = np.nan
        df["REC_POS_SIZE_INR"] = np.nan
        
        risk_amount = equity * risk_pct
        max_position = equity * max_cap_pct
        
        for idx, row in df.iterrows():
            sym = row["SYMBOL"]
            close_px = row["CLOSE"]
            
            try:
                # Handle single ticker vs multi-ticker dataframe structure from yfinance
                if len(symbols) == 1:
                    ticker_df = data
                else:
                    ticker_df = data[f"{sym}.NS"]
                    
                if not ticker_df.empty and len(ticker_df) >= 14:
                    # Calculate ATR 14
                    high_low = ticker_df['High'] - ticker_df['Low']
                    high_close = np.abs(ticker_df['High'] - ticker_df['Close'].shift())
                    low_close = np.abs(ticker_df['Low'] - ticker_df['Close'].shift())
                    
                    ranges = pd.concat([high_low, high_close, low_close], axis=1)
                    true_range = np.max(ranges, axis=1)
                    atr14 = true_range.rolling(14).mean().iloc[-1]
                    
                    if pd.notna(atr14) and atr14 > 0:
                        sl_dist = 2.0 * atr14
                        tp_dist = 4.0 * atr14
                        
                        df.at[idx, "ATR14"] = atr14
                        df.at[idx, "STOP_LOSS"] = close_px - sl_dist
                        df.at[idx, "TAKE_PROFIT"] = close_px + tp_dist
                        
                        shares_to_buy = risk_amount / sl_dist
                        pos_size_inr = shares_to_buy * close_px
                        
                        # Cap at 10%
                        if pos_size_inr > max_position:
                            pos_size_inr = max_position
                            
                        df.at[idx, "REC_POS_SIZE_INR"] = pos_size_inr
            except Exception as e:
                pass # If ticker fails, leave as NaN
                
    except Exception as e:
        print(f"Failed to fetch ATR data: {e}")
        
    return df

def run_scoring():
    if not os.path.exists(CONFIG_FILE):
        print(f"ERROR: {CONFIG_FILE} missing.")
        sys.exit(1)
        
    if not os.path.exists(INPUT_FILE):
        print(f"ERROR: {INPUT_FILE} missing.")
        sys.exit(1)
        
    df = pd.read_csv(INPUT_FILE)
    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
    
    from progressive_screener import ProgressiveSpiker
    
    all_scored = []
    
    print("Running Progressive Screener on all dates...")
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

    archive_path = "data/survivors_archive.csv"
    
    if all_scored:
        scored_df = pd.concat(all_scored, ignore_index=True)
        keep_cols = ["DATE", "SYMBOL", "EXCHANGE", "CLOSE", "DELIV_PER", "ATW", "DELIVERY_TURNOVER",
                     "MOMENTUM_RAW", "FOOTPRINT_RAW", "STABILITY_RAW"]
                     
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
    else:
        if os.path.exists(archive_path):
            hist = pd.read_csv(archive_path)
            hist["DATE"] = pd.to_datetime(hist["DATE"], errors="coerce")
        else:
            print("No signals found.")
            sys.exit(0)
            
    # Legacy Pool (45 Days)
    dates_sorted = sorted(hist["DATE"].unique(), reverse=True)
    legacy_cutoff = dates_sorted[min(45, len(dates_sorted))-1]
    legacy_pool = hist[hist["DATE"] >= legacy_cutoff].copy()
    
    # SBIA Pool (45 Days)
    sbia_cutoff = dates_sorted[min(45, len(dates_sorted))-1]
    sbia_pool = hist[hist["DATE"] >= sbia_cutoff].copy()

    # Update latest live data
    df_latest = df.set_index("SYMBOL")
    update_cols = ["CLOSE", "DELIV_PER", "DELIVERY_TURNOVER", "ATW", 
                   "DELIV_PER_1W", "DELIV_PER_1M", "DELIV_PER_3M", 
                   "DELIVERY_TURNOVER_1W", "DELIVERY_TURNOVER_1M", "DELIVERY_TURNOVER_3M", 
                   "ATW_1W", "ATW_1M", "ATW_3M"]
                   
    for p in [legacy_pool, sbia_pool]:
        for col in update_cols:
            if col in df_latest.columns:
                p[col] = p["SYMBOL"].map(df_latest[col]).fillna(p.get(col, 0))
        
    legacy_pool = score_signals(legacy_pool)
    sbia_pool = score_signals(sbia_pool)
    
    # Calculate Percentiles
    for p in [legacy_pool, sbia_pool]:
        p["MOMENTUM_SCORE"] = p["MOMENTUM_RAW"].rank(pct=True, na_option="bottom")
        p["FOOTPRINT_SCORE"] = p["FOOTPRINT_RAW"].rank(pct=True, na_option="bottom")
        p["STABILITY_SCORE"] = p["STABILITY_RAW"].rank(pct=True, na_option="bottom")
        
        p["COMBINED_SCORE"] = (
            0.4 * p["MOMENTUM_SCORE"] +
            0.4 * p["FOOTPRINT_SCORE"] +
            0.2 * p["STABILITY_SCORE"]
        )
        
        # True 30-Day Rolling Trigger Count
        p["DATE"] = pd.to_datetime(p["DATE"])
        p["TRIGGER_COUNT_30D"] = 1
        
        for idx, row in p.iterrows():
            window_start = row["DATE"] - pd.Timedelta(days=30)
            count = p[(p["SYMBOL"] == row["SYMBOL"]) & (p["DATE"] <= row["DATE"]) & (p["DATE"] > window_start)].shape[0]
            p.at[idx, "TRIGGER_COUNT_30D"] = count
        
        # Raw AI Score (Old)
        p["AI_SCORE"] = (
            0.6 * p["STABILITY_SCORE"] +
            0.1 * p["MOMENTUM_SCORE"] +
            0.1 * p["FOOTPRINT_SCORE"] +
            0.2 * (1.0 / p["TRIGGER_COUNT_30D"])
        )
        
        # Institutional ML Metrics (Applied to both Legacy and SBIA)
        p["SIS"] = ((p['STABILITY_SCORE'] + 1)**0.50 * 
                    (p['FOOTPRINT_SCORE'] + 1)**0.30 * 
                    (p['MOMENTUM_SCORE'] + 1)**0.20) - 1
                    

        
        p['Whale_Density'] = (p['ATW'] / p['DELIVERY_TURNOVER'].replace(0, np.nan)).fillna(0) * 100000
        p['Implied_Trades'] = (p['DELIVERY_TURNOVER'] / p['ATW'].replace(0, np.nan)).fillna(0)
        
    # Apply ML Sanity Filters to SBIA Pool
    print("Applying SBIA Institutional Machine Learning Filters...")
    
    # 1. Baseline Sanity Filters (Liquidity only, let the ML model decide the rest)
    sanity_mask = (
        (sbia_pool['DELIVERY_TURNOVER'] > 10000000) &  # > 1 Cr Minimum Delivery Turnover
        (sbia_pool['Whale_Density'] > 0)
    )
    
    # 2. ML Gate
    if os.path.exists("shadow_box_model.pkl"):
        model = joblib.load("shadow_box_model.pkl")
        features = ['SIS', 'Whale_Density', 'Implied_Trades']
        
        # Predict only on those that have all features
        pred_mask = sbia_pool[features].notna().all(axis=1)
        sbia_pool.loc[pred_mask, "AI_WIN_PROBABILITY"] = model.predict_proba(sbia_pool.loc[pred_mask, features])[:, 1] * 100
        sbia_pool["AI_WIN_PROBABILITY"] = sbia_pool["AI_WIN_PROBABILITY"].fillna(0)
        
        final_sbia_mask = sanity_mask & (sbia_pool["AI_WIN_PROBABILITY"] >= 60.0)
        sbia_live_watchlist = sbia_pool[final_sbia_mask].copy()
    else:
        print("WARNING: shadow_box_model.pkl not found! Using sanity filters only for SBIA.")
        sbia_live_watchlist = sbia_pool[sanity_mask].copy()
        sbia_live_watchlist["AI_WIN_PROBABILITY"] = 0
        
    # Apply Legacy Hard Gates
    print("Applying Legacy Institutional Ranking Gates...")
    legacy_mask = (legacy_pool["AI_SCORE"] > 0.60) & (legacy_pool["SIS"].between(0.15, 0.93))
    legacy_pool = legacy_pool[legacy_mask].copy()

    # Calculate Risk Data
    legacy_watchlist = calculate_atr_and_risk(legacy_pool)
    sbia_live_watchlist = calculate_atr_and_risk(sbia_live_watchlist)
    
    # Sort and Save
    legacy_watchlist = legacy_watchlist.sort_values(by=["SIS"], ascending=[False])
    sbia_live_watchlist = sbia_live_watchlist.sort_values(by=["AI_WIN_PROBABILITY", "STABILITY_RAW"], ascending=[False, False])
    
    legacy_watchlist.to_csv("data/legacy_watchlist.csv", index=False)
    sbia_live_watchlist.to_csv("data/sbia_institutional_watchlist.csv", index=False)
    
    # Backward compatibility for old Streamlit dashboard while testing
    legacy_watchlist.to_csv("data/active_signals_ranked.csv", index=False)
    
    print(f"Legacy Watchlist saved: {len(legacy_watchlist)} rows")
    print(f"SBIA Institutional Watchlist saved: {len(sbia_live_watchlist)} rows")

if __name__ == "__main__":
    run_scoring()
