import pandas as pd
import numpy as np
import scipy.stats
import time
import os
import sys

sys.path.append('.')
from fundamental_fetcher import FundamentalFetcher

def main():
    print("=== Stage 1: Loading Trades Ledger ===")
    df = pd.read_csv('data/trades_ledger.csv')
    
    # Filter only resolved trades
    valid_outcomes = ['WINNER', 'LOSER']
    df = df[df['outcome_tag'].isin(valid_outcomes)].copy()
    print(f"Loaded {len(df)} resolved trades.")
    
    winners = df[df['outcome_tag'] == 'WINNER']
    losers = df[df['outcome_tag'] == 'LOSER']
    print(f"Winners: {len(winners)}")
    print(f"Losers: {len(losers)}")
    
    print("\n=== Stage 2 & 3: Batch Scoring (Blind) ===")
    # Use a separate cache for analysis to avoid touching live cache
    import fundamental_fetcher
    fundamental_fetcher.CACHE_PATH = 'data/fundamental_analysis_cache.json'
    fetcher = fundamental_fetcher.FundamentalFetcher()
    
    scores = []
    unique_tickers = df['ticker'].unique()
    print(f"Fetching fundamentals for {len(unique_tickers)} unique tickers...")
    
    import json
    try:
        with open('data/rpt_filings_cache.json', 'r') as f:
            rpt_cache = json.load(f)
    except FileNotFoundError:
        rpt_cache = {}
        
    for i, ticker in enumerate(unique_tickers):
        print(f"[{i+1}/{len(unique_tickers)}] Fetching {ticker}...")
        try:
            fund_data = fetcher.fetch(ticker)
            
            # Map trend string to numeric
            trend_val = np.nan
            raw_trend = fund_data.get('interest_coverage_trend', '')
            if raw_trend == 'improving':
                trend_val = 1
            elif raw_trend == 'deteriorating':
                trend_val = -1
            elif raw_trend == 'stable':
                trend_val = 0
            
            # Find BSE scrip if available
            bse_scrip = df[df['ticker'] == ticker]['bse_scrip'].iloc[0] if 'bse_scrip' in df.columns else np.nan
            
            # Try to get RPT from Phase B cache
            rpt_pct = rpt_cache.get(ticker, np.nan)
            
            scores.append({
                'ticker': ticker,
                'sector_type': fund_data.get('sector_type', 'unknown'),
                'op_lev_ratio': fund_data.get('op_lev_ratio', np.nan),
                'roice_pct': fund_data.get('roice_pct', np.nan),
                'interest_coverage_trend_numeric': trend_val,
                'rpt_pct_mcap': rpt_pct,
                '_data_confidence': 'estimated_pit' # As per plan
            })
            time.sleep(0.1) # small delay to prevent IP bans if hitting APIs
        except Exception as e:
            print(f"  Error fetching {ticker}: {e}")
            scores.append({'ticker': ticker})
            
    scores_df = pd.DataFrame(scores)
    # Merge back to trades
    df = df.merge(scores_df, on='ticker', how='left')
    df.to_csv('data/fundamental_scores.csv', index=False)
    
    print("\n=== Stage 4: Statistical Analysis ===")
    
    TIER_1_METRICS = [
        "op_lev_ratio",
        "rpt_pct_mcap",
        "roice_pct",
        "interest_coverage_trend_numeric"
    ]
    
    results = []
    
    winners_df = df[df["outcome_tag"] == "WINNER"]
    losers_df  = df[df["outcome_tag"] == "LOSER"]
    
    for metric in TIER_1_METRICS:
        if metric not in df.columns:
            continue
            
        if metric == 'op_lev_ratio':
            # EXCLUDE FINANCIALS
            w_vals_raw = winners_df[winners_df['sector_type'] != 'financial'][metric]
            l_vals_raw = losers_df[losers_df['sector_type'] != 'financial'][metric]
            
            w_vals = pd.to_numeric(w_vals_raw, errors='coerce').dropna()
            l_vals = pd.to_numeric(l_vals_raw, errors='coerce').dropna()
            
            # WINSORIZATION (1st & 99th percentile of combined pool)
            combined = pd.concat([w_vals, l_vals])
            p1 = combined.quantile(0.01)
            p99 = combined.quantile(0.99)
            
            w_vals = w_vals.clip(lower=p1, upper=p99)
            l_vals = l_vals.clip(lower=p1, upper=p99)
        else:
            w_vals = pd.to_numeric(winners_df[metric], errors='coerce').dropna()
            l_vals = pd.to_numeric(losers_df[metric], errors='coerce').dropna()
        
        n_winners = len(w_vals)
        n_losers = len(l_vals)
        
        w_mean = w_vals.mean() if n_winners > 0 else np.nan
        l_mean = l_vals.mean() if n_losers > 0 else np.nan
        
        if n_winners >= 10 and n_losers >= 10:
            t_stat, p_val = scipy.stats.ttest_ind(w_vals, l_vals, equal_var=False)
        else:
            p_val = np.nan
            
        coverage = ((n_winners + n_losers) / len(df)) * 100
        discriminates = "Yes" if p_val < 0.05 else "No"
        
        results.append({
            'Metric': metric,
            'N Winners': n_winners,
            'N Losers': n_losers,
            'Winner Mean': round(w_mean, 2) if pd.notna(w_mean) else 'N/A',
            'Loser Mean': round(l_mean, 2) if pd.notna(l_mean) else 'N/A',
            'p-value': round(p_val, 4) if pd.notna(p_val) else 'N/A',
            'Data Coverage %': f"{round(coverage, 1)}%",
            'Discriminates (p<0.05)': discriminates
        })
    
    results_df = pd.DataFrame(results)
    print("\n=== Tier 1 Metrics Report ===")
    print(results_df.to_string(index=False))
    
    # Print descriptive breakdowns
    print("\n=== By-Engine Breakdown ===")
    engine_stats = df.groupby('engine').agg(
        N=('ticker', 'count'),
        Winner_Pct=('outcome_tag', lambda x: (x == 'WINNER').mean() * 100),
        Mean_Return_Winners=('return_pct', lambda x: df.loc[x.index, 'return_pct'][df.loc[x.index, 'outcome_tag'] == 'WINNER'].mean()),
        Mean_Return_Losers=('return_pct', lambda x: df.loc[x.index, 'return_pct'][df.loc[x.index, 'outcome_tag'] == 'LOSER'].mean())
    ).reset_index()
    engine_stats = engine_stats.round(2)
    print(engine_stats.to_string(index=False))
    
    print("\n=== By-Cap-Class Breakdown ===")
    if 'market_cap_class' in df.columns:
        cap_stats = df.groupby('market_cap_class').agg(
            N_Total=('ticker', 'count'),
            N_Winners=('outcome_tag', lambda x: (x == 'WINNER').sum()),
            N_Losers=('outcome_tag', lambda x: (x == 'LOSER').sum())
        ).reset_index()
        cap_stats['Note'] = cap_stats['N_Total'].apply(lambda x: 'N>=10' if x >= 10 else 'N<10 (Too Small)')
        print(cap_stats.to_string(index=False))
        
    results_df.to_csv('data/fundamental_analysis_results.csv', index=False)
    print("\nReport saved to data/fundamental_analysis_results.csv")

if __name__ == "__main__":
    main()
