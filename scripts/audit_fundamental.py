import pandas as pd
import numpy as np
import scipy.stats
import json
import os
import time

df = pd.read_csv('data/fundamental_scores.csv')

print('='*60)
print('AUDIT 1: OUTLIER EFFECT ON OP_LEV_RATIO')
print('='*60)
winners_raw = df[df['outcome_tag']=='WINNER']['op_lev_ratio'].dropna()
losers_raw = df[df['outcome_tag']=='LOSER']['op_lev_ratio'].dropna()
t, p = scipy.stats.ttest_ind(winners_raw, losers_raw, equal_var=False)
print(f'WITH OUTLIER: Winner mean={winners_raw.mean():.2f}, Loser mean={losers_raw.mean():.2f}, p={p:.4f}')
print(f'Winner std: {winners_raw.std():.2f}, Loser std: {losers_raw.std():.2f}')
print(f'Winner MEDIAN: {winners_raw.median():.2f}, Loser MEDIAN: {losers_raw.median():.2f}')

df_no_outlier = df[df['op_lev_ratio'].abs() < 50]
w2 = df_no_outlier[df_no_outlier['outcome_tag']=='WINNER']['op_lev_ratio'].dropna()
l2 = df_no_outlier[df_no_outlier['outcome_tag']=='LOSER']['op_lev_ratio'].dropna()
t2, p2 = scipy.stats.ttest_ind(w2, l2, equal_var=False)
print(f'WITHOUT OUTLIERS (|ratio|<50): Winner mean={w2.mean():.2f}, Loser mean={l2.mean():.2f}, p={p2:.4f}')
print(f'Winner MEDIAN: {w2.median():.2f}, Loser MEDIAN: {l2.median():.2f}')
print()

print('='*60)
print('AUDIT 2: INTEREST COVERAGE TREND DISTRIBUTION')
print('='*60)
w_ic = df[df['outcome_tag']=='WINNER']['interest_coverage_trend_numeric'].dropna()
l_ic = df[df['outcome_tag']=='LOSER']['interest_coverage_trend_numeric'].dropna()
t3, p3 = scipy.stats.ttest_ind(w_ic, l_ic, equal_var=False)
print(f'Winner mean={w_ic.mean():.3f}, Loser mean={l_ic.mean():.3f}, p={p3:.4f}')
print('Winner IC dist (-1=deteriorating, 0=stable, 1=improving):')
print(dict(w_ic.value_counts().sort_index()))
print('Loser IC dist:')
print(dict(l_ic.value_counts().sort_index()))
print()

print('='*60)
print('AUDIT 3: DATA COMPLETENESS - HOW MUCH IS ACTUALLY MISSING?')
print('='*60)
total = len(df)
for col in ['op_lev_ratio', 'roice_pct', 'interest_coverage_trend_numeric']:
    n = df[col].notna().sum()
    w_n = df[df['outcome_tag']=='WINNER'][col].notna().sum()
    l_n = df[df['outcome_tag']=='LOSER'][col].notna().sum()
    print(f'{col}: total={n}/{total} ({n/total*100:.0f}%), winners={w_n}/44, losers={l_n}/75')
print()

print('='*60)
print('AUDIT 4: IS THIS THE SAME DATASET AS THE PREVIOUS TEST?')
print('='*60)
print('Checking if trades_ledger has same winner/loser composition...')
ledger = pd.read_csv('data/trades_ledger.csv')
print(f'Ledger rows: {len(ledger)}')
print(f'Ledger outcome_tag distribution:')
print(ledger['outcome_tag'].value_counts().to_dict())
print()
print('Scores file outcome_tag distribution:')
print(df['outcome_tag'].value_counts().to_dict())
print()

print('='*60)
print('AUDIT 5: CACHE - WHAT DATA WAS ACTUALLY FETCHED?')
print('='*60)
cache_path = 'data/fundamental_analysis_cache.json'
if os.path.exists(cache_path):
    with open(cache_path) as f:
        cache = json.load(f)
    print(f'Cache entries: {len(cache)}')
    now = time.time()
    ages = [(now - v['ts'])/3600 for v in cache.values() if 'ts' in v]
    if ages:
        print(f'Oldest entry: {max(ages):.1f}h ago')
        print(f'Newest entry: {min(ages):.1f}h ago')
    
    # Count how many returned valid op_lev_ratio vs None
    have_oplev = sum(1 for v in cache.values() if v.get('data', {}).get('op_lev_ratio') is not None)
    have_ic = sum(1 for v in cache.values() if v.get('data', {}).get('interest_coverage_trend') is not None)
    have_error = sum(1 for v in cache.values() if 'error' in v.get('data', {}))
    print(f'Have op_lev_ratio: {have_oplev}/{len(cache)}')
    print(f'Have interest_coverage_trend: {have_ic}/{len(cache)}')
    print(f'Have error: {have_error}/{len(cache)}')
    
    # Sample a few entries
    for key in list(cache.keys())[:3]:
        data = cache[key].get('data', {})
        print(f'\n{key}:')
        print(f'  op_lev_ratio={data.get("op_lev_ratio")}')
        print(f'  interest_coverage_trend={data.get("interest_coverage_trend")}')
        print(f'  roice_pct={data.get("roice_pct")}')
        print(f'  error={data.get("error", "none")}')

print()
print('='*60)
print('AUDIT 6: PREVIOUS FINDINGS - WHERE DID "GREAT INFLUENCE" COME FROM?')
print('='*60)
# Check if there's a fundamental_scores.csv or results from a PRIOR run
prior_results = 'data/fundamental_analysis_results.csv'
if os.path.exists(prior_results):
    r = pd.read_csv(prior_results)
    print('Current results CSV:')
    print(r.to_string(index=False))
