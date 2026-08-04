import pandas as pd
from progressive_screener import ProgressiveSpiker

df = pd.read_csv('data/historical_full_universe.csv', low_memory=False)
df['DATE'] = pd.to_datetime(df['DATE'], errors='coerce')

# Filter for last 3 months
cutoff = df['DATE'].max() - pd.Timedelta(days=90)
df = df[df['DATE'] >= cutoff]

all_sigs = []
for dt in sorted(df['DATE'].dropna().unique()):
    day_df = df[df['DATE'] == dt].copy()
    sigs = ProgressiveSpiker(day_df).get_signals()
    if not sigs.empty:
        all_sigs.append(sigs)

if all_sigs:
    final = pd.concat(all_sigs, ignore_index=True)
    print(f"Total signals in last 3 months: {len(final)}")
    print(final[['DATE', 'SYMBOL', 'EXCHANGE', 'CLOSE']].tail(10))
else:
    print("0 signals in last 3 months.")
