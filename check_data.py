import pandas as pd

file_path = r'c:\Users\fawaz\Desktop\trading_dashboard\data\historical_full_universe.csv'

print("Loading dataset...")
df = pd.read_csv(file_path, parse_dates=['DATE'])

print("\n--- Dataset Summary ---")
print(f"Total Rows: {len(df):,}")
print(f"Date Range: {df['DATE'].min().date()} to {df['DATE'].max().date()}")
print(f"Total Unique Symbols: {df['SYMBOL'].nunique():,}")
print("-----------------------")
