import pandas as pd
from config import Config

df = pd.read_csv(Config.COMBINED_FILE)

print(f"Total rows: {len(df)}")
print(f"\nColumns: {list(df.columns)}")
print(f"\nFirst 5 rows:")
print(df.head())

print(f"\n--- Data Quality Check ---")
print(f"Rows with CLOSE > 0: {len(df[df['CLOSE'] > 0])}")
print(f"Rows with CLOSE = 0: {len(df[df['CLOSE'] == 0])}")
print(f"Rows with DELIV_PER >= 50: {len(df[df['DELIV_PER'] >= 50])}")
print(f"Rows with DELIVERY_TURNOVER >= 5M: {len(df[df['DELIVERY_TURNOVER'] >= 5000000])}")

print(f"\n--- Sample of non-zero prices ---")
print(df[df['CLOSE'] > 0].head())
