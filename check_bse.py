import pandas as pd
from config import Config

# Load the final combined file
df = pd.read_csv(Config.COMBINED_FILE)

print("=" * 70)
print("BSE DIAGNOSTIC CHECK")
print("=" * 70)

print(f"\nTotal stocks in combined file: {len(df)}")
print(f"NSE stocks: {len(df[df['EXCHANGE']=='NSE'])}")
print(f"BSE stocks: {len(df[df['EXCHANGE']=='BSE'])}")

print("\n" + "=" * 70)
print("EXCHANGE column unique values:")
print(df['EXCHANGE'].unique())

print("\n" + "=" * 70)
print("EXCHANGE value counts:")
print(df['EXCHANGE'].value_counts())

# Check if there are ANY stocks that might be BSE with different labeling
print("\n" + "=" * 70)
print("Sample of first 10 stocks:")
print(df[['SYMBOL', 'EXCHANGE']].head(10))

# Check if merged BSE file has EXCHANGE column set correctly
print("\n" + "=" * 70)
print("Checking merged BSE file directly...")
import glob
bse_merged = glob.glob('data/bse_merged/*.csv')
if bse_merged:
    df_bse = pd.read_csv(bse_merged[0])
    print(f"BSE merged file columns: {df_bse.columns.tolist()}")
    if 'EXCHANGE' in df_bse.columns:
        print(f"BSE EXCHANGE values: {df_bse['EXCHANGE'].unique()}")
    else:
        print("⚠️ WARNING: No EXCHANGE column in BSE merged file!")
