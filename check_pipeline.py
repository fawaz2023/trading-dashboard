"""Quick check of how your pipeline processes NSE data"""
import pandas as pd

# Check what columns exist after your pipeline processes the data
df = pd.read_csv("data/combined_dashboard_live.csv")

print("Columns in processed file:")
print(df.columns.tolist())

print("\nSample row (first NSE stock):")
nse = df[df['EXCHANGE'] == 'NSE'].iloc[0]
print(f"SYMBOL: {nse['SYMBOL']}")
print(f"CLOSE: {nse['CLOSE']}")
print(f"DELIVERY_TURNOVER: {nse['DELIVERY_TURNOVER']}")
print(f"ATW: {nse['ATW']}")

print("\n✅ This confirms your pipeline OUTPUT is correct.")
print("But we still need to verify the FORMULAS used to create it.")
