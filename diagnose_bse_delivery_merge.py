import pandas as pd
import glob

print("== Diagnosing BSE Delivery Merge ==")

# Load combined dashboard live CSV
combined_file = "data/combined_dashboard_live.csv"
df = pd.read_csv(combined_file)

print(f"Total stocks in combined file: {len(df)}")

# Check stocks with zero delivery turnover
zero_delivery = df[df["DELIVERY_TURNOVER"] == 0]
zero_count = len(zero_delivery)
print(f"Stocks with DELIVERY_TURNOVER=0: {zero_count} ({(zero_count / len(df)) * 100:.2f}%)")

# Check split by exchange
nse_zero = len(zero_delivery[zero_delivery["EXCHANGE"] == "NSE"])
bse_zero = len(zero_delivery[zero_delivery["EXCHANGE"] == "BSE"])

print(f"NSE stocks with zero delivery: {nse_zero}")
print(f"BSE stocks with zero delivery: {bse_zero}")

# Show sample rows from BSE with zero delivery
sample_bse_zero = zero_delivery[zero_delivery["EXCHANGE"] == "BSE"].head(5)
print("\nSample BSE stocks with zero delivery turnover:")
print(sample_bse_zero[["SYMBOL", "DELIVERY_TURNOVER", "DELIV_PER"]])

# Check columns in latest BSE delivery file
bse_delivery_files = sorted(glob.glob("data/bse_delivery_*.csv"))
if bse_delivery_files:
    latest_bse_file = bse_delivery_files[-1]
    df_bse_del = pd.read_csv(latest_bse_file)
    print(f"\nLatest BSE delivery file: {latest_bse_file}")
    print("Columns in BSE delivery file:")
    print(df_bse_del.columns.tolist())
else:
    print("No BSE delivery files found.")

print("\nDiagnostic complete.")
