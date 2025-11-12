import pandas as pd
import os
from datetime import datetime, timedelta
from config import Config

print("=" * 70)
print("REAL PROGRESSIVE CALCULATOR - Using Historical Data")
print("=" * 70)

# Load all historical bhav files
nse_raw_dir = Config.NSE_RAW_DIR
bhav_files = sorted([f for f in os.listdir(nse_raw_dir) if f.startswith("nse_bhav_")])

if len(bhav_files) < 20:
    print(f"❌ Only {len(bhav_files)} files found. Need at least 20 days of data.")
    exit(1)

print(f"\nFound {len(bhav_files)} historical files")
print(f"Oldest: {bhav_files[0]}")
print(f"Latest: {bhav_files[-1]}")

# Load all files
all_data = []
for f in bhav_files:
    date_str = f.replace("nse_bhav_", "").replace(".csv", "")
    date = datetime.strptime(date_str, '%Y%m%d')
    
    df = pd.read_csv(os.path.join(nse_raw_dir, f))
    df["DATE"] = date
    all_data.append(df)

df_all = pd.concat(all_data, ignore_index=True)
print(f"\nTotal rows loaded: {len(df_all)}")

# Get latest date
latest_date = df_all["DATE"].max()
print(f"Latest date: {latest_date.strftime('%Y-%m-%d')}")

# Load delivery data
delivery_files = sorted([f for f in os.listdir(nse_raw_dir) if f.startswith("nse_delivery_")])
all_delivery = []
for f in delivery_files:
    date_str = f.replace("nse_delivery_", "").replace(".csv", "")
    date = datetime.strptime(date_str, '%Y%m%d')
    
    df_d = pd.read_csv(os.path.join(nse_raw_dir, f))
    if " SYMBOL" in df_d.columns:
        df_d = df_d.rename(columns={" SYMBOL": "SYMBOL", " DELIV_PER": "DELIV_PER"})
    df_d["DATE"] = date
    all_delivery.append(df_d)

if len(all_delivery) > 0:
    df_delivery_all = pd.concat(all_delivery, ignore_index=True)
    print(f"Delivery data loaded: {len(df_delivery_all)} rows")
    
    df_all = df_all.merge(
        df_delivery_all[["SYMBOL", "DATE", "DELIV_PER"]], 
        on=["SYMBOL", "DATE"], 
        how="left"
    )
    df_all["DELIV_PER"] = pd.to_numeric(df_all["DELIV_PER"], errors='coerce').fillna(50)
else:
    df_all["DELIV_PER"] = 50

# Convert to numeric
df_all["CLOSE"] = pd.to_numeric(df_all["CLOSE"], errors='coerce').fillna(0)
df_all["TOTTRDQTY"] = pd.to_numeric(df_all["TOTTRDQTY"], errors='coerce').fillna(0)
df_all["TOTTRDVAL"] = pd.to_numeric(df_all["TOTTRDVAL"], errors='coerce').fillna(0)

# Calculate metrics
df_all["DELIVERY_TURNOVER"] = df_all["TOTTRDQTY"] * df_all["CLOSE"]
df_all["ATW"] = df_all["TOTTRDVAL"] / 1000

# Filter equity only
if "SERIES" in df_all.columns:
    df_all = df_all[df_all["SERIES"] == "EQ"].copy()

# Calculate averages using TRADING DAYS
print("\nCalculating progressive averages (based on TRADING DAYS)...")

symbols = df_all["SYMBOL"].unique()
results = []
processed = 0

for symbol in symbols:
    df_stock = df_all[df_all["SYMBOL"] == symbol].sort_values("DATE")
    
    # Get latest day data
    latest = df_stock[df_stock["DATE"] == latest_date]
    if len(latest) == 0:
        continue
    
    latest = latest.iloc[0]
    
    # Get historical data (excluding latest date)
    df_hist = df_stock[df_stock["DATE"] < latest_date].sort_values("DATE", ascending=False)
    
    # Calculate averages from last N TRADING DAYS
    df_1w = df_hist.head(5)   # Last 5 trading days
    df_1m = df_hist.head(22)  # Last 22 trading days (~1 month)
    df_3m = df_hist.head(66)  # Last 66 trading days (~3 months)
    
    results.append({
        "SYMBOL": symbol,
        "CLOSE": latest["CLOSE"],
        "DELIV_PER": latest["DELIV_PER"],
        "DELIVERY_TURNOVER": latest["DELIVERY_TURNOVER"],
        "ATW": latest["ATW"],
        "DELIV_PER_1W": df_1w["DELIV_PER"].mean() if len(df_1w) > 0 else latest["DELIV_PER"],
        "DELIV_PER_1M": df_1m["DELIV_PER"].mean() if len(df_1m) > 0 else latest["DELIV_PER"],
        "DELIV_PER_3M": df_3m["DELIV_PER"].mean() if len(df_3m) > 0 else latest["DELIV_PER"],
        "DELIVERY_TURNOVER_1W": df_1w["DELIVERY_TURNOVER"].mean() if len(df_1w) > 0 else latest["DELIVERY_TURNOVER"],
        "DELIVERY_TURNOVER_1M": df_1m["DELIVERY_TURNOVER"].mean() if len(df_1m) > 0 else latest["DELIVERY_TURNOVER"],
        "DELIVERY_TURNOVER_3M": df_3m["DELIVERY_TURNOVER"].mean() if len(df_3m) > 0 else latest["DELIVERY_TURNOVER"],
        "ATW_1W": df_1w["ATW"].mean() if len(df_1w) > 0 else latest["ATW"],
        "ATW_1M": df_1m["ATW"].mean() if len(df_1m) > 0 else latest["ATW"],
        "ATW_3M": df_3m["ATW"].mean() if len(df_3m) > 0 else latest["ATW"],
    })
    
    processed += 1
    if processed % 500 == 0:
        print(f"  Processed {processed}/{len(symbols)} stocks...")

df_final = pd.DataFrame(results)

# Save
out_file = Config.COMBINED_FILE
os.makedirs(os.path.dirname(out_file), exist_ok=True)
df_final.to_csv(out_file, index=False)

print(f"\n✅ SUCCESS!")
print(f"   Stocks: {len(df_final)}")
print(f"   Saved to: {out_file}")
print("=" * 70)
print("\nSample (360ONE):")
sample = df_final[df_final["SYMBOL"] == "360ONE"]
if len(sample) > 0:
    s = sample.iloc[0]
    print(f"   Today DELIV_PER: {s['DELIV_PER']:.2f}%")
    print(f"   1W avg: {s['DELIV_PER_1W']:.2f}% (last 5 trading days)")
    print(f"   1M avg: {s['DELIV_PER_1M']:.2f}% (last 22 trading days)")
    print(f"   3M avg: {s['DELIV_PER_3M']:.2f}% (last 66 trading days)")
    if s['DELIV_PER'] > s['DELIV_PER_1W'] > s['DELIV_PER_1M'] > s['DELIV_PER_3M']:
        print("   ✅ Progressive spike detected!")
    else:
        print("   ❌ No progressive spike")
