import pandas as pd
import os
import glob
from datetime import datetime, timedelta
from config import Config

print("=" * 70)
print("REAL PROGRESSIVE CALCULATOR - NSE + BSE WITH DELIVERY DATA")
print("=" * 70)

# ========== LOAD NSE DATA ==========
print("\n📥 Loading NSE data...")
nse_raw_dir = Config.NSE_RAW_DIR
nse_bhav_files = sorted(glob.glob(os.path.join(nse_raw_dir, "nse_bhav_*.csv")))

if len(nse_bhav_files) < 20:
    print(f"❌ Only {len(nse_bhav_files)} NSE files found. Need at least 20 days.")
    exit(1)

print(f"Found {len(nse_bhav_files)} NSE bhav files")
print(f"  Oldest: {os.path.basename(nse_bhav_files[0])}")
print(f"  Latest: {os.path.basename(nse_bhav_files[-1])}")

# Load all NSE bhav files
nse_all = []
for f in nse_bhav_files:
    date_str = os.path.basename(f).replace("nse_bhav_", "").replace(".csv", "")
    date = datetime.strptime(date_str, '%Y%m%d')
    
    df = pd.read_csv(f)
    df["DATE"] = date
    df["EXCHANGE"] = "NSE"
    nse_all.append(df)

df_nse = pd.concat(nse_all, ignore_index=True)
print(f"✅ NSE total rows: {len(df_nse)}")

# ========== LOAD BSE DATA ==========
print("\n📥 Loading BSE data...")
bse_raw_dir = os.path.join(os.path.dirname(Config.NSE_RAW_DIR), "bse_raw")
bse_bhav_files = sorted(glob.glob(os.path.join(bse_raw_dir, "bse_bhav_*.csv")))

if len(bse_bhav_files) > 0:
    print(f"Found {len(bse_bhav_files)} BSE bhav files")
    
    # Load all BSE bhav files
    bse_all = []
    for f in bse_bhav_files:
        date_str = os.path.basename(f).replace("bse_bhav_", "").replace(".csv", "")
        date = datetime.strptime(date_str, '%Y%m%d')
        
        df = pd.read_csv(f)
        df["DATE"] = date
        df["EXCHANGE"] = "BSE"
        bse_all.append(df)
    
    df_bse = pd.concat(bse_all, ignore_index=True)
    print(f"✅ BSE total rows: {len(df_bse)}")
    
    # Merge NSE + BSE
    df_all = pd.concat([df_nse, df_bse], ignore_index=True)
    print(f"✅ Total merged: {len(df_all)} rows")
else:
    print("⚠️ No BSE data found")
    df_all = df_nse

# Get latest date
latest_date = df_all["DATE"].max()
print(f"\nLatest date: {latest_date.strftime('%Y-%m-%d')}")

# ========== LOAD NSE DELIVERY DATA ==========
print("\n📥 Loading NSE delivery data...")
nse_delivery_files = sorted(glob.glob(os.path.join(Config.NSE_RAW_DIR, "nse_delivery_*.csv")))

if len(nse_delivery_files) > 0:
    nse_delivery = []
    for f in nse_delivery_files:
        date_str = os.path.basename(f).replace("nse_delivery_", "").replace(".csv", "")
        date = datetime.strptime(date_str, '%Y%m%d')
        
        df_d = pd.read_csv(f)
        
        # Rename columns if needed
        if " SYMBOL" in df_d.columns:
            df_d = df_d.rename(columns={" SYMBOL": "SYMBOL"})
        if " DELIV_PER" in df_d.columns:
            df_d = df_d.rename(columns={" DELIV_PER": "DELIV_PER"})
        if " DELIV_QTY" in df_d.columns:
            df_d = df_d.rename(columns={" DELIV_QTY": "DELIV_QTY"})
        elif "DELIV" in df_d.columns and "DELIV_QTY" not in df_d.columns:
            df_d = df_d.rename(columns={"DELIV": "DELIV_QTY"})
        
        # Ensure columns exist
        if "DELIV_QTY" not in df_d.columns:
            df_d["DELIV_QTY"] = 0
        if "DELIV_PER" not in df_d.columns:
            df_d["DELIV_PER"] = 0
        
        df_d["DATE"] = date
        df_d["EXCHANGE"] = "NSE"
        nse_delivery.append(df_d)
    
    df_nse_delivery = pd.concat(nse_delivery, ignore_index=True)
    print(f"✅ NSE delivery: {len(df_nse_delivery)} rows")
else:
    df_nse_delivery = pd.DataFrame()

# ========== LOAD BSE DELIVERY DATA ==========
print("📥 Loading BSE delivery data...")
bse_delivery_files = sorted(glob.glob(os.path.join("data", "bse_delivery_*.csv")))

if len(bse_delivery_files) > 0:
    bse_delivery = []
    for f in bse_delivery_files:
        date_str = os.path.basename(f).replace("bse_delivery_", "").replace(".csv", "")
        date = datetime.strptime(date_str, '%Y%m%d')
        
        df_d = pd.read_csv(f)
        
        # Rename columns if needed
        if "SCRIP CODE" in df_d.columns or "SCRIP_CODE" in df_d.columns:
            df_d = df_d.rename(columns={"SCRIP CODE": "SYMBOL", "SCRIP_CODE": "SYMBOL"})
        if "DELIV. PER." in df_d.columns:
            df_d = df_d.rename(columns={"DELIV. PER.": "DELIV_PER"})
        if "DELIVERY QTY" in df_d.columns:
            df_d = df_d.rename(columns={"DELIVERY QTY": "DELIV_QTY"})
        
        # Ensure columns exist
        if "DELIV_QTY" not in df_d.columns:
            df_d["DELIV_QTY"] = 0
        if "DELIV_PER" not in df_d.columns:
            df_d["DELIV_PER"] = 0
        
        df_d["DATE"] = date
        df_d["EXCHANGE"] = "BSE"
        bse_delivery.append(df_d)
    
    df_bse_delivery = pd.concat(bse_delivery, ignore_index=True)
    print(f"✅ BSE delivery: {len(df_bse_delivery)} rows")
    
    # Merge NSE + BSE delivery
    df_all_delivery = pd.concat([df_nse_delivery, df_bse_delivery], ignore_index=True)
else:
    df_all_delivery = df_nse_delivery
    print(f"⚠️ BSE delivery files not found")

# ========== MERGE DELIVERY DATA ==========
if len(df_all_delivery) > 0:
    # Convert to numeric
    df_all_delivery["DELIV_QTY"] = pd.to_numeric(df_all_delivery["DELIV_QTY"], errors='coerce').fillna(0)
    df_all_delivery["DELIV_PER"] = pd.to_numeric(df_all_delivery["DELIV_PER"], errors='coerce').fillna(0)
    
    # Merge
    df_all = df_all.merge(
        df_all_delivery[["SYMBOL", "DATE", "DELIV_PER", "DELIV_QTY"]],
        on=["SYMBOL", "DATE"],
        how="left"
    )
    
    print(f"\n✅ Delivery data merged")
    print(f"Total symbols with DELIV_QTY > 0: {(df_all['DELIV_QTY'] > 0).sum()}")
else:
    df_all["DELIV_PER"] = 0
    df_all["DELIV_QTY"] = 0

# ========== CONVERT & CALCULATE ==========
print("\n📊 Converting and calculating metrics...")

# Convert to numeric
df_all["CLOSE"] = pd.to_numeric(df_all["CLOSE"], errors='coerce').fillna(0)
df_all["TOTTRDQTY"] = pd.to_numeric(df_all["TOTTRDQTY"], errors='coerce').fillna(0)
df_all["TOTTRDVAL"] = pd.to_numeric(df_all["TOTTRDVAL"], errors='coerce').fillna(0)
df_all["DELIV_QTY"] = pd.to_numeric(df_all["DELIV_QTY"], errors='coerce').fillna(0)
df_all["DELIV_PER"] = pd.to_numeric(df_all["DELIV_PER"], errors='coerce').fillna(0)

# Calculate metrics
df_all["DELIVERY_TURNOVER"] = df_all["DELIV_QTY"] * df_all["CLOSE"]
df_all["ATW"] = df_all["TOTTRDVAL"] / 1000

# Filter equity only
if "SERIES" in df_all.columns:
    df_all = df_all[df_all["SERIES"] == "EQ"].copy()

# ========== CALCULATE PROGRESSIVE AVERAGES ==========
print("Calculating progressive averages (based on TRADING DAYS)...")

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
    df_1m = df_hist.head(22)  # Last 22 trading days
    df_3m = df_hist.head(66)  # Last 66 trading days
    
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

# ========== SAVE OUTPUT ==========
out_file = Config.COMBINED_FILE
os.makedirs(os.path.dirname(out_file), exist_ok=True)
df_final.to_csv(out_file, index=False)

print(f"\n✅ SUCCESS!")
print(f"   Total stocks: {len(df_final)}")
print(f"   Saved to: {out_file}")
print("=" * 70)

# Show sample
sample = df_final[df_final["SYMBOL"] == "360ONE"]
if len(sample) > 0:
    s = sample.iloc[0]
    print(f"\nSample (360ONE):")
    print(f"   Today DELIV_PER: {s['DELIV_PER']:.2f}%")
    print(f"   1W avg: {s['DELIV_PER_1W']:.2f}%")
    print(f"   1M avg: {s['DELIV_PER_1M']:.2f}%")
    print(f"   3M avg: {s['DELIV_PER_3M']:.2f}%")
    if s['DELIV_PER'] > s['DELIV_PER_1W'] > s['DELIV_PER_1M'] > s['DELIV_PER_3M']:
        print("   ✅ Progressive spike detected!")
    else:
        print("   ❌ No progressive spike")
