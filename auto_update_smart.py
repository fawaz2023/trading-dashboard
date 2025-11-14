import os
import glob
import pandas as pd
from datetime import datetime, timedelta
from nse_downloader_fixed_nov2025 import NSEDownloaderFixed
from bse_downloader_working import BSEDownloaderWorking
from config import Config

print("=" * 70)
print("SMART AUTO-UPDATE - NSE + BSE with Real Progressives")
print("=" * 70)

holidays = [
    "2025-01-26", "2025-03-14", "2025-03-29", "2025-04-10", "2025-04-14",
    "2025-05-01", "2025-08-15", "2025-10-02", "2025-10-22",
    "2025-11-01", "2025-11-05", "2025-12-25"
]

# Check last downloaded NSE file
existing_files = []
last_download_date = None

if os.path.exists(Config.NSE_RAW_DIR):
    existing_files = sorted([f for f in os.listdir(Config.NSE_RAW_DIR) if f.startswith("nse_bhav_")])
    if existing_files:
        last_file = existing_files[-1]
        try:
            date_str = last_file.replace("nse_bhav_", "").replace(".csv", "")
            last_download_date = datetime.strptime(date_str, '%Y%m%d')
            print(f"Last downloaded: {last_download_date.strftime('%d %b %Y')}")
        except:
            last_download_date = datetime.now() - timedelta(days=7)
    else:
        last_download_date = datetime.now() - timedelta(days=7)
else:
    last_download_date = datetime.now() - timedelta(days=7)

# Calculate missing dates
start_date = last_download_date + timedelta(days=1)
end_date = datetime.now()

if start_date > end_date:
    print("\n✅ Already up to date!")
else:
    print(f"\nDownloading from {start_date.strftime('%d %b')} to {end_date.strftime('%d %b')}")
    print("=" * 70)
    
    nse_downloader = NSEDownloaderFixed()
    bse_downloader = BSEDownloaderWorking()
    downloaded = 0
    
    current_date = start_date
    while current_date <= end_date:
        if current_date.weekday() >= 5:
            current_date += timedelta(days=1)
            continue
        
        if current_date.strftime('%Y-%m-%d') in holidays:
            print(f"⏭️  {current_date.strftime('%d %b')} - Holiday")
            current_date += timedelta(days=1)
            continue
        
        print(f"[DOWNLOAD] {current_date.strftime('%d %b')} - ", end="")

        
        # Download NSE
        df_bhav, ok_bhav, _ = nse_downloader.download_nse_bhav_new_format(current_date)
        df_deliv, ok_deliv, _ = nse_downloader.download_nse_delivery(current_date)
        
        # Download BSE
        df_bse, ok_bse, _ = bse_downloader.download_bse_bhav(current_date)
        
        if ok_bhav:
            status = "✅ NSE"
            if ok_bse:
                status += " + BSE"
            print(status)
            downloaded += 1
        else:
            print("❌")
        
        current_date += timedelta(days=1)
    
    if downloaded > 0:
        print(f"\nDownloaded: {downloaded} days")

# ==== LOAD NSE DATA ====
nse_raw_dir = Config.NSE_RAW_DIR
nse_bhav_files = sorted([f for f in os.listdir(nse_raw_dir) if f.startswith("nse_bhav_")])
print(f"Loading {len(nse_bhav_files)} NSE files...")

nse_data = []
for f in nse_bhav_files:
    date_str = f.replace("nse_bhav_", "").replace(".csv", "")
    date = datetime.strptime(date_str, '%Y%m%d')
    df = pd.read_csv(os.path.join(nse_raw_dir, f))
    df["DATE"] = date
    df["EXCHANGE"] = "NSE"
    nse_data.append(df)

df_nse = pd.concat(nse_data, ignore_index=True)
print(f"NSE records: {len(df_nse)}")

# ==== LOAD BSE DATA (MERGED) ====
bse_merged_dir = 'data/bse_merged'
merged_bse_files = sorted(glob.glob(os.path.join(bse_merged_dir, 'bse_merged_*.csv')))

bse_data = []
if merged_bse_files:
    print(f"Loading {len(merged_bse_files)} merged BSE delivery files with real delivery data...")

    for f in merged_bse_files:
        try:
            df = pd.read_csv(f)
            
            # Rename BSE columns to match NSE format
            df = df.rename(columns={
                "TckrSymb": "SYMBOL",
                "BizDt": "DATE",
                "ClsPric": "CLOSE",
                "TtlTradgVol": "TOTTRDQTY",
                "TtlTrfVal": "TOTTRDVAL"
            })
            
            # ✅ CRITICAL FIX: Add EXCHANGE column
            df["EXCHANGE"] = "BSE"
            
            # Convert DATE if it exists as BizDt
            if "DATE" in df.columns:
                df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
            
            # Convert numeric columns
            for col in ["CLOSE", "TOTTRDQTY", "TOTTRDVAL", "DELIV_PER", "DELIV_QTY"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            
            bse_data.append(df)
            
        except Exception as e:
            print(f"Error loading {f}: {e}")

    df_bse = pd.concat(bse_data, ignore_index=True)
    print(f"✅ BSE EXCHANGE column added: {df_bse['EXCHANGE'].unique() if 'EXCHANGE' in df_bse.columns else 'MISSING!'}")

# ==== COMBINE NSE + BSE ====
df_all = pd.concat([df_nse, df_bse], ignore_index=True)

# CRITICAL FIX: Deduplicate by ISIN with STRICT NSE priority
if "ISIN" in df_all.columns:
    print("Deduplicating by ISIN (STRICT NSE priority)...")
    before = len(df_all)

    df_all["EXCH_PRIORITY"] = df_all["EXCHANGE"].apply(lambda x: 0 if x == "NSE" else 1)
    df_all = df_all.sort_values(["ISIN", "DATE", "EXCH_PRIORITY"])
    df_all = df_all.drop_duplicates(subset=["ISIN", "DATE"], keep="first")
    df_all = df_all.drop(columns=["EXCH_PRIORITY"])

    after = len(df_all)
    print(f"Removed {before - after} duplicates, {after} records remaining")
    print(f"NSE records retained: {len(df_all[df_all['EXCHANGE']=='NSE'])}")
    print(f"BSE records retained: {len(df_all[df_all['EXCHANGE']=='BSE'])}")
else:
    print("No ISIN column, skipping deduplication")

# Fix missing columns before usage
if "DELIVERY_TURNOVER" not in df_all.columns:
    df_all["DELIVERY_TURNOVER"] = df_all["DELIV_QTY"] * df_all["CLOSE"]


if "ATW" not in df_all.columns:
    df_all["ATW"] = df_all["TOTTRDVAL"] / 1000

latest_date = df_all["DATE"].max()

# ==== LOAD NSE DELIVERY DATA ====
print("Loading NSE delivery data...")
delivery_files = sorted([f for f in os.listdir(nse_raw_dir) if f.startswith("nse_delivery_")])
all_delivery = []

for f in delivery_files:
    date_str = f.replace("nse_delivery_", "").replace(".csv", "")
    date = datetime.strptime(date_str, '%Y%m%d')
    df_d = pd.read_csv(os.path.join(nse_raw_dir, f))

    # Rename columns to standard names
    if " SYMBOL" in df_d.columns:
        df_d = df_d.rename(columns={" SYMBOL": "SYMBOL", " DELIV_PER": "DELIV_PER"})

    # Rename delivery quantity column variants to DELIV_QTY
    if " DELIV_QTY" in df_d.columns:
        df_d.rename(columns={" DELIV_QTY": "DELIV_QTY"}, inplace=True)
    elif " DELIV" in df_d.columns:
        df_d.rename(columns={" DELIV": "DELIV_QTY"}, inplace=True)
    elif "DELIV" in df_d.columns:
        df_d.rename(columns={"DELIV": "DELIV_QTY"}, inplace=True)

    # Fill missing columns with default values
    if "DELIV_PER" not in df_d.columns:
        df_d["DELIV_PER"] = pd.NA
    if "DELIV_QTY" not in df_d.columns:
        df_d["DELIV_QTY"] = 0

    df_d["DATE"] = date
    all_delivery.append(df_d)

if len(all_delivery) > 0:
    df_delivery_all = pd.concat(all_delivery, ignore_index=True)
    # Merge delivery percent and quantity columns
    df_all = df_all.merge(
        df_delivery_all[["SYMBOL", "DATE", "DELIV_PER", "DELIV_QTY"]],
        on=["SYMBOL", "DATE"], how="left", suffixes=('', '_nse')
    )

    # Combine delivery percent columns safely
    if 'DELIV_PER' in df_all.columns and 'DELIV_PER_nse' in df_all.columns:
        df_all['DELIV_PER'] = df_all['DELIV_PER'].combine_first(df_all['DELIV_PER_nse'])
        df_all.drop(columns=['DELIV_PER_nse'], inplace=True)

    # Ensure numeric types and fill missing values
    df_all["DELIV_PER"] = pd.to_numeric(df_all["DELIV_PER"], errors='coerce').fillna(60)
    df_all["DELIV_QTY"] = pd.to_numeric(df_all["DELIV_QTY"], errors='coerce').fillna(0)
else:
    df_all["DELIV_PER"] = 60
    df_all["DELIV_QTY"] = 0

# Calculate delivery turnover based on merged deliverable quantity
df_all["DELIVERY_TURNOVER"] = df_all["DELIV_QTY"] * df_all["CLOSE"]

# ===== CALCULATE METRICS =====
df_all["CLOSE"] = pd.to_numeric(df_all["CLOSE"], errors='coerce').fillna(0)
df_all["TOTTRDQTY"] = pd.to_numeric(df_all["TOTTRDQTY"], errors='coerce').fillna(0)
df_all["TOTTRDVAL"] = pd.to_numeric(df_all["TOTTRDVAL"], errors='coerce').fillna(0)
df_all["DELIVERY_TURNOVER"] = df_all["DELIV_QTY"] * df_all["CLOSE"]

df_all["ATW"] = df_all["TOTTRDVAL"] / 1000
# ===== FILTERS =====
print(f"Before SERIES filter: Total={len(df_all)}, BSE={len(df_all[df_all['EXCHANGE']=='BSE'])}")

if "SERIES" in df_all.columns:
    # For NSE: keep only EQ series
    # For BSE: keep all (BSE doesn't use SERIES column the same way)
    df_all = df_all[
        ((df_all["EXCHANGE"] == "NSE") & (df_all["SERIES"] == "EQ")) |
        (df_all["EXCHANGE"] == "BSE")
    ].copy()

print(f"After SERIES filter: Total={len(df_all)}, BSE={len(df_all[df_all['EXCHANGE']=='BSE'])}")

# Symbol exclusion filter
df_all = df_all[~df_all["SYMBOL"].str.contains("ETF|LIQUID|FUND|INDEX|NIFTY|SENSEX|GLOBE", case=False, na=False)].copy()

print(f"After SYMBOL filter: Total={len(df_all)}, BSE={len(df_all[df_all['EXCHANGE']=='BSE'])}")
# ===== CALCULATE PROGRESSIVE AVERAGES =====
print("Calculating progressive averages...")
symbols = df_all["SYMBOL"].unique()
results = []

for symbol in symbols:
    df_stock = df_all[df_all["SYMBOL"] == symbol].sort_values("DATE")
    
    # Use each stock's own latest date instead of global latest_date
    if len(df_stock) == 0:
        continue
    
    latest = df_stock.iloc[-1]  # Most recent row for this stock
    stock_latest_date = latest["DATE"]
    
    # Get historical data (everything before the latest date for this stock)
    df_hist = df_stock[df_stock["DATE"] < stock_latest_date].sort_values("DATE", ascending=False)
    df_1w = df_hist.head(5)
    df_1m = df_hist.head(22)
    df_3m = df_hist.head(66)
    
    results.append({
        "SYMBOL": symbol,
        "EXCHANGE": latest.get("EXCHANGE", "NSE"),
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

print(f"Processed {len(results)} stocks successfully")

df_final = pd.DataFrame(results)
df_final.to_csv(Config.COMBINED_FILE, index=False)

print("\n✅ SUCCESS!")
print(f"   Total Stocks: {len(df_final)} (NSE + BSE deduplicated)")
print(f"   NSE Stocks: {len(df_final[df_final['EXCHANGE'] == 'NSE'])}")
print(f"   BSE Stocks: {len(df_final[df_final['EXCHANGE'] == 'BSE'])}")
print(f"   Latest Date: {latest_date.strftime('%d %b %Y')}")
print("=" * 70)

import sys
sys.exit(0)  # success
