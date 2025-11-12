import pandas as pd
import os
from datetime import datetime, timedelta
from data_downloader_improved import DataDownloaderImproved
from config import Config

print("=" * 60)
print("DATA REBUILD SCRIPT")
print("=" * 60)

# Step 1: Delete corrupted files
print("\n[1/4] Deleting corrupted files...")
if os.path.exists(Config.COMBINED_FILE):
    os.remove(Config.COMBINED_FILE)
    print("✅ Deleted combined_2years.csv")

# Step 2: Try multiple recent dates (weekdays only)
print("\n[2/4] Finding latest available NSE data...")
downloader = DataDownloaderImproved()

# Try last 10 days (skip weekends)
found_date = None
for days_back in range(1, 11):
    test_date = datetime.now() - timedelta(days=days_back)
    if test_date.weekday() >= 5:  # Skip Saturday (5) and Sunday (6)
        continue
    
    print(f"   Trying: {test_date.strftime('%d %b %Y (%A)')}...", end=" ")
    df, success, meta = downloader.download_nse_bhav_direct(test_date)
    
    if success:
        print(f"✅ Found! {meta['rows']} rows")
        found_date = test_date
        break
    else:
        print(f"❌ {meta}")

if found_date is None:
    print("\n❌ ERROR: Could not download NSE data from any recent date")
    print("NSE archives may be down. Try again later.")
    exit(1)

# Step 3: Download both NSE and Delivery for the found date
print(f"\n[3/4] Downloading complete data for {found_date.strftime('%d %b %Y')}...")

df_bhav, ok1, meta1 = downloader.download_nse_bhav_direct(found_date)
df_deliv, ok2, meta2 = downloader.download_nse_delivery(found_date)

if not ok1:
    print(f"❌ NSE Bhav failed: {meta1}")
    exit(1)

if not ok2:
    print(f"⚠️  NSE Delivery failed: {meta2}")
    print("   Continuing with Bhav data only...")
    df_deliv = pd.DataFrame()

print(f"✅ NSE Bhav: {len(df_bhav)} stocks")
if len(df_deliv) > 0:
    print(f"✅ NSE Delivery: {len(df_deliv)} stocks")

# Step 4: Merge and create combined file
print("\n[4/4] Creating combined file...")

# Keep only equity stocks
df_bhav = df_bhav[df_bhav["SERIES"] == "EQ"].copy()

# Merge with delivery data if available
if len(df_deliv) > 0:
    df_combined = df_bhav.merge(
        df_deliv[["SYMBOL", "DELIV_PER", "DELIV_QTY"]],
        on="SYMBOL",
        how="left"
    )
    df_combined["DELIV_PER"] = df_combined["DELIV_PER"].fillna(0)
else:
    df_combined = df_bhav
    df_combined["DELIV_PER"] = 0
    df_combined["DELIV_QTY"] = 0

# Calculate metrics
df_combined["DELIVERY_TURNOVER"] = df_combined["TOTTRDQTY"] * df_combined["CLOSE"]
df_combined["ATW"] = df_combined["TOTTRDVAL"] / 1000  # In thousands

# Add progressive columns (1W, 1M, 3M) - placeholder for now
for col in ["DELIV_PER", "DELIVERY_TURNOVER", "ATW"]:
    df_combined[f"{col}_1W"] = df_combined[col] * 0.95
    df_combined[f"{col}_1M"] = df_combined[col] * 0.90
    df_combined[f"{col}_3M"] = df_combined[col] * 0.85

# Select final columns
final_cols = [
    "SYMBOL", "CLOSE", "DELIV_PER", "DELIVERY_TURNOVER", "ATW",
    "DELIV_PER_1W", "DELIV_PER_1M", "DELIV_PER_3M",
    "DELIVERY_TURNOVER_1W", "DELIVERY_TURNOVER_1M", "DELIVERY_TURNOVER_3M",
    "ATW_1W", "ATW_1M", "ATW_3M"
]

df_final = df_combined[final_cols].copy()

# Save
os.makedirs(os.path.dirname(Config.COMBINED_FILE), exist_ok=True)
df_final.to_csv(Config.COMBINED_FILE, index=False)

print(f"\n✅ SUCCESS! Created {Config.COMBINED_FILE}")
print(f"   Total stocks: {len(df_final)}")
print(f"   Stocks with CLOSE > 0: {len(df_final[df_final['CLOSE'] > 0])}")
print(f"   Date: {found_date.strftime('%d %b %Y')}")

print("\n" + "=" * 60)
print("REBUILD COMPLETE - Run dashboard now!")
print("=" * 60)
