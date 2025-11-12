import os
from datetime import datetime, timedelta
from nse_downloader_fixed_nov2025 import NSEDownloaderFixed
from config import Config

print("=" * 70)
print("DOWNLOADING 3 MONTHS HISTORICAL DATA")
print("=" * 70)

# Indian market holidays 2025
holidays = [
    "2025-01-26", "2025-03-14", "2025-03-29", "2025-04-10", "2025-04-14",
    "2025-05-01", "2025-08-15", "2025-10-02", "2025-10-22",
    "2025-11-01", "2025-11-05", "2025-12-25"
]

# Check existing files
existing_files = []
if os.path.exists(Config.NSE_RAW_DIR):
    existing_files = [f for f in os.listdir(Config.NSE_RAW_DIR) if "bhav" in f and f.startswith("nse_bhav_")]
    print(f"\nCurrently have {len(existing_files)} files")
    if len(existing_files) > 0:
        print(f"Oldest: {sorted(existing_files)[0]}")
        print(f"Latest: {sorted(existing_files)[-1]}")

# Download last 100 days (covers 3 months of trading days)
start_date = datetime.now() - timedelta(days=100)
end_date = datetime.now()

print(f"\nDownloading from {start_date.strftime('%d %b %Y')} to {end_date.strftime('%d %b %Y')}")
print("This will take 5-10 minutes...\n")
print("=" * 70)

downloader = NSEDownloaderFixed()

current_date = start_date
downloaded = 0
skipped = 0
failed = 0
total_trading_days = 0

while current_date <= end_date:
    # Skip weekends
    if current_date.weekday() >= 5:
        current_date += timedelta(days=1)
        continue
    
    total_trading_days += 1
    
    # Skip holidays
    if current_date.strftime('%Y-%m-%d') in holidays:
        print(f"⏭️  {current_date.strftime('%d %b %Y')} - Holiday")
        current_date += timedelta(days=1)
        continue
    
    # Check if already exists
    filename = f"nse_bhav_{current_date.strftime('%Y%m%d')}.csv"
    if filename in existing_files:
        print(f"✓  {current_date.strftime('%d %b %Y')} - Already exists")
        skipped += 1
        current_date += timedelta(days=1)
        continue
    
    # Download
    print(f"📥 {current_date.strftime('%d %b %Y')} - Downloading...", end=" ")
    df_bhav, ok_bhav, _ = downloader.download_nse_bhav_new_format(current_date)
    df_deliv, ok_deliv, _ = downloader.download_nse_delivery(current_date)
    
    if ok_bhav:
        status = "✅"
        if ok_deliv:
            status += " (Bhav + Delivery)"
        else:
            status += " (Bhav only)"
        print(status)
        downloaded += 1
    else:
        print(f"❌ Failed (Data may not be available yet)")
        failed += 1
    
    current_date += timedelta(days=1)

print("\n" + "=" * 70)
print(f"DOWNLOAD COMPLETE")
print(f"  Total trading days checked: {total_trading_days}")
print(f"  Downloaded: {downloaded} days")
print(f"  Skipped (already exist): {skipped} days")
print(f"  Failed: {failed} days")
print(f"  Total files now: {downloaded + skipped} days")
print("=" * 70)

# Check if we have enough data
total_files = downloaded + skipped
if total_files >= 60:
    print(f"\n✅ SUCCESS! You have {total_files} days of data (3+ months)")
    print("   Now run: python calculate_real_progressives.py")
else:
    print(f"\n⚠️  WARNING: Only {total_files} days downloaded")
    print("   Need at least 60 trading days for accurate 3M averages")
    print("   NSE archives may not have older data available")
