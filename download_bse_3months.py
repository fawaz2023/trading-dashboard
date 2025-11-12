import os
from datetime import datetime, timedelta
from bse_downloader_working import BSEDownloaderWorking
from config import Config

print("=" * 70)
print("BSE 3-MONTH HISTORICAL BACKFILL")
print("=" * 70)

# Holidays 2025
holidays = [
    "2025-01-26", "2025-03-14", "2025-03-29", "2025-04-10", "2025-04-14",
    "2025-05-01", "2025-08-15", "2025-10-02", "2025-10-22",
    "2025-11-01", "2025-11-05", "2025-12-25"
]

# Check existing BSE files
existing_files = []
if os.path.exists(Config.BSE_RAW_DIR):
    existing_files = [f for f in os.listdir(Config.BSE_RAW_DIR) if f.startswith("bse_bhav_")]
    print(f"\nCurrently have {len(existing_files)} BSE files")
    if len(existing_files) > 0:
        print(f"Oldest: {sorted(existing_files)[0]}")
        print(f"Latest: {sorted(existing_files)[-1]}")

# Download from Aug 4 to Nov 11 (matching NSE dates)
start_date = datetime(2025, 8, 4)
end_date = datetime.now()

print(f"\nDownloading BSE from {start_date.strftime('%d %b %Y')} to {end_date.strftime('%d %b %Y')}")
print("=" * 70)

downloader = BSEDownloaderWorking()

current_date = start_date
downloaded = 0
skipped = 0
failed = 0

while current_date <= end_date:
    # Skip weekends
    if current_date.weekday() >= 5:
        current_date += timedelta(days=1)
        continue
    
    # Skip holidays
    if current_date.strftime('%Y-%m-%d') in holidays:
        print(f"⏭️  {current_date.strftime('%d %b %Y')} - Holiday")
        current_date += timedelta(days=1)
        continue
    
    # Check if already exists
    filename = f"bse_bhav_{current_date.strftime('%Y%m%d')}.csv"
    if filename in existing_files:
        print(f"✓  {current_date.strftime('%d %b %Y')} - Already exists")
        skipped += 1
        current_date += timedelta(days=1)
        continue
    
    # Download
    print(f"📥 {current_date.strftime('%d %b %Y')} - ", end="")
    df, success, meta = downloader.download_bse_bhav(current_date)
    
    if success:
        downloaded += 1
    else:
        failed += 1
    
    current_date += timedelta(days=1)

print("\n" + "=" * 70)
print(f"BSE BACKFILL COMPLETE")
print(f"  Downloaded: {downloaded} days")
print(f"  Skipped (already exist): {skipped} days")
print(f"  Failed: {failed} days")
print(f"  Total BSE files now: {downloaded + skipped} days")
print("=" * 70)

if (downloaded + skipped) >= 60:
    print(f"\n✅ SUCCESS! You have {downloaded + skipped} days of BSE data")
else:
    print(f"\n⚠️  Only {downloaded + skipped} days. BSE may have gaps in older archives")
