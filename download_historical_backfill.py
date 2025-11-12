import os
from datetime import datetime, timedelta
from nse_downloader_fixed_nov2025 import NSEDownloaderFixed
from config import Config

print("=" * 70)
print("HISTORICAL DATA BACKFILL")
print("=" * 70)

# Check existing files
existing_files = []
if os.path.exists(Config.NSE_RAW_DIR):
    existing_files = [f for f in os.listdir(Config.NSE_RAW_DIR) if "bhav" in f]
    print(f"\nFound {len(existing_files)} existing files")
    for f in sorted(existing_files)[-5:]:
        print(f"  - {f}")

# Define date range (last 30 trading days)
start_date = datetime.now() - timedelta(days=45)
end_date = datetime.now()

downloader = NSEDownloaderFixed()

# Indian market holidays 2025 (add more as needed)
holidays = [
    "2025-01-26",  # Republic Day
    "2025-03-14",  # Holi
    "2025-03-29",  # Good Friday
    "2025-04-10",  # Ramzan
    "2025-04-14",  # Ambedkar Jayanti
    "2025-05-01",  # May Day
    "2025-08-15",  # Independence Day
    "2025-10-02",  # Gandhi Jayanti
    "2025-11-01",  # Diwali
    "2025-12-25",  # Christmas
]

print(f"\nDownloading data from {start_date.strftime('%d %b %Y')} to {end_date.strftime('%d %b %Y')}")
print("=" * 70)

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
    filename = f"nse_bhav_{current_date.strftime('%Y%m%d')}.csv"
    if filename in existing_files:
        print(f"✓  {current_date.strftime('%d %b %Y')} - Already exists")
        skipped += 1
        current_date += timedelta(days=1)
        continue
    
    # Try to download
    print(f"📥 {current_date.strftime('%d %b %Y')} - Downloading...", end=" ")
    df_bhav, ok_bhav, _ = downloader.download_nse_bhav_new_format(current_date)
    df_deliv, ok_deliv, _ = downloader.download_nse_delivery(current_date)
    
    if ok_bhav:
        print(f"✅ Success (Bhav: {ok_bhav}, Deliv: {ok_deliv})")
        downloaded += 1
    else:
        print(f"❌ Failed")
        failed += 1
    
    current_date += timedelta(days=1)

print("\n" + "=" * 70)
print(f"BACKFILL COMPLETE")
print(f"  Downloaded: {downloaded}")
print(f"  Skipped: {skipped}")
print(f"  Failed: {failed}")
print("=" * 70)
