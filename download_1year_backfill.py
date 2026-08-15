import os
import time
import sys
import io
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

from datetime import datetime, timedelta
from nse_downloader_fixed_nov2025 import NSEDownloaderFixed
from bse_downloader_working import BSEDownloaderWorking
from config import Config

print("=" * 70)
print("1-YEAR HISTORICAL BACKFILL (NSE + BSE FALLBACK)")
print("=" * 70)

# Known Indian market holidays (2025-2026)
holidays = [
    "2025-01-26", "2025-03-14", "2025-03-29", "2025-04-10", "2025-04-14",
    "2025-05-01", "2025-08-15", "2025-10-02", "2025-10-22",
    "2025-11-01", "2025-11-05", "2025-12-25",
    "2026-01-26", "2026-03-03", "2026-03-27", "2026-04-14", "2026-05-01",
    "2026-08-15"
]

# Check existing files
nse_files = set(f for f in os.listdir(Config.NSE_RAW_DIR) if f.startswith("nse_bhav_")) if os.path.exists(Config.NSE_RAW_DIR) else set()
bse_files = set(f for f in os.listdir(Config.BSE_RAW_DIR) if f.startswith("bse_bhav_")) if os.path.exists(Config.BSE_RAW_DIR) else set()

start_date = datetime(2025, 8, 1)
end_date = datetime.now()

print(f"\nDownloading missing data from {start_date.strftime('%d %b %Y')} to {end_date.strftime('%d %b %Y')}")
print("This will take up to 20-30 minutes if many days are missing...\n")

nse_downloader = NSEDownloaderFixed()
bse_downloader = BSEDownloaderWorking()

current_date = start_date
downloaded_nse = 0
downloaded_bse = 0
skipped = 0
failed = 0

while current_date <= end_date:
    # Skip weekends
    if current_date.weekday() >= 5:
        current_date += timedelta(days=1)
        continue
    
    date_str_iso = current_date.strftime('%Y-%m-%d')
    date_str = current_date.strftime('%Y%m%d')
    
    # Skip holidays
    if date_str_iso in holidays:
        current_date += timedelta(days=1)
        continue
    
    nse_filename = f"nse_bhav_{date_str}.csv"
    bse_filename = f"bse_bhav_{date_str}.csv"
    
    if nse_filename in nse_files or bse_filename in bse_files:
        skipped += 1
        current_date += timedelta(days=1)
        continue
        
    print(f"📥 {date_str_iso} - Downloading...", end=" ")
    
    # 1. Try NSE First
    df_bhav, ok_bhav, _ = nse_downloader.download_nse_bhav_new_format(current_date)
    df_deliv, ok_deliv, _ = nse_downloader.download_nse_delivery(current_date)
    
    if ok_bhav and ok_deliv:
        print("✅ NSE Success")
        downloaded_nse += 1
        time.sleep(2)  # Rate limiting
    else:
        # NSE failed (likely too old), fallback to BSE
        print("❌ NSE Failed, trying BSE...", end=" ")
        df_bse, ok_bse, _ = bse_downloader.download_bse_bhav(current_date)
        df_bse_del, ok_bse_del = bse_downloader.download_bse_delivery(current_date)
        
        if ok_bse and ok_bse_del:
            print("✅ BSE Success")
            downloaded_bse += 1
            time.sleep(2)
        else:
            print("❌ BSE Failed (No data)")
            failed += 1
            
    current_date += timedelta(days=1)

print("\n" + "=" * 70)
print(f"DOWNLOAD COMPLETE")
print(f"  Downloaded NSE: {downloaded_nse} days")
print(f"  Downloaded BSE: {downloaded_bse} days")
print(f"  Skipped (already exist): {skipped} days")
print(f"  Failed (no data on either): {failed} days")
print("=" * 70)
