import os
import glob
import pandas as pd
import requests
import zipfile
from datetime import datetime
import time
import urllib.parse

os.makedirs('data', exist_ok=True)

print("=" * 70)
print("🚀 BSE DELIVERY DOWNLOADER - ADVANCED (Multiple Strategies)")
print("=" * 70)

# Get NSE dates
nse_files = sorted(glob.glob('data/nse_raw/nse_bhav_*.csv'))
nse_dates = set()

for f in nse_files:
    try:
        date_str = f.split('nse_bhav_')[-1].replace('.csv', '')
        nse_dates.add(date_str)
    except:
        pass

existing_bse = set()
bse_files = glob.glob('data/bse_delivery_*.csv')
for f in bse_files:
    try:
        date_str = f.split('bse_delivery_')[-1].replace('.csv', '')
        existing_bse.add(date_str)
    except:
        pass

missing_dates = sorted(nse_dates - existing_bse)

print(f"\n✅ NSE dates found: {len(nse_dates)}")
if nse_dates:
    sorted_nse = sorted(nse_dates)
    print(f"   Oldest: {sorted_nse[0]}, Latest: {sorted_nse[-1]}")

print(f"✅ BSE delivery files existing: {len(existing_bse)}")
print(f"❌ Missing BSE delivery dates: {len(missing_dates)}")

if not missing_dates:
    print("✅ All BSE delivery files already downloaded!")
    exit()

# Create session with headers to avoid bot detection
session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/zip, */*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive',
    'Cache-Control': 'no-cache',
})

downloaded = 0
failed = 0
skipped = 0

for idx, date_str in enumerate(missing_dates):
    date_obj = datetime.strptime(date_str, '%Y%m%d')
    bse_date_str = date_obj.strftime('%d%m%Y')  # DDMMYYYY
    year = date_obj.year

    url = f"https://www.bseindia.com/BSEDATA/gross/{year}/SCBSEALL{bse_date_str}.zip"

    print(f"\n[{idx+1}/{len(missing_dates)}] 📥 {date_str} ({bse_date_str})...", end=" ", flush=True)

    try:
        # Strategy 1: D
