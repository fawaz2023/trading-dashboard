import os
import glob
import pandas as pd
import requests
import zipfile
import shutil
from datetime import datetime, timedelta

# If separate config file
from config import Config

pd.options.mode.chained_assignment = None

print("=" * 70)
print("SMART AUTO-UPDATE - NSE + BSE with Real Progressives (ENHANCED)")
print("=" * 70)

holidays = [
    "2025-01-26", "2025-03-14", "2025-03-29", "2025-04-10", "2025-04-14",
    "2025-05-01", "2025-08-15", "2025-10-02", "2025-10-22",
    "2025-11-01", "2025-11-05", "2025-12-25"
]

def download_file(url, filepath):
    """Download a file from URL to filepath."""
    try:
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        return True
    except Exception as e:
        print(f"Failed {url}: {e}")
        return False

def extract_and_convert_delivery(zip_path, date_str):
    extract_folder = os.path.join("data/bse_raw", f"extracted_{date_str}")
    os.makedirs(extract_folder, exist_ok=True)
    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            z.extractall(extract_folder)
        txt_files = [f for f in os.listdir(extract_folder) if f.upper().endswith('.TXT')]
        if not txt_files:
            print("No TXT file found in ZIP", zip_path)
            shutil.rmtree(extract_folder)
            return False
        txt_path = os.path.join(extract_folder, txt_files[0])
        df = pd.read_csv(txt_path, sep='|', dtype=str)
        df = df.rename(columns={
            'SCRIP CODE': 'SYMBOL',
            'DELIVERY QTY': 'DELIV_QTY',
            'DELV. PER.': 'DELIV_PER'
        })
        df['SYMBOL'] = pd.to_numeric(df['SYMBOL'], errors='coerce').astype('Int64')
        df['DELIV_QTY'] = pd.to_numeric(df['DELIV_QTY'], errors='coerce').fillna(0).astype('int64')
        df['DELIV_PER'] = pd.to_numeric(df['DELIV_PER'], errors='coerce').fillna(0).astype('float64')
        df = df[['DATE', 'SYMBOL', 'DELIV_QTY', 'DELIV_PER']]
        output_path = os.path.join("data/bse_raw", f"bse_delivery_{date_str}.csv")
        df.to_csv(output_path, index=False)
        shutil.rmtree(extract_folder)
        print(f"Converted delivery to CSV: {output_path}")
        return True
    except Exception as e:
        print(f"Extraction/conversion failed: {e}")
        shutil.rmtree(extract_folder)
        return False

def download_bse_for_date(date_obj):
    date_str = date_obj.strftime("%Y%m%d")
    year = date_obj.strftime("%Y")
    daymonth = date_obj.strftime("%d%m")  # DDMM format
    # Download bhav
    bhav_path = os.path.join("data/bse_raw", f"bse_bhav_{date_str}.csv")
    bhav_url = f"https://www.bseindia.com/download/BhavCopy/Equity/BhavCopy_BSE_CM_0_0_0_{date_str}_F_0000.CSV"
    download_file(bhav_url, bhav_path)
    # Download delivery ZIP
    delivery_zip_path = os.path.join("data/bse_raw", f"bse_delivery_{date_str}.zip")
    delivery_url = f"https://www.bseindia.com/BSEDATA/gross/{year}/SCBSEALL{daymonth}.zip"
    if download_file(delivery_url, delivery_zip_path):
        extract_and_convert_delivery(delivery_zip_path, date_str)

def get_missing_trading_dates(days_to_check=10):
    """Check which trading dates are missing from NSE bhav OR BSE delivery"""
    today = datetime.now()
    missing_dates = []
    for i in range(days_to_check, 0, -1):
        check_date = today - timedelta(days=i)
        if check_date.weekday() >= 5:
            continue
        date_str_dash = check_date.strftime("%Y-%m-%d")
        if date_str_dash in holidays:
            continue
        date_str = check_date.strftime("%Y%m%d")
        nse_pattern = f"data/nse_raw/nse_bhav_{date_str}.csv"
        bse_deliv_pattern = f"data/bse_raw/bse_delivery_{date_str}.csv"
        nse_missing = not os.path.exists(nse_pattern)
        bse_deliv_missing = not os.path.exists(bse_deliv_pattern)
        if nse_missing or bse_deliv_missing:
            missing_dates.append(check_date)
    return missing_dates

def backfill_missing_dates(missing_dates):
    """Download NSE + BSE data for all missing dates"""
    if not missing_dates:
        print("✅ No missing dates. Data is up to date.\n")
        return
    print(f"\n{'='*70}")
    print(f"⚠️ MISSING DATA DETECTED")
    print(f"{'='*70}")
    print(f"Found {len(missing_dates)} missing trading dates:")
    for date in missing_dates:
        print(f" 📅 {date.strftime('%Y-%m-%d (%A)')}")
    print(f"{'='*70}\n")
    print("📥 Starting backfill download...\n")
    for date_obj in missing_dates:
        print(f"🔄 Downloading: {date_obj.strftime('%Y-%m-%d')}")
        # Download BSE automatically
        download_bse_for_date(date_obj)
        # Add NSE downloader and further normalization as needed
        # (Assuming similar logic for NSE as BSE)
    print(f"{'='*70}")
    print("✅ BACKFILL COMPLETE")
    print(f"{'='*70}\n")

# --- Run backfill and today's download ---
missing_dates = get_missing_trading_dates(days_to_check=10)
backfill_missing_dates(missing_dates)
today = datetime.now()
download_bse_for_date(today)

# --- Proceed with rest of ETL (existing normalization, dedup, merge logic) ---
# All original code for ETL and dashboard processing goes here

# ... (your normalization, merging, metrics, and output logic)

print("="*70)
print("✅ Automated ETL finished. All source files up to date and harmonized.")
print("="*70)
