"""
BSE Downloader for Past 3 Days - TXT Format Handler
Downloads BSE bhav (CSV) and delivery (TXT inside ZIP), converts TXT to CSV.
"""

import os
import requests
from datetime import datetime, timedelta
import zipfile
import shutil
import pandas as pd

# Base URLs
BSE_BHAV_URL = "https://www.bseindia.com/download/BhavCopy/Equity/BhavCopy_BSE_CM_0_0_0_{date}_F_0000.CSV"
BSE_DELIVERY_URL = "https://www.bseindia.com/BSEDATA/gross/{year}/SCBSEALL{daymonth}.zip"

# Headers
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

def download_file(url, filepath):
    """Download a file from URL to filepath."""
    try:
        print(f"Downloading: {url}")
        response = requests.get(url, headers=HEADERS, stream=True, timeout=30)
        response.raise_for_status()
        
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"✅ Saved: {filepath}")
        return True
    except Exception as e:
        print(f"❌ Failed: {e}")
        return False

def extract_and_convert_delivery(zip_path, date_str):
    """
    Extract TXT from ZIP and convert to normalized CSV.
    BSE delivery TXT format: pipe-delimited with columns:
    DATE|SCRIP CODE|DELIVERY QTY|DELIVERY VAL|DAY'S VOLUME|DAY'S TURNOVER|DELV. PER.
    """
    try:
        extract_folder = os.path.join("data/bse_raw", f"extracted_{date_str}")
        os.makedirs(extract_folder, exist_ok=True)
        
        with zipfile.ZipFile(zip_path, 'r') as z:
            z.extractall(extract_folder)
        print(f"✅ Extracted: {zip_path}")
        
        # Find TXT file
        txt_files = [f for f in os.listdir(extract_folder) if f.upper().endswith('.TXT')]
        if not txt_files:
            print("⚠️ No TXT file found in ZIP")
            shutil.rmtree(extract_folder)
            return False
        
        txt_path = os.path.join(extract_folder, txt_files[0])
        
        # Read pipe-delimited TXT
        df = pd.read_csv(txt_path, sep='|', dtype=str)
        
        # Normalize column names
        df = df.rename(columns={
            'SCRIP CODE': 'SYMBOL',
            'DELIVERY QTY': 'DELIV_QTY',
            'DELV. PER.': 'DELIV_PER'
        })
        
        # Convert numeric columns (remove leading zeros)
        df['SYMBOL'] = pd.to_numeric(df['SYMBOL'], errors='coerce').astype('Int64')
        df['DELIV_QTY'] = pd.to_numeric(df['DELIV_QTY'], errors='coerce').fillna(0).astype('int64')
        df['DELIV_PER'] = pd.to_numeric(df['DELIV_PER'], errors='coerce').fillna(0).astype('float64')
        
        # Keep only needed columns
        df = df[['DATE', 'SYMBOL', 'DELIV_QTY', 'DELIV_PER']]
        
        # Save as CSV
        output_path = os.path.join("data/bse_raw", f"bse_delivery_{date_str}.csv")
        df.to_csv(output_path, index=False)
        print(f"✅ Converted to CSV: {output_path} ({len(df)} rows)")
        
        # Clean up
        shutil.rmtree(extract_folder)
        return True
        
    except Exception as e:
        print(f"❌ Extraction/conversion failed: {e}")
        if os.path.exists(extract_folder):
            shutil.rmtree(extract_folder)
        return False

def download_bse_for_date(date_obj):
    """Download BSE bhav and delivery for a specific date."""
    date_str = date_obj.strftime("%Y%m%d")
    year = date_obj.strftime("%Y")
    daymonth = date_obj.strftime("%d%m")  # DDMM format
    
    # File paths
    bhav_path = os.path.join("data/bse_raw", f"bse_bhav_{date_str}.csv")
    delivery_zip_path = os.path.join("data/bse_raw", f"bse_delivery_{date_str}.zip")
    
    # Download bhav
    bhav_url = BSE_BHAV_URL.format(date=date_str)
    bhav_success = download_file(bhav_url, bhav_path)
    
    # Download delivery ZIP (DDMM format)
    delivery_url = BSE_DELIVERY_URL.format(year=year, daymonth=daymonth)
    delivery_success = download_file(delivery_url, delivery_zip_path)
    
    # Extract and convert delivery TXT to CSV
    if delivery_success:
        delivery_success = extract_and_convert_delivery(delivery_zip_path, date_str)
    
    return bhav_success, delivery_success

def main():
    print("="*70)
    print("BSE DOWNLOADER - PAST 3 TRADING DAYS (TXT Format)")
    print("="*70)
    
    today = datetime.now()
    dates = []
    for i in range(1, 6):
        d = today - timedelta(days=i)
        if d.weekday() < 5:
            dates.append(d)
        if len(dates) >= 3:
            break
    
    print(f"Dates: {[d.strftime('%Y-%m-%d') for d in dates]}")
    print("="*70)
    
    for d in dates:
        print(f"\n--- {d.strftime('%Y-%m-%d')} ---")
        bhav_ok, deliv_ok = download_bse_for_date(d)
        if not bhav_ok:
            print("⚠️ Bhav failed")
        if not deliv_ok:
            print("⚠️ Delivery failed")
    
    print("\n" + "="*70)
    print("Done. Check data/bse_raw/ for CSV files.")
    print("="*70)

if __name__ == "__main__":
    main()
