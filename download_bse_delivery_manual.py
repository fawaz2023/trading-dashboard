"""
BSE Downloader for Past 3 Days (Corrected URL Pattern)
Downloads BSE bhav and delivery for the past 3 trading days.
Delivery ZIP uses DDMM format: SCBSEALLDDMM.zip
"""

import os
import requests
from datetime import datetime, timedelta
import zipfile
import shutil

# Base URLs
BSE_BHAV_URL = "https://www.bseindia.com/download/BhavCopy/Equity/BhavCopy_BSE_CM_0_0_0_{date}_F_0000.CSV"
BSE_DELIVERY_URL = "https://www.bseindia.com/BSEDATA/gross/{year}/SCBSEALL{daymonth}.zip"

# Headers to mimic a browser (BSE blocks simple requests)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
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

def extract_zip(zip_path, extract_folder):
    """Extract ZIP file to folder."""
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_folder)
        print(f"✅ Extracted: {zip_path}")
        return True
    except Exception as e:
        print(f"❌ Extraction failed: {e}")
        return False

def download_bse_for_date(date_obj):
    """Download BSE bhav and delivery for a specific date."""
    date_str = date_obj.strftime("%Y%m%d")      # Example: 20251120
    year = date_obj.strftime("%Y")
    daymonth = date_obj.strftime("%d%m")        # CORRECT: DDMM format
    
    # File paths
    bhav_filename = f"bse_bhav_{date_str}.csv"
    delivery_zipname = f"bse_delivery_{date_str}.zip"
    bhav_path = os.path.join("data/bse_raw", bhav_filename)
    delivery_zip_path = os.path.join("data/bse_raw", delivery_zipname)
    delivery_extract_path = os.path.join("data/bse_raw", f"bse_delivery_extracted_{date_str}")
    
    # Download bhav CSV
    bhav_url = BSE_BHAV_URL.format(date=date_str)
    bhav_success = download_file(bhav_url, bhav_path)
    
    # Download delivery ZIP (using DDMM format)
    delivery_url = BSE_DELIVERY_URL.format(year=year, daymonth=daymonth)
    delivery_success = download_file(delivery_url, delivery_zip_path)
    
    # Extract delivery ZIP if downloaded
    if delivery_success:
        if extract_zip(delivery_zip_path, delivery_extract_path):
            # Find and rename the extracted CSV
            extracted_files = [f for f in os.listdir(delivery_extract_path) if f.lower().endswith('.csv')]
            if extracted_files:
                src = os.path.join(delivery_extract_path, extracted_files[0])
                dst = os.path.join("data/bse_raw", f"bse_delivery_{date_str}.csv")
                shutil.move(src, dst)
                print(f"✅ Renamed delivery to: {dst}")
                # Clean up extraction folder
                shutil.rmtree(delivery_extract_path)
            else:
                print("⚠️ No CSV found in extracted delivery ZIP")
    
    return bhav_success, delivery_success

def main():
    """Download BSE data for past 3 trading days."""
    print("="*70)
    print("BSE DOWNLOADER - PAST 3 DAYS (DDMM format)")
    print("="*70)
    
    today = datetime.now()
    dates_to_download = []
    
    # Get past 3 trading days (skip weekends)
    for i in range(1, 6):  # Check up to 5 days back to find 3 trading days
        date = today - timedelta(days=i)
        if date.weekday() < 5:  # Monday=0, Friday=4
            dates_to_download.append(date)
        if len(dates_to_download) >= 3:
            break
    
    print(f"Dates to download: {[d.strftime('%Y-%m-%d') for d in dates_to_download]}")
    print("="*70)
    
    for date_obj in dates_to_download:
        print(f"\n--- Processing {date_obj.strftime('%Y-%m-%d')} ---")
        bhav_ok, deliv_ok = download_bse_for_date(date_obj)
        
        if not bhav_ok:
            print("⚠️ Bhav download failed")
        if not deliv_ok:
            print("⚠️ Delivery download failed")
    
    print("\n" + "="*70)
    print("Download complete. Check data/bse_raw/ for files.")
    print("="*70)

if __name__ == "__main__":
    main()
