import pandas as pd
import requests
from datetime import datetime, timedelta
import time
import os
import zipfile

def download_bse_delivery(date_obj):
    """
    Download BSE delivery position report for a specific date.
    BSE URL: https://www.bseindia.com/BSEDATA/gross/YYYY/SCBSEALLDDMM.zip
    """
    
    # Format: DDMM (e.g., 1010 for Oct 10)
    date_str = date_obj.strftime('%d%m')
    year = date_obj.strftime('%Y')
    
    # BSE delivery report URL
    url = f"https://www.bseindia.com/BSEDATA/gross/{year}/SCBSEALL{date_str}.zip"
    
    print(f"📥 Downloading BSE delivery for {date_obj.strftime('%Y-%m-%d')}...")
    print(f"   URL: {url}")
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            # Create directory
            os.makedirs('data/bse_delivery', exist_ok=True)
            
            # Save zip file
            zip_path = f"data/bse_delivery/SCBSEALL{date_str}.zip"
            
            with open(zip_path, 'wb') as f:
                f.write(response.content)
            
            # Extract CSV
            try:
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall('data/bse_delivery')
                
                # Find extracted CSV (usually SCBSEALL{DDMM}.TXT)
                csv_file = f"data/bse_delivery/SCBSEALL{date_str}.TXT"
                
                if os.path.exists(csv_file):
                    print(f"   ✅ Downloaded: {csv_file}")
                    return csv_file
                else:
                    # Try without extension
                    files = [f for f in os.listdir('data/bse_delivery') if date_str in f]
                    if files:
                        csv_file = f"data/bse_delivery/{files[0]}"
                        print(f"   ✅ Downloaded: {csv_file}")
                        return csv_file
                    else:
                        print(f"   ❌ CSV not found after extraction")
                        return None
            except Exception as e:
                print(f"   ❌ Error extracting: {e}")
                return None
                
        else:
            print(f"   ❌ HTTP {response.status_code}")
            return None
            
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return None

def process_bse_delivery(csv_file):
    """
    Load and process BSE delivery CSV/TXT
    BSE Format: Pipe-delimited (|) with columns:
    SC_CODE|SC_NAME|QTY_TRADED|DELIVERABLE_QTY|%_DELI_QTY_TO_TRADED
    """
    try:
        # Read BSE delivery file (pipe-delimited)
        df = pd.read_csv(csv_file, sep='|', skipinitialspace=True)
        
        # Clean column names
        df.columns = df.columns.str.strip().str.upper()
        
        # BSE uses different column names - map to standard
        column_mapping = {
            '%_DELI_QTY_TO_TRADED': 'DELIV_PER',
            'DELIVERABLE_QTY': 'DELIV_QTY',
            'QTY_TRADED': 'TOTTRDQTY'
        }
        
        df = df.rename(columns=column_mapping)
        
        # Keep only needed columns
        needed = ['SC_CODE', 'SC_NAME', 'DELIV_QTY', 'DELIV_PER', 'TOTTRDQTY']
        df = df[[col for col in needed if col in df.columns]]
        
        # Convert to numeric
        df['SC_CODE'] = pd.to_numeric(df['SC_CODE'], errors='coerce')
        df['DELIV_QTY'] = pd.to_numeric(df['DELIV_QTY'], errors='coerce')
        df['DELIV_PER'] = pd.to_numeric(df['DELIV_PER'], errors='coerce')
        df['TOTTRDQTY'] = pd.to_numeric(df['TOTTRDQTY'], errors='coerce')
        
        # Remove NaN
        df = df.dropna(subset=['SC_CODE'])
        
        # Save as processed CSV for easy access
        csv_out = csv_file.replace('.TXT', '_processed.csv')
        df.to_csv(csv_out, index=False)
        
        print(f"   📊 Processed {len(df)} BSE delivery records")
        print(f"   💾 Saved: {csv_out}")
        return df
        
    except Exception as e:
        print(f"   ❌ Error processing: {e}")
        return None

def download_last_n_days(days=67):
    """
    Download last N days of BSE delivery data
    """
    print("=" * 70)
    print("BSE DELIVERY DATA DOWNLOADER")
    print("=" * 70)
    
    os.makedirs('data/bse_delivery', exist_ok=True)
    
    success_count = 0
    failed_dates = []
    
    for i in range(days):
        date_obj = datetime.now() - timedelta(days=i)
        
        # Skip weekends
        if date_obj.weekday() >= 5:
            print(f"⏭️  Skipping {date_obj.strftime('%Y-%m-%d')} (Weekend)")
            continue
        
        csv_file = download_bse_delivery(date_obj)
        
        if csv_file:
            df = process_bse_delivery(csv_file)
            if df is not None:
                success_count += 1
        else:
            failed_dates.append(date_obj.strftime('%Y-%m-%d'))
        
        time.sleep(2)  # Be nice to BSE servers
    
    print("=" * 70)
    print(f"✅ Downloaded {success_count} days of BSE delivery data")
    if failed_dates:
        print(f"\n❌ Failed dates: {', '.join(failed_dates[:10])}")
        if len(failed_dates) > 10:
            print(f"   ... and {len(failed_dates)-10} more")
    print("=" * 70)

if __name__ == "__main__":
    # Download last 67 trading days
    download_last_n_days(67)
