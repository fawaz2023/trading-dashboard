import os
import glob
import pandas as pd
import requests
import zipfile
from datetime import datetime
import time

os.makedirs('data', exist_ok=True)

print("=" * 70)
print("🚀 BSE DELIVERY DOWNLOADER - CORRECT FORMAT")
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

# Create session
session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
})

downloaded = 0
failed = 0
skipped = 0

for idx, date_str in enumerate(missing_dates):
    date_obj = datetime.strptime(date_str, '%Y%m%d')
    # ✅ CORRECT FORMAT: DDMM (just day and month, 4 digits)
    bse_date_str = date_obj.strftime('%d%m')
    year = date_obj.year
    
    # URL format: SCBSEALL0408.zip (August 4), SCBSEALL1310.zip (October 13)
    url = f"https://www.bseindia.com/BSEDATA/gross/{year}/SCBSEALL{bse_date_str}.zip"
    
    print(f"[{idx+1}/{len(missing_dates)}] 📥 {date_str} ({bse_date_str})...", end=" ", flush=True)
    
    try:
        response = session.get(url, timeout=20, verify=False)
        
        if response.status_code == 404:
            print("⏭️ 404")
            skipped += 1
            continue
        elif response.status_code == 200:
            zip_path = f"data/temp_SCBSEALL{bse_date_str}.zip"
            with open(zip_path, 'wb') as f:
                f.write(response.content)
            
            try:
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall('data/')
            except:
                print("⚠️ ZIP Error")
                os.remove(zip_path)
                failed += 1
                time.sleep(1)
                continue
            
            txt_files = glob.glob(f"data/SCBSEALL{bse_date_str}.TXT") + glob.glob(f"data/SCBSEALL{bse_date_str}.txt")
            
            if not txt_files:
                print("⚠️ TXT not found")
                os.remove(zip_path)
                failed += 1
                time.sleep(1)
                continue
            
            txt_path = txt_files[0]
            
            try:
                df = pd.read_csv(txt_path, delimiter='|', dtype=str)
                
                rename_map = {
                    'SCRIP CODE': 'SYMBOL',
                    'DELIVERY QTY': 'DELIV_QTY',
                    'DELV. PER.': 'DELIV_PER',
                }
                
                rename_map = {k: v for k, v in rename_map.items() if k in df.columns}
                df.rename(columns=rename_map, inplace=True)
                
                for col in ['DELIV_QTY', 'DELIV_PER']:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                
                df['DATE'] = date_str
                
                cols_to_keep = ['DATE', 'SYMBOL', 'DELIV_QTY', 'DELIV_PER']
                df = df[[c for c in cols_to_keep if c in df.columns]]
                
                csv_path = f"data/bse_delivery_{date_str}.csv"
                df.to_csv(csv_path, index=False)
                
                print(f"✅ ({len(df)} rows)")
                downloaded += 1
                
            except Exception as e:
                print(f"❌ Parse: {str(e)[:20]}")
                failed += 1
            
            try:
                os.remove(zip_path)
                os.remove(txt_path)
            except:
                pass
        else:
            print(f"⚠️ HTTP {response.status_code}")
            failed += 1
        
        time.sleep(0.5)
    
    except Exception as e:
        print(f"❌ {str(e)[:20]}")
        failed += 1

print("\n" + "=" * 70)
print(f"✅ Downloaded: {downloaded}")
print(f"⏭️ Skipped (404): {skipped}")
print(f"❌ Failed: {failed}")
print("=" * 70)

bse_after = len(glob.glob('data/bse_delivery_*.csv'))
print(f"\n✅ Total BSE delivery files now: {bse_after}")
