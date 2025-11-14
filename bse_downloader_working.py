import requests
import pandas as pd
import zipfile
import io
import os
from datetime import datetime
from config import Config
import warnings
warnings.filterwarnings('ignore')

class BSEDownloaderWorking:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': '*/*'
        })
    
    def download_bse_bhav_new_format(self, date):
        date_str = date.strftime('%Y%m%d')
        url = f"https://www.bseindia.com/download/BhavCopy/Equity/BhavCopy_BSE_CM_0_0_0_{date_str}_F_0000.CSV"
    
        try:
            r = self.session.get(url, timeout=30, verify=False)
            if r.status_code == 200:
                df = pd.read_csv(io.StringIO(r.text))
                return df, True
        except Exception as e:
            # Handle exception or log error
            pass
        return None, False


    
    def download_bse_delivery_zip(self, date):
        """BSE Delivery ZIP: SCBSEALLDDMM.zip"""
        date_str = date.strftime('%d%m')
        url = f"https://www.bseindia.com/BSEDATA/gross/{date.year}/SCBSEALL{date_str}.zip"
        
        try:
            r = self.session.get(url, timeout=30, verify=False)
            if r.status_code == 200:
                with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                    # Find the CSV file in ZIP
                    csv_file = [f for f in z.namelist() if f.endswith('.csv') or f.endswith('.CSV')][0]
                    df = pd.read_csv(z.open(csv_file))
                return df, True
        except:
            pass
        return None, False
    
    def download_bse_bhav(self, date=None):
        """Download BSE Bhav Copy using working formats"""
        if date is None:
            date = datetime.now()
        
        print(f"\n📅 {date.strftime('%d %b %Y')}")
        
        # Try new format
        print(f"  Trying BSE Bhav (new format)...", end=" ")
        df_bhav, ok_bhav = self.download_bse_bhav_new_format(date)
        
        if ok_bhav and df_bhav is not None and len(df_bhav) > 0:
            print(f"✅ ({len(df_bhav)} records)")
            
            # Save bhav
            out_dir = Config.BSE_RAW_DIR
            os.makedirs(out_dir, exist_ok=True)
            out_file = os.path.join(out_dir, f"bse_bhav_{date.strftime('%Y%m%d')}.csv")
            df_bhav.to_csv(out_file, index=False)
            
            return df_bhav, True, {
                "rows": len(df_bhav), 
                "date": date.strftime('%Y-%m-%d'),
                "columns": list(df_bhav.columns)[:5]
            }
        else:
            print("❌")
            return None, False, "BSE bhav not available"

# Test it
if __name__ == "__main__":
    from datetime import timedelta
    
    downloader = BSEDownloaderWorking()
    
    print("=" * 70)
    print("BSE DOWNLOADER - Using Working URL Formats")
    print("=" * 70)
    
    # Try last 10 days
    for days_back in range(1, 11):
        test_date = datetime.now() - timedelta(days=days_back)
        if test_date.weekday() >= 5:
            continue
        
        df, success, meta = downloader.download_bse_bhav(test_date)
        
        if success:
            print(f"\n🎉 SUCCESS!")
            print(f"   Records: {meta['rows']}")
            print(f"   Sample columns: {meta['columns']}")
            break
    else:
        print("\n❌ Could not download BSE data from any recent date")
