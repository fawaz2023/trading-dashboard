import requests
import pandas as pd
import zipfile
import io
import os
from datetime import datetime
from config import Config
import warnings
warnings.filterwarnings('ignore')

class BSEDownloaderFixed:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': '*/*'
        })
    
    def download_bse_method1(self, date):
        """Method 1: Direct BSE ZIP download"""
        date_str = date.strftime('%d%m%y')
        url = f"https://www.bseindia.com/download/BhavCopy/Equity/EQ{date_str}_CSV.ZIP"
        
        try:
            r = self.session.get(url, timeout=30, verify=False)
            if r.status_code == 200:
                with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                    csv_file = [f for f in z.namelist() if '.CSV' in f.upper()][0]
                    df = pd.read_csv(z.open(csv_file))
                return df, True
        except:
            pass
        return None, False
    
    def download_bse_method2(self, date):
        """Method 2: BSE API format"""
        date_str = date.strftime('%Y%m%d')
        url = f"https://api.bseindia.com/BseIndiaAPI/api/DefaultData/GetMarketInfo?strType=AllMkt&strCategory=Equity&dtDate={date_str}"
        
        try:
            r = self.session.get(url, timeout=30, verify=False)
            if r.status_code == 200:
                data = r.json()
                if 'Table' in data:
                    df = pd.DataFrame(data['Table'])
                    return df, True
        except:
            pass
        return None, False
    
    def download_bse_method3(self, date):
        """Method 3: Old format lowercase"""
        date_str = date.strftime('%d%m%y')
        url = f"https://www.bseindia.com/download/BhavCopy/Equity/eq{date_str}_csv.zip"
        
        try:
            r = self.session.get(url, timeout=30, verify=False)
            if r.status_code == 200:
                with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                    csv_file = z.namelist()[0]
                    df = pd.read_csv(z.open(csv_file))
                return df, True
        except:
            pass
        return None, False
    
    def download_bse_bhav(self, date=None):
        """Try all methods until one works"""
        if date is None:
            date = datetime.now()
        
        methods = [
            ("Method 1 (New ZIP)", self.download_bse_method1),
            ("Method 2 (API)", self.download_bse_method2),
            ("Method 3 (Old ZIP)", self.download_bse_method3)
        ]
        
        for method_name, method_func in methods:
            print(f"  Trying {method_name}...", end=" ")
            df, success = method_func(date)
            
            if success and df is not None and len(df) > 0:
                # Save
                out_dir = Config.BSE_RAW_DIR
                os.makedirs(out_dir, exist_ok=True)
                out_file = os.path.join(out_dir, f"bse_bhav_{date.strftime('%Y%m%d')}.csv")
                df.to_csv(out_file, index=False)
                
                print(f"✅ ({len(df)} records)")
                return df, True, {"rows": len(df), "date": date.strftime('%Y-%m-%d'), "method": method_name}
            else:
                print("❌")
        
        return None, False, "All methods failed"

# Test it
if __name__ == "__main__":
    from datetime import timedelta
    
    downloader = BSEDownloaderFixed()
    
    print("=" * 70)
    print("BSE DOWNLOADER TEST - Trying Multiple Methods")
    print("=" * 70)
    
    # Try last 10 days
    for days_back in range(1, 11):
        test_date = datetime.now() - timedelta(days=days_back)
        if test_date.weekday() >= 5:
            continue
        
        print(f"\n📅 {test_date.strftime('%d %b %Y (%A)')}")
        df, success, meta = downloader.download_bse_bhav(test_date)
        
        if success:
            print(f"\n🎉 SUCCESS!")
            print(f"   Records: {meta['rows']}")
            print(f"   Method: {meta['method']}")
            print(f"   Columns: {list(df.columns)[:5]}")
            break
    else:
        print("\n❌ Could not download BSE data from any recent date")
        print("BSE may require authentication or has changed format")
