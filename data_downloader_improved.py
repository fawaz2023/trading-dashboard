import requests
import pandas as pd
import zipfile
import io
import os
from datetime import datetime, timedelta
from config import Config
import warnings
warnings.filterwarnings('ignore')

class DataDownloaderImproved:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def download_nse_bhav_direct(self, date=None):
        if date is None:
            date = datetime.now() - timedelta(days=1)
        
        url = f"https://nsearchives.nseindia.com/content/historical/EQUITIES/{date.year}/{date.strftime('%b').upper()}/cm{date.strftime('%d%b%Y').upper()}bhav.csv.zip"
        
        try:
            r = self.session.get(url, timeout=30, verify=False)
            if r.status_code == 200:
                with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                    csv_file = z.namelist()[0]
                    df = pd.read_csv(z.open(csv_file))
                
                out = os.path.join(Config.NSE_RAW_DIR, f"nse_bhav_{date.strftime('%Y%m%d')}.csv")
                os.makedirs(Config.NSE_RAW_DIR, exist_ok=True)
                df.to_csv(out, index=False)
                return df, True, {"rows": len(df), "cols": list(df.columns)}
            else:
                return None, False, f"HTTP {r.status_code}"
        except Exception as e:
            return None, False, str(e)
    
    def download_nse_delivery(self, date=None):
        if date is None:
            date = datetime.now() - timedelta(days=1)
        
        url = f"https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{date.strftime('%d%m%Y')}.csv"
        
        try:
            r = self.session.get(url, timeout=30, verify=False)
            if r.status_code == 200:
                df = pd.read_csv(io.StringIO(r.text))
                out = os.path.join(Config.NSE_RAW_DIR, f"nse_delivery_{date.strftime('%Y%m%d')}.csv")
                os.makedirs(Config.NSE_RAW_DIR, exist_ok=True)
                df.to_csv(out, index=False)
                return df, True, {"rows": len(df)}
            else:
                return None, False, f"HTTP {r.status_code}"
        except Exception as e:
            return None, False, str(e)
    
    def download_bse_bhav(self, date=None):
        if date is None:
            date = datetime.now() - timedelta(days=1)
        
        urls_to_try = [
            f"https://www.bseindia.com/download/BhavCopy/Equity/EQ_ISINCODE_{date.strftime('%d%m%y')}.zip",
            f"https://www.bseindia.com/download/BhavCopy/Equity/SCBSEALL_{date.strftime('%d%m%y')}.zip",
            f"https://www.bseindia.com/download/BhavCopy/Equity/EQ{date.strftime('%d%m%y')}_CSV.ZIP"
        ]
        
        for url in urls_to_try:
            try:
                r = self.session.get(url, timeout=30, verify=False)
                if r.status_code == 200:
                    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                        csv_file = [f for f in z.namelist() if f.endswith('.CSV') or f.endswith('.csv')][0]
                        df = pd.read_csv(z.open(csv_file))
                    
                    out = os.path.join(Config.BSE_RAW_DIR, f"bse_bhav_{date.strftime('%Y%m%d')}.csv")
                    os.makedirs(Config.BSE_RAW_DIR, exist_ok=True)
                    df.to_csv(out, index=False)
                    return df, True, {"rows": len(df), "cols": list(df.columns), "url": url}
            except Exception as e:
                continue
        
        return None, False, "All BSE URLs failed"
    
    def download_bse_from_nse_symbols(self, date=None):
        if date is None:
            date = datetime.now() - timedelta(days=1)
        
        try:
            nse_df, success, _ = self.download_nse_bhav_direct(date)
            if not success:
                return None, False, "NSE download failed"
            
            bse_stocks = nse_df[nse_df["SERIES"] == "EQ"].copy()
            
            out = os.path.join(Config.BSE_RAW_DIR, f"bse_proxy_{date.strftime('%Y%m%d')}.csv")
            os.makedirs(Config.BSE_RAW_DIR, exist_ok=True)
            bse_stocks.to_csv(out, index=False)
            
            return bse_stocks, True, {"rows": len(bse_stocks), "source": "NSE proxy for BSE"}
        except Exception as e:
            return None, False, str(e)
    
    def download_all(self, date=None):
        res = {}
        
        df, ok, meta = self.download_nse_bhav_direct(date)
        res["nse_bhav"] = (ok, meta)
        
        df2, ok2, meta2 = self.download_nse_delivery(date)
        res["nse_delivery"] = (ok2, meta2)
        
        df3, ok3, meta3 = self.download_bse_bhav(date)
        if not ok3:
            df3, ok3, meta3 = self.download_bse_from_nse_symbols(date)
        res["bse"] = (ok3, meta3)
        
        return res
