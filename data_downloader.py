import requests
import zipfile
import io
import pandas as pd
from datetime import datetime, timedelta
import time
import os
from config import Config
import logging

logging.basicConfig(filename=os.path.join(Config.LOGS_DIR, 'downloads.log'), level=logging.INFO)

class DataDownloader:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(Config.NSE_HEADERS)
    
    def download_nse_bhav(self, date=None):
        if date is None:
            date = datetime.now() - timedelta(days=1)
        
        date_str = date.strftime('%Y%m%d')
        url = Config.NSE_BHAV_URL.format(date=date_str)
        
        try:
            print(f'Downloading NSE Bhav: {date_str}')
            response = self.session.get(url, verify=False, timeout=30)
            
            if response.status_code == 200:
                with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                    csv_file = z.namelist()[0]
                    df = pd.read_csv(z.open(csv_file))
                
                output_file = os.path.join(Config.NSE_RAW_DIR, f'cm_bhav_{date_str}.csv')
                df.to_csv(output_file, index=False)
                print(f'Success: {len(df)} records')
                return df, True, None
            else:
                return None, False, f'HTTP {response.status_code}'
        except Exception as e:
            print(f'Error: {str(e)}')
            return None, False, str(e)
    
    def download_nse_delivery(self, date=None):
        if date is None:
            date = datetime.now() - timedelta(days=1)
        
        date_str = date.strftime('%d%m%Y')
        url = Config.NSE_DELIVERY_URL.format(date=date_str)
        
        try:
            print(f'Downloading NSE Delivery: {date_str}')
            response = self.session.get(url, verify=False, timeout=30)
            
            if response.status_code == 200:
                lines = response.text.split('\n')[4:]
                data = [line.split('|') for line in lines if line.strip()]
                
                if len(data) > 1:
                    df = pd.DataFrame(data[1:], columns=data[0])
                    df = df[df['Sgmt'].str.strip() == 'EQ'].copy()
                    
                    output_file = os.path.join(Config.NSE_RAW_DIR, f'delivery_{date.strftime("%Y%m%d")}.csv')
                    df.to_csv(output_file, index=False)
                    print(f'Success: {len(df)} records')
                    return df, True, None
            
            return None, False, f'HTTP {response.status_code}'
        except Exception as e:
            print(f'Error: {str(e)}')
            return None, False, str(e)
    
    def download_bse_bhav(self, date=None):
        if date is None:
            date = datetime.now() - timedelta(days=1)
        
        date_str = date.strftime('%d%m%y')
        url = Config.BSE_BHAV_URL.format(date=date_str)
        
        try:
            print(f'Downloading BSE Bhav: {date_str}')
            response = requests.get(url, timeout=30)
            
            if response.status_code == 200:
                with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                    csv_file = z.namelist()[0]
                    df = pd.read_csv(z.open(csv_file))
                
                output_file = os.path.join(Config.BSE_RAW_DIR, f'bse_bhav_{date.strftime("%Y%m%d")}.csv')
                df.to_csv(output_file, index=False)
                print(f'Success: {len(df)} records')
                return df, True, None
            else:
                return None, False, f'HTTP {response.status_code}'
        except Exception as e:
            print(f'Error: {str(e)}')
            return None, False, str(e)
    
    def download_all(self, date=None):
        if date is None:
            date = datetime.now() - timedelta(days=1)
        
        print(f'\nDownloading data for {date.strftime("%Y-%m-%d")}')
        print('='*50)
        
        results = {}
        results['nse_bhav'] = self.download_nse_bhav(date)
        results['nse_delivery'] = self.download_nse_delivery(date)
        results['bse_bhav'] = self.download_bse_bhav(date)
        
        print('='*50)
        return results

if __name__ == '__main__':
    downloader = DataDownloader()
    downloader.download_all()
