import os
import pandas as pd
import glob
from config import Config
from bse_downloader_working import merge_bse_bhav_delivery

print("Merging historical BSE Delivery with BSE Bhav...")

deliv_files = glob.glob('data/bse_delivery_temp/bse_deliv_*.csv')
print(f"Found {len(deliv_files)} delivery files.")

for d_file in deliv_files:
    # Extract date
    date_str = d_file.split('_')[-1].replace('.csv', '')
    
    bhav_file = os.path.join(Config.BSE_RAW_DIR, f'bse_bhav_{date_str}.csv')
    if not os.path.exists(bhav_file):
        continue
        
    df_deliv = pd.read_csv(d_file)
    df_bhav = pd.read_csv(bhav_file)
    
    if 'DATE' not in df_bhav.columns:
        if 'TradDt' in df_bhav.columns:
            df_bhav['DATE'] = df_bhav['TradDt']
        elif 'BizDt' in df_bhav.columns:
            df_bhav['DATE'] = df_bhav['BizDt']
        else:
            df_bhav['DATE'] = pd.to_datetime(date_str, format='%Y%m%d')
            
    df_merged = merge_bse_bhav_delivery(df_bhav, df_deliv)
    
    # Save back
    df_merged.to_csv(bhav_file, index=False)
    print(f"Merged and updated: {bhav_file}")

print("Done.")
