import os
import time
import requests
import zipfile
from datetime import datetime, timedelta
import pandas as pd

def download_bse_delivery(date_obj):
    date_str = date_obj.strftime('%d%m')
    year = date_obj.strftime('%Y')
    url = f"https://www.bseindia.com/BSEDATA/gross/{year}/SCBSEALL{date_str}.zip"
    
    print(f"Downloading BSE delivery for {date_obj.strftime('%Y-%m-%d')}...")
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code == 200:
            os.makedirs('data/bse_delivery', exist_ok=True)
            zip_path = f"data/bse_delivery/SCBSEALL{date_str}.zip"
            with open(zip_path, 'wb') as f:
                f.write(response.content)
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall('data/bse_delivery')
            print(f"Downloaded and extracted {zip_path}")
            return True
        else:
            print(f"File not found (HTTP {response.status_code}) for {date_str}")
            return False
    except Exception as e:
        print(f"Error downloading: {e}")
        return False

def process_bse_delivery(csv_file):
    try:
        df = pd.read_csv(csv_file, sep='|', header=0)
        df.rename(columns={
            'SCRIP CODE': 'SC_CODE',
            'DELIVERY QTY': 'DELIV_QTY',
            'DELV. PER.': 'DELIV_PER',
            "DAY'S VOLUME": 'QTY_TRADED',
            "DAY'S TURNOVER": 'TURNOVER',
            'DELIVERY VAL': 'DELIVERY_VAL',
            'DATE': 'DATE'
        }, inplace=True)

        df['SC_CODE'] = pd.to_numeric(df['SC_CODE'], errors='coerce')
        df['DELIV_QTY'] = pd.to_numeric(df['DELIV_QTY'], errors='coerce')
        df['DELIV_PER'] = pd.to_numeric(df['DELIV_PER'], errors='coerce')
        df['QTY_TRADED'] = pd.to_numeric(df['QTY_TRADED'], errors='coerce')
        df['TURNOVER'] = pd.to_numeric(df['TURNOVER'], errors='coerce')

        df.dropna(subset=['SC_CODE'], inplace=True)
        print(f"Processed {len(df)} BSE delivery records from {csv_file}")
        return df
    except Exception as e:
        print(f"Error processing delivery file {csv_file}: {e}")
        return None

def merge_bhav_and_delivery(bhav_file, delivery_df, output_file):
    bhav_df = pd.read_csv(bhav_file)
    if 'FinInstrmId' in bhav_df.columns:
        bhav_df.rename(columns={'FinInstrmId': 'SC_CODE'}, inplace=True)
    bhav_df['SC_CODE'] = bhav_df['SC_CODE'].astype(str)
    delivery_df['SC_CODE'] = delivery_df['SC_CODE'].astype(str)

    merged_df = bhav_df.merge(
        delivery_df[['SC_CODE', 'DELIV_PER', 'DELIV_QTY']],
        on='SC_CODE',
        how='left',
        suffixes=('', '_deliv')
    )

    merged_df['DELIV_PER'] = merged_df['DELIV_PER_deliv'].combine_first(merged_df.get('DELIV_PER'))
    merged_df['DELIV_QTY'] = merged_df['DELIV_QTY'].combine_first(pd.Series([None]*len(merged_df)))

    merged_df.drop(columns=[col for col in merged_df.columns if col.endswith('_deliv')], inplace=True)
    merged_df.to_csv(output_file, index=False)
    print(f"Merged dataset saved to {output_file}")

def main():
    # Download last 7 days of delivery data (adjust as needed)
    for i in range(7):
        date_obj = datetime.now() - timedelta(days=i)
        if date_obj.weekday() < 5:  # skip weekends
            download_bse_delivery(date_obj)
            time.sleep(1)

    # Use latest available extracted file for processing and merging
    delivery_dir = 'data/bse_delivery'
    files = [f for f in os.listdir(delivery_dir) if f.startswith('SCBSEALL') and f.endswith('.TXT')]
    if not files:
        print("No BSE delivery files found for processing.")
        return

    latest_file = max(files, key=lambda x: os.path.getmtime(os.path.join(delivery_dir, x)))
    delivery_df = process_bse_delivery(os.path.join(delivery_dir, latest_file))

    if delivery_df is None:
        print("Delivery processing failed. Aborting merge.")
        return

    bhav_file = 'data/bse_raw/bse_bhav_20251111.csv'  # your BSE bhav file
    output_file = 'data/bse_merged_20251111.csv'

    merge_bhav_and_delivery(bhav_file, delivery_df, output_file)

if __name__ == "__main__":
    main()
