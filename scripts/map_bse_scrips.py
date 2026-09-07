import pandas as pd
import requests
import re
import time
import os

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
ledger_path = 'data/trades_ledger.csv'

def get_bse_scrip(ticker):
    try:
        r = requests.get(f'https://www.screener.in/api/company/search/?q={ticker}', headers=headers, timeout=5)
        data = r.json()
        if not data:
            return None
        url = 'https://www.screener.in' + data[0]['url']
        r2 = requests.get(url, headers=headers, timeout=5)
        # Look for BSE: 123456
        match = re.search(r'BSE:\s*(\d{6})', r2.text)
        if match:
            return match.group(1)
        # Fallback to check if it's in the link directly e.g. /company/526161/
        match2 = re.search(r'/company/(\d{6})/', url)
        if match2:
            return match2.group(1)
    except Exception as e:
        print(f"Error for {ticker}: {e}")
    return None

df = pd.read_csv(ledger_path)
unique_tickers = df['ticker'].unique()
print(f"Resolving BSE Scrip codes for {len(unique_tickers)} tickers...")

scrip_map = {}
for i, ticker in enumerate(unique_tickers):
    scrip = get_bse_scrip(ticker)
    scrip_map[ticker] = scrip
    print(f"[{i+1}/{len(unique_tickers)}] {ticker} -> {scrip}")
    time.sleep(0.3)

df['bse_scrip'] = df['ticker'].map(scrip_map)
df.to_csv(ledger_path, index=False)
print("Done resolving scrip codes.")
