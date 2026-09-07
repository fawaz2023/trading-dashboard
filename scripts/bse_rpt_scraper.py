import os
import time
import json
import re
import pandas as pd
import pdfplumber
import requests
from datetime import datetime, timedelta

SCORES_PATH = 'data/fundamental_scores.csv'
FUND_CACHE_PATH = 'data/fundamental_analysis_cache.json'
RPT_CACHE_PATH = 'data/rpt_filings_cache.json'
PDF_DIR = 'data/rpt_pdfs'

# ETF and Fund exclusions
EXCLUDE_TICKERS = {'GROWWLIQID', 'ABSL10BANK', 'DEFENCE', 'CPPLUS', 'GROWW', 'SMCGLOBAL'}

def init_cache():
    if os.path.exists(RPT_CACHE_PATH):
        with open(RPT_CACHE_PATH, 'r') as f:
            return json.load(f)
    return {}

def save_cache(cache):
    with open(RPT_CACHE_PATH, 'w') as f:
        json.dump(cache, f, indent=2)

def extract_rpt_value_from_pdf(pdf_path):
    total_val = 0.0
    found_table = False
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                table = page.extract_table()
                if table:
                    # Find the target column index
                    target_col = -1
                    for row_idx, row in enumerate(table[:5]):
                        for col_idx, cell in enumerate(row):
                            if cell and 'Value of transaction during the reporting period' in str(cell).replace('\n', ' '):
                                target_col = col_idx
                                break
                        if target_col != -1:
                            break
                    
                    if target_col != -1:
                        found_table = True
                        for row in table[3:]:
                            if len(row) > target_col and row[target_col] is not None:
                                val_str = str(row[target_col]).replace(',', '').strip()
                                if val_str.startswith('(') and val_str.endswith(')'):
                                    val_str = '-' + val_str[1:-1]
                                val_str = re.sub(r'[^\d\.-]', '', val_str)
                                try:
                                    if val_str and val_str not in ('-', '.'):
                                        total_val += abs(float(val_str))
                                except ValueError:
                                    pass
        if found_table:
            return total_val
    except Exception as e:
        print(f"Error parsing PDF: {e}")
    return None

def main():
    os.makedirs(PDF_DIR, exist_ok=True)
    df = pd.read_csv(SCORES_PATH)
    
    with open(FUND_CACHE_PATH, 'r') as f:
        fund_cache = json.load(f)
        
    rpt_cache = init_cache()
    
    targets = df[(df['sector_type'] != 'financial') & (~df['ticker'].isin(EXCLUDE_TICKERS)) & (df['bse_scrip'].notna())].copy()
    targets = targets.drop_duplicates(subset=['ticker'])
    
    print(f"Target pool size: {len(targets)} unique non-financial corporate tickers.")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Origin': 'https://www.bseindia.com',
        'Referer': 'https://www.bseindia.com/'
    }
    
    for idx, row in targets.iterrows():
        ticker = row['ticker']
        # Even if in cache, re-fetch if None to try the new logic
        if ticker in rpt_cache and rpt_cache[ticker] is not None:
            continue
            
        scrip = str(row['bse_scrip']).split('.')[0]
        trigger_date_str = str(row['trigger_date'])
        
        from_date_str = '20230630'
        to_date_str = '20240630'
        
        print(f"[{ticker}] Fetching Scrip {scrip} from {from_date_str} to {to_date_str} via API...")
        
        try:
            pdf_filename = None
            for pageno in range(1, 6): # Check up to 5 pages (250 announcements)
                url = f"https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w?pageno={pageno}&strCat=-1&strPrevDate={from_date_str}&strScrip={scrip}&strSearch=P&strToDate={to_date_str}&strType=C"
                r = requests.get(url, headers=headers, timeout=10)
                if r.status_code != 200:
                    print(f"[{ticker}] API HTTP {r.status_code}")
                    break
                    
                data = r.json()
                table = data.get('Table', [])
                if not table:
                    break # No more results
                
                for item in table:
                    subject = str(item.get('NEWSSUB', ''))
                    headline = str(item.get('HEADLINE', ''))
                    if 'Related Party' in subject or 'Related Party' in headline:
                        pdf_filename = item.get('ATTACHMENTNAME')
                        if pdf_filename:
                            break
                            
                if pdf_filename:
                    break
                    
            if pdf_filename:
                print(f"[{ticker}] Found RPT filing: {pdf_filename}")
                pdf_path = os.path.join(PDF_DIR, f"{ticker}_rpt.pdf")
                
                # Try AttachLive first, then AttachHis
                pdf_url_live = f"https://www.bseindia.com/xml-data/corpfiling/AttachLive/{pdf_filename}"
                pdf_url_his = f"https://www.bseindia.com/xml-data/corpfiling/AttachHis/{pdf_filename}"
                
                pdf_r = requests.get(pdf_url_live, headers=headers, timeout=10)
                if pdf_r.status_code == 404:
                    pdf_r = requests.get(pdf_url_his, headers=headers, timeout=10)
                    
                if pdf_r.status_code == 200:
                    with open(pdf_path, 'wb') as f_pdf:
                        f_pdf.write(pdf_r.content)
                    print(f"[{ticker}] Downloaded. Parsing...")
                    
                    rpt_val = extract_rpt_value_from_pdf(pdf_path)
                    if rpt_val is not None:
                        fund_data = fund_cache.get(ticker, {}).get('data', {})
                        mcap = fund_data.get('market_cap_cr')
                        
                        if mcap and mcap > 0:
                            rpt_pct = (rpt_val / mcap) * 100
                            rpt_cache[ticker] = rpt_pct
                            print(f"[{ticker}] SUCCESS: RPT = {rpt_val} Cr, MCap = {mcap} Cr -> RPT/MCap = {rpt_pct:.2f}%")
                        else:
                            print(f"[{ticker}] REJECTED: No market cap found in cache.")
                            rpt_cache[ticker] = None
                    else:
                        print(f"[{ticker}] REJECTED: Parse failed/No grand total.")
                        rpt_cache[ticker] = None
                else:
                    print(f"[{ticker}] Failed to download PDF (HTTP {pdf_r.status_code})")
                    rpt_cache[ticker] = None
            else:
                print(f"[{ticker}] No Related Party filings found in date range.")
                rpt_cache[ticker] = None
                
            save_cache(rpt_cache)
            
        except Exception as e:
            print(f"[{ticker}] Error: {e}")
            rpt_cache[ticker] = None
            save_cache(rpt_cache)
            
        time.sleep(0.5)
            
    print("Done Phase B scraping (API Mode).")

if __name__ == '__main__':
    main()
