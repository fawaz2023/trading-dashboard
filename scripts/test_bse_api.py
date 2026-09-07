import requests
import json

url = 'https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w'
params = {
    'strCat': 'RPT',
    'strPrevDate': '20240101',
    'strScrip': '532955',
    'strSearch': 'P',
    'strToDate': '20260906',
    'strType': 'C'
}
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
    'Accept': 'application/json, text/plain, */*',
    'Origin': 'https://www.bseindia.com',
    'Referer': 'https://www.bseindia.com/',
    'X-Requested-With': 'XMLHttpRequest'
}

r = requests.get(url, params=params, headers=headers)
print(f'Status: {r.status_code}')
try:
    data = r.json()
    print('SUCCESS: Received JSON response.')
    print(f"Total records found: {len(data.get('Table', []))}")
    # Look for RPT
    rpt_filings = [x for x in data.get('Table', []) if x.get('SUBCATNAME') == 'Related Party' or 'Reg23' in str(x.get('ATTACHMENTNAME', ''))]
    print(f'RPT Filings found: {len(rpt_filings)}')
    if rpt_filings:
        print(f"Sample RPT attachment: {rpt_filings[0].get('ATTACHMENTNAME')}")
        print(f"PDF URL: https://www.bseindia.com/xml-data/corpfiling/AttachLive/{rpt_filings[0].get('ATTACHMENTNAME')}")
except Exception as e:
    print('FAILED to parse JSON. API returned HTML or blocked.')
    print(f'Response start: {r.text[:200]}')
