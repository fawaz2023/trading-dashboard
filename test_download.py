from datetime import datetime, timedelta
from data_downloader_improved import DataDownloaderImproved

# Try Thursday's data (Nov 6)
date = datetime(2025, 11, 6)

downloader = DataDownloaderImproved()
print(f"Trying date: {date.strftime('%d %b %Y')}")

# Test NSE Bhav
df, success, meta = downloader.download_nse_bhav_direct(date)
if success:
    print(f"✅ NSE Bhav: {meta['rows']} rows")
    print(df.head())
else:
    print(f"❌ NSE Bhav failed: {meta}")

# Test NSE Delivery
df2, success2, meta2 = downloader.download_nse_delivery(date)
if success2:
    print(f"✅ NSE Delivery: {meta2['rows']} rows")
else:
    print(f"❌ NSE Delivery failed: {meta2}")
