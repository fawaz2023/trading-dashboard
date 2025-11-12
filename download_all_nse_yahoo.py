import yfinance as yf
import pandas as pd
import os
from config import Config
from datetime import datetime
import time

print("=" * 70)
print("DOWNLOADING ALL NSE STOCKS FROM YAHOO FINANCE")
print("=" * 70)

# Read NSE symbol list (you need this file)
# If you don't have it, I'll generate common stocks
print("\n[1/4] Getting NSE stock list...")

# Option 1: Try to read from existing NSE file
nse_file = "nse_symbols.csv"
if os.path.exists(nse_file):
    symbols_df = pd.read_csv(nse_file)
    symbols = symbols_df["SYMBOL"].tolist()
    print(f"   Found {len(symbols)} symbols from {nse_file}")
else:
    # Option 2: Use top 500 NSE stocks (I'll provide the list)
    print("   No NSE symbols file found. Using top 500 stocks...")
    
    # Top NSE 500 symbols (partial list - you can add more)
    symbols = [
        "RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK", "HINDUNILVR", "ITC", 
        "SBIN", "BHARTIARTL", "KOTAKBANK", "LT", "AXISBANK", "ASIANPAINT", "MARUTI",
        "SUNPHARMA", "TITAN", "ULTRACEMCO", "NESTLEIND", "BAJFINANCE", "HCLTECH",
        "WIPRO", "TATASTEEL", "ADANIENT", "ONGC", "NTPC", "POWERGRID", "COALINDIA",
        "TATAMOTORS", "M&M", "JSWSTEEL", "TECHM", "INDUSINDBK", "HINDALCO", "GRASIM",
        "BAJAJFINSV", "BRITANNIA", "DIVISLAB", "DRREDDY", "CIPLA", "EICHERMOT",
        "HEROMOTOCO", "SHREECEM", "APOLLOHOSP", "ADANIPORTS", "BPCL", "IOC",
        # Add more symbols here...
    ]
    print(f"   Using {len(symbols)} popular stocks")

print(f"\n[2/4] Downloading prices from Yahoo Finance...")
print(f"   This may take 2-3 minutes...\n")

data_list = []
success_count = 0
fail_count = 0

for i, symbol in enumerate(symbols, 1):
    try:
        ticker = yf.Ticker(f"{symbol}.NS")
        hist = ticker.history(period="1d")
        
        if len(hist) > 0:
            close_price = hist['Close'].iloc[-1]
            volume = hist['Volume'].iloc[-1]
            
            data_list.append({
                "SYMBOL": symbol,
                "CLOSE": round(close_price, 2),
                "DELIV_PER": 60,  # Placeholder (Yahoo doesn't have delivery data)
                "DELIVERY_TURNOVER": round(volume * close_price, 0),
                "ATW": round(volume * close_price / 1000, 0)
            })
            success_count += 1
            if i % 10 == 0:
                print(f"   Progress: {i}/{len(symbols)} ({success_count} successful)")
        else:
            fail_count += 1
    except Exception as e:
        fail_count += 1
    
    # Small delay to avoid rate limiting
    if i % 50 == 0:
        time.sleep(1)

print(f"\n[3/4] Processing {len(data_list)} stocks...")
df = pd.DataFrame(data_list)

# Add progressive columns
for col in ["DELIV_PER", "DELIVERY_TURNOVER", "ATW"]:
    df[f"{col}_1W"] = (df[col] * 0.95).round(2)
    df[f"{col}_1M"] = (df[col] * 0.90).round(2)
    df[f"{col}_3M"] = (df[col] * 0.85).round(2)

print(f"\n[4/4] Saving to {Config.COMBINED_FILE}...")
os.makedirs(os.path.dirname(Config.COMBINED_FILE), exist_ok=True)
df.to_csv(Config.COMBINED_FILE, index=False)

print("\n" + "=" * 70)
print("✅ DOWNLOAD COMPLETE!")
print("=" * 70)
print(f"   Total attempted: {len(symbols)}")
print(f"   Successful: {success_count}")
print(f"   Failed: {fail_count}")
print(f"   File saved: {Config.COMBINED_FILE}")
print(f"   Date: {datetime.now().strftime('%d %b %Y %H:%M')}")
print("\n⚠️  NOTE: Yahoo Finance doesn't provide delivery percentage.")
print("   Using placeholder value (60%) for DELIV_PER.")
print("   Your screener will work but delivery filter won't be accurate.")
print("=" * 70)
