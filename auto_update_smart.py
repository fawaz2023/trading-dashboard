import os
import sys
import io
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import glob
import pandas as pd
import requests
import zipfile
from datetime import datetime, timedelta
from nse_downloader_fixed_nov2025 import NSEDownloaderFixed
from bse_downloader_working import BSEDownloaderWorking, normalize_bse_delivery, merge_bse_bhav_delivery
from config import Config

pd.options.mode.chained_assignment = None

print("=" * 70)
print("SMART AUTO-UPDATE - NSE + BSE with Real Progressives (v4 ENHANCED)")
print("=" * 70)

holidays = [
    "2025-01-26", "2025-03-14", "2025-03-29", "2025-04-10", "2025-04-14",
    "2025-05-01", "2025-08-15", "2025-10-02", "2025-10-22",
    "2025-11-01", "2025-11-05", "2025-12-25"
]

# ================================================================
# BACKFILL MISSING DATES (v4 - NOW INCLUDES BSE DELIVERY WITH DATE FIX)
# ================================================================
def get_missing_trading_dates(days_to_check=10):
    """Check which trading dates are missing from NSE bhav OR BSE delivery"""
    today = datetime.now()
    missing_dates = []
    for i in range(days_to_check, 0, -1):
        check_date = today - timedelta(days=i)
        # Skip weekends
        if check_date.weekday() >= 5:
            continue
        # Skip holidays
        date_str_dash = check_date.strftime("%Y-%m-%d")
        if date_str_dash in holidays:
            continue
        # Check if NSE bhav OR BSE delivery is missing
        date_str = check_date.strftime("%Y%m%d")
        nse_pattern = f"data/nse_raw/nse_bhav_{date_str}.csv"
        bse_deliv_pattern = f"data/bse_delivery_{date_str}.csv"
        nse_missing = not glob.glob(nse_pattern)
        bse_deliv_missing = not glob.glob(bse_deliv_pattern)
        if nse_missing or bse_deliv_missing:
            missing_dates.append(check_date)
    return missing_dates

def backfill_missing_dates(missing_dates):
    """Download NSE + BSE data for all missing dates (v4 - includes BSE delivery)"""
    if not missing_dates:
        print("✅ No missing dates. Data is up to date.\n")
        return

    print(f"\n{'='*70}")
    print(f"⚠️  MISSING DATA DETECTED")
    print(f"{'='*70}")
    print(f"Found {len(missing_dates)} missing trading dates:")
    for date in missing_dates:
        print(f"  📅 {date.strftime('%Y-%m-%d (%A)')}")
    print(f"{'='*70}\n")
    print("📥 Starting backfill download...\n")

    for date_obj in missing_dates:
        date_str = date_obj.strftime("%Y%m%d")
        print(f"🔄 Downloading: {date_obj.strftime('%Y-%m-%d')}")
        try:
            # Download NSE Bhavcopy and Delivery
            nse_downloader = NSEDownloaderFixed()
            nse_downloader.download_nse_bhav_new_format(date_obj)
            nse_downloader.download_nse_delivery(date_obj)

            # Download BSE Bhavcopy
            bse_downloader = BSEDownloaderWorking()
            bse_downloader.download_bse_bhav(date_obj)


            # v4 FIX: Download BSE Delivery with proper DATE injection
            out_date = date_obj.strftime("%Y%m%d")
            df_bse_del, bse_deliv_ok = bse_downloader.download_bse_delivery(date_obj)
            if bse_deliv_ok and df_bse_del is not None:
                # The helper normalizes and sets DATE=YYYYMMDD
                df_bse_del.to_csv(f"data/bse_delivery_{out_date}.csv", index=False)

            print(f"  ✅ NSE + BSE{'+ Delivery' if bse_deliv_ok else ''} downloaded")
        except Exception as e:
            print(f"  ⚠️  Error: {e}")
            continue

    print()
    print(f"{'='*70}")
    print("✅ BACKFILL COMPLETE")
    print(f"{'='*70}\n")

# -------------------------------
# Helpers (defined BEFORE use)
# -------------------------------
def to_num(s):
    return pd.to_numeric(s, errors="coerce")

def safe_read_csv(path, **kw):
    try:
        return pd.read_csv(path, **kw)
    except Exception as e:
        print(f"Read error: {path} -> {e}")
        return pd.DataFrame()

def ensure_cols(df, cols_with_default):
    for c, v in cols_with_default.items():
        if c not in df.columns:
            df[c] = v
    return df

def normalize_bse_bhav(df):
    # Map BSE schema to standard names
    ren = {
        "BizDt": "DATE",
        "TckrSymb": "SYMBOL",
        "ClsPric": "CLOSE",
        "TtlTradgVol": "TOTTRDQTY",
        "TtlTrfVal": "TOTTRDVAL",
        "TtlNbOfTxsExctd": "NO_OF_TRADES",
    }
    df = df.rename(columns=ren)
    # Parse DATE
    if "DATE" in df.columns:
        df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
    # Numeric conversions
    for c in ["CLOSE","TOTTRDQTY","TOTTRDVAL","NO_OF_TRADES"]:
        if c in df.columns:
            df[c] = to_num(df[c]).fillna(0)
    df["EXCHANGE"] = "BSE"
    # Ensure critical columns exist
    df = ensure_cols(df, {"ISIN": None, "FinInstrmId": None, "SYMBOL": None})
    return df

def normalize_nse_bhav(df, date):
    df = df.copy()
    df["DATE"] = date
    df["EXCHANGE"] = "NSE"
    if "TtlNbOfTxsExctd" in df.columns:
        df["NO_OF_TRADES"] = df["TtlNbOfTxsExctd"]
    for c in ["CLOSE","TOTTRDQTY","TOTTRDVAL","NO_OF_TRADES"]:
        if c in df.columns:
            df[c] = to_num(df[c]).fillna(0)
    # Ensure expected columns
    df = ensure_cols(df, {"ISIN": None, "SYMBOL": None})
    return df


# Run backfill check
print(f"{'='*70}")
print("[SEARCH] CHECKING FOR MISSING DATES...")
print(f"{'='*70}")
missing_dates = get_missing_trading_dates(days_to_check=10)
backfill_missing_dates(missing_dates)

# -------------------------------
# Step 0: Determine download window
# -------------------------------
if os.path.exists(Config.NSE_RAW_DIR):
    existing = sorted([f for f in os.listdir(Config.NSE_RAW_DIR) if f.startswith("nse_bhav_")])
    if existing:
        fn = existing[-1]
        try:
            date_str = fn.replace("nse_bhav_", "").replace(".csv", "")
            last_download_date = datetime.strptime(date_str, "%Y%m%d")
            print(f"Last downloaded: {last_download_date.strftime('%d %b %Y')}")
        except:
            last_download_date = datetime.now() - timedelta(days=7)
    else:
        last_download_date = datetime.now() - timedelta(days=7)
else:
    last_download_date = datetime.now() - timedelta(days=7)

start_date = last_download_date + timedelta(days=1)
end_date = datetime.now()

# -------------------------------
# Step 1: Download missing days
# -------------------------------
if start_date <= end_date:
    print(f"\nDownloading from {start_date.strftime('%d %b')} to {end_date.strftime('%d %b')}")
    print("=" * 70)

    nse_downloader = NSEDownloaderFixed()
    bse_downloader = BSEDownloaderWorking()
    downloaded = 0
    cur = start_date

    while cur <= end_date:
        if cur.weekday() >= 5 or cur.strftime("%Y-%m-%d") in holidays:
            cur += timedelta(days=1)
            continue

        print(f"[DOWNLOAD] {cur.strftime('%d %b')} - ", end="")

        # NSE
        _, ok_bhav, _ = nse_downloader.download_nse_bhav_new_format(cur)
        _, ok_deliv, _ = nse_downloader.download_nse_delivery(cur)

        # BSE bhav
        _, ok_bse, _ = bse_downloader.download_bse_bhav(cur)

        # v4 FIX: BSE delivery with proper DATE injection
        out_date = cur.strftime("%Y%m%d")
        df_bse_del, bse_deliv_ok = bse_downloader.download_bse_delivery(cur)
        if bse_deliv_ok and df_bse_del is not None:
            # The helper normalizes and sets DATE=YYYYMMDD
            df_bse_del.to_csv(f"data/bse_delivery_{out_date}.csv", index=False)

        if ok_bhav or ok_deliv or ok_bse or bse_deliv_ok:
            msg = "✅ Download Status:\n"
            msg += f"    NSE Bhav: {'✅' if ok_bhav else '❌'}\n"
            msg += f"    NSE Deliv: {'✅' if ok_deliv else '❌'}\n"
            msg += f"    BSE Bhav: {'✅' if ok_bse else '❌'}\n"
            msg += f"    BSE Deliv: {'✅' if bse_deliv_ok else '❌'}"
            print(msg)
            if ok_bhav and ok_bse: # Count as success if we at least got the price files
                downloaded += 1
        else:
            print("❌ All downloads failed")

        cur += timedelta(days=1)

    if downloaded > 0:
        print(f"\n{'='*70}")
        print(f"DOWNLOAD SUMMARY")
        print(f"{'='*70}")
        print(f"Downloaded: {downloaded} days")

        # Validate all files exist
        print("\n📊 Validating downloads...")
        cur = start_date
        while cur <= end_date:
            if cur.weekday() >= 5 or cur.strftime("%Y-%m-%d") in holidays:
                cur += timedelta(days=1)
                continue
            date_str = cur.strftime("%Y%m%d")
            nse_exists = os.path.exists(f"data/nse_raw/nse_bhav_{date_str}.csv")
            bse_exists = os.path.exists(f"data/bse_raw/bse_bhav_{date_str}.csv")
            nse_deliv_exists = os.path.exists(f"data/nse_raw/nse_delivery_{date_str}.csv")
            bse_deliv_exists = os.path.exists(f"data/bse_delivery_{date_str}.csv")
            
            status = "✅" if (nse_exists and nse_deliv_exists and bse_exists and bse_deliv_exists) else "⚠️ "
            msg = f"{status} {cur.strftime('%d %b')}: "
            msg += f"NSE={'✓' if nse_exists else '✗'} | "
            msg += f"BSE={'✓' if bse_exists else '✗'} | "
            msg += f"NSE Deliv={'✓' if nse_deliv_exists else '✗'} | "
            msg += f"BSE Deliv={'✓' if bse_deliv_exists else '✗'}"
            print(msg)
            cur += timedelta(days=1)
        print(f"{'='*70}\n")
else:
    print("\n✅ Already up to date!")

# -------------------------------
# Step 2: Load NSE bhav
# -------------------------------
nse_raw_dir = Config.NSE_RAW_DIR
nse_bhav_files = sorted([f for f in os.listdir(nse_raw_dir) if f.startswith("nse_bhav_")])
print(f"\n📥 Loading {len(nse_bhav_files)} NSE bhav files...")

nse_frames = []
for fn in nse_bhav_files:
    date_str = fn.replace("nse_bhav_", "").replace(".csv", "")
    d = datetime.strptime(date_str, "%Y%m%d")
    df = safe_read_csv(os.path.join(nse_raw_dir, fn))
    if df.empty: continue
    df = normalize_nse_bhav(df, d)
    nse_frames.append(df)

df_nse = pd.concat(nse_frames, ignore_index=True) if nse_frames else pd.DataFrame()
print(f"✅ NSE records: {len(df_nse)}")

# -------------------------------
# Step 3: Load BSE bhav
# -------------------------------
print(f"📥 Loading BSE bhav data...")
bse_raw_dir = os.path.join(os.path.dirname(nse_raw_dir), "bse_raw")
bse_bhav_files = sorted(glob.glob(os.path.join(bse_raw_dir, "bse_bhav_*.csv")))

bse_frames = []
if bse_bhav_files:
    print(f"Found {len(bse_bhav_files)} BSE bhav files")
    for fp in bse_bhav_files:
        df = safe_read_csv(fp)
        if df.empty: continue
        df = normalize_bse_bhav(df)
        bse_frames.append(df)

df_bse = pd.concat(bse_frames, ignore_index=True) if bse_frames else pd.DataFrame()
print(f"✅ BSE records: {len(df_bse)}")

# -------------------------------
# Step 4: Load NSE delivery
# -------------------------------
print(f"\n📥 Loading NSE delivery data...")
nse_delivery_files = sorted([f for f in os.listdir(nse_raw_dir) if f.startswith("nse_delivery_")])

nse_del_frames = []
for fn in nse_delivery_files:
    date_str = fn.replace("nse_delivery_", "").replace(".csv", "")
    d = datetime.strptime(date_str, "%Y%m%d")
    df = safe_read_csv(os.path.join(nse_raw_dir, fn))
    if df.empty: continue
    # normalize
    if " SYMBOL" in df.columns:
        df = df.rename(columns={" SYMBOL":"SYMBOL"})
    df = normalize_bse_delivery(df)  # reuse standardizer
    df["DATE"] = d
    nse_del_frames.append(df)

df_nse_deliv = pd.concat(nse_del_frames, ignore_index=True) if nse_del_frames else pd.DataFrame()
print(f"✅ NSE delivery rows: {len(df_nse_deliv)}")

# -------------------------------
# Step 5: Load BSE delivery (v4 - DATE now pre-injected)
# -------------------------------
print(f"📥 Loading BSE delivery data...")
bse_delivery_files = sorted(glob.glob("data/bse_delivery_*.csv"))

bse_del_frames = []
for fp in bse_delivery_files:
    date_str = os.path.basename(fp).replace("bse_delivery_", "").replace(".csv","")
    d = datetime.strptime(date_str, "%Y%m%d")
    df = safe_read_csv(fp)
    if df.empty: continue
    df = normalize_bse_delivery(df)
    # v4 FIX: Do NOT overwrite DATE - it should already exist from download
    # Only inject if missing (for backward compat with old CSVs)
    if "DATE" not in df.columns or df["DATE"].isna().all():
        df["DATE"] = d
    bse_del_frames.append(df)

df_bse_deliv = pd.concat(bse_del_frames, ignore_index=True) if bse_del_frames else pd.DataFrame()
print(f"✅ BSE delivery rows: {len(df_bse_deliv)}")


# -------------------------------
# Save Exact Status JSON
# -------------------------------
import json
import pandas as pd
try:
    def get_max_date(df):
        if df is None or df.empty or 'DATE' not in df.columns:
            return 'Missing'
        max_val = df['DATE'].max()
        if pd.isna(max_val):
            return 'Missing'
        if str(type(max_val)) == "<class 'numpy.int64'>" or isinstance(max_val, (int, str)):
            dt = pd.to_datetime(str(max_val), format='%Y%m%d', errors='coerce')
            return dt.strftime('%d %b %Y') if pd.notna(dt) else 'Missing'
        else:
            return max_val.strftime('%d %b %Y')
            
    status_data = {
        'nse_bhav_date': get_max_date(df_nse),
        'nse_deliv_date': get_max_date(df_nse_deliv),
        'bse_bhav_date': get_max_date(df_bse),
        'bse_deliv_date': get_max_date(df_bse_deliv),
        'last_run': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    with open('data/data_status.json', 'w') as f:
        json.dump(status_data, f, indent=4)
except Exception as e:
    print(f'Error saving data_status.json: {e}')


# -------------------------------
# Step 6: Merge delivery into NSE
# -------------------------------
if not df_nse.empty and not df_nse_deliv.empty:
    # NSE merge via SYMBOL + DATE
    cols_keep = [c for c in ["SYMBOL","DATE","DELIV_PER","DELIV_QTY"] if c in df_nse_deliv.columns]
    df_nse = df_nse.merge(df_nse_deliv[cols_keep], on=["SYMBOL","DATE"], how="left")
else:
    df_nse = ensure_cols(df_nse, {"DELIV_PER":0, "DELIV_QTY":0})

# -------------------------------
# Step 7: Merge delivery into BSE
# -------------------------------
print("\n🔀 Merging BSE delivery data...")
df_bse = merge_bse_bhav_delivery(df_bse, df_bse_deliv)
bse_merged_count = len(df_bse[df_bse["DELIV_PER"] > 0])
print(f"✅ BSE stocks with delivery data merged: {bse_merged_count}/{len(df_bse)}")


import os
import pandas as pd
from datetime import datetime

def log_bse_delivery_stats(df_bse, run_date=None):
    if run_date is None:
        run_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total = len(df_bse)
    zero_delivery = (df_bse['DELIV_PER'] == 0).sum()
    partial_delivery = ((df_bse['DELIV_PER'] > 0) & (df_bse['DELIV_PER'] < 100)).sum()
    full_delivery = (df_bse['DELIV_PER'] == 100).sum()

    stats = {
        'run_date': run_date,
        'total_bse_rows': total,
        'zero_delivery': zero_delivery,
        'partial_delivery': partial_delivery,
        'full_delivery': full_delivery,
    }

    log_file = "data/debug_bse_row_counts.csv"
    df_stats = pd.DataFrame([stats])
    if os.path.exists(log_file):
        df_stats.to_csv(log_file, mode='a', header=False, index=False)
    else:
        df_stats.to_csv(log_file, index=False)
    print("📊 Logged BSE delivery stats:", stats)

# Immediately after your existing merge:
log_bse_delivery_stats(df_bse)

# -------------------------------
# Step 8: Combine NSE + BSE, dedupe by ISIN+DATE (NSE priority)
# -------------------------------
print("\n🔀 Combining NSE + BSE and deduplicating by ISIN+DATE (NSE priority)")

df_all = pd.concat([df_nse, df_bse], ignore_index=True, sort=False)
print(f"Total rows before dedup: {len(df_all)}")
print(f"Count by exchange before dedup:\n{df_all['EXCHANGE'].value_counts()}")

# Set DATE and latest_date
if "DATE" in df_all.columns:
    latest_date = pd.to_datetime(df_all["DATE"], errors="coerce").max()
else:
    latest_date = None

df_all["EXCH_PRIORITY"] = df_all["EXCHANGE"].apply(lambda x: 0 if x == "NSE" else 1)

has_isin = "ISIN" in df_all.columns and df_all["ISIN"].notna().sum() > 0
if has_isin:
    df_all = df_all.sort_values(["ISIN", "DATE", "EXCH_PRIORITY"])
    before_dedup = len(df_all)
    df_all = df_all.drop_duplicates(subset=["ISIN", "DATE"], keep="first")
    after_dedup = len(df_all)
    print(f"Total rows after dedup: {after_dedup}")
    print(f"Rows removed: {before_dedup - after_dedup}")
    print(f"Count by exchange after dedup:\n{df_all['EXCHANGE'].value_counts()}")
else:
    print("⚠️  ISIN missing or empty, falling back to SYMBOL deduplication")
    df_all = df_all.sort_values(["SYMBOL", "DATE", "EXCH_PRIORITY"])
    before_dedup = len(df_all)
    df_all = df_all.drop_duplicates(subset=["SYMBOL", "DATE"], keep="first")
    after_dedup = len(df_all)
    print(f"Total rows after dedup: {after_dedup}")
    print(f"Rows removed: {before_dedup - after_dedup}")
    print(f"Count by exchange after dedup:\n{df_all['EXCHANGE'].value_counts()}")

df_all.drop(columns=["EXCH_PRIORITY"], inplace=True, errors="ignore")

# -------------------------------
# Step 9: Compute metrics + filter universe
# -------------------------------
df_all = ensure_cols(df_all, {"CLOSE":0,"TOTTRDQTY":0,"TOTTRDVAL":0, "NO_OF_TRADES":0,"DELIV_QTY":0,"DELIV_PER":0})

for c in ["CLOSE","TOTTRDQTY","TOTTRDVAL","DELIV_QTY","DELIV_PER"]:
    df_all[c] = to_num(df_all[c]).fillna(0)

# Core metrics
df_all["DELIVERY_TURNOVER"] = df_all["DELIV_QTY"] * df_all["CLOSE"]
df_all["ATW"] = (df_all["TOTTRDVAL"] / df_all["NO_OF_TRADES"].replace(0, pd.NA)).fillna(0)

# Filter SERIES (keep NSE EQ & all BSE)
if "SERIES" in df_all.columns:
    before = len(df_all)
    df_all = df_all[
        ((df_all["EXCHANGE"]=="NSE") & (df_all["SERIES"]=="EQ")) |
        (df_all["EXCHANGE"]=="BSE")
    ].copy()
    after = len(df_all)
    print(f"\nFiltered by SERIES (NSE EQ + all BSE): {before} -> {after}")

# Symbol-based exclusions
if "SYMBOL" in df_all.columns:
    before = len(df_all)
    df_all = df_all[
        ~df_all["SYMBOL"].str.contains(
            "ETF|LIQUID|FUND|INDEX|NIFTY|SENSEX|GLOBE|BEES|HDFCPVTBAN|HDFCPSU|BANKPSU|MOMENTUM|LOWVOL|ESILVER|BBNPP|PSUBANK|GOLD|SILVER",
            case=False,
            na=False
        )
    ].copy()
    after = len(df_all)
    print(f"Excluded generic ETFs/FUNDS/INDEX: {before} -> {after}")

# Explicitly drop known bond / NCD type BSE instruments
bad_isins = [
    "INE148I07PY7", "INE1O3X15025", "INE296G07200", "INE296G07226",
    "INE306N08342", "INE443L08172", "INE501X07554", "INE501X08081",
    "INE549K08293", "INE612U07118", "INE733E07JR2", "INE787H07362",
    "INE836K07312", "INE939X07093",
]
if "ISIN" in df_all.columns:
    before = len(df_all)
    df_all = df_all[~df_all["ISIN"].isin(bad_isins)].copy()
    after = len(df_all)

# Exclude non-equity BSE instruments (bonds, T-bills, SGBs, G-secs)
if "SYMBOL" in df_all.columns:
    before = len(df_all)
    bond_patterns = [
        r'^GS\d',                           # Government Securities: GS15MAR34C
        r'^\d{3,4}GS\d',                    # G-Secs: 723GS39P, 824GS2027
        r'^\d{3,4}[A-Z]{2,4}\d{2,4}[A-Z]?$', # ALL bonds: 754SBI38, 781IHFCL28
        r'^SGB',                            # Sovereign Gold Bonds
        r'\d+TB$',                          # Treasury Bills
        r'SDL',                             # State Development Loans
        r'MHSDL',                           # Maharashtra SDL
        r'^\d{2,}[A-Z]+\d{2,}[A-Z]*$',      # G-Secs, SDLs, T-Bills (e.g. 75GS2034, 182T101025)
        r'^[A-Z]+\d{4,}[A-Z]*$',            # Corp bonds with full dates (e.g. ICLF160525)
        r'ZC\d{2,}',                        # Zero coupon bonds (e.g. JFCZC28)
        r'PP$',                             # Preference Shares
        r'^CS\d',                           # Convertible Securities
        r'^EELZ',                           # EELZ T2T exception
    ]
    pattern = '|'.join(bond_patterns)
    df_all = df_all[~df_all["SYMBOL"].str.contains(pattern, regex=True, na=False, case=False)].copy()
    after = len(df_all)
    print(f"Excluded bonds, T-bills, SGBs, and G-Secs: {before} -> {after}")

# -------------------------------
# Step 10: Calculate progressive averages
# -------------------------------
print("\n📈 Calculating progressive averages...")

if "DATE" not in df_all.columns:
    raise ValueError("DATE column missing in df_all before progressive averages")

df_all["DATE"] = pd.to_datetime(df_all["DATE"], errors="coerce")
symbols = df_all["SYMBOL"].dropna().unique().tolist()
results = []
processed = 0

for symbol in symbols:
    df_stock = df_all[df_all["SYMBOL"] == symbol].sort_values("DATE")
    if df_stock.empty:
        continue

    latest = df_stock.iloc[-1]
    latest_dt = latest["DATE"]
    hist = df_stock[df_stock["DATE"] < latest_dt].sort_values("DATE", ascending=False)

    df_1w = hist.head(5)
    df_1m = hist.head(22)
    df_3m = hist.head(66)

    # Check if this stock ever had 100% delivery in the processed history
    has_100_deliv = bool((df_stock["DELIV_PER"] >= 99.9).any())

    # Calculate VWAP logic
    latest_tottrdval = latest.get("TOTTRDVAL", 0)
    latest_volume = latest.get("TOTTRDQTY", 0)
    latest_vwap = (latest_tottrdval / latest_volume) if (pd.notna(latest_volume) and latest_volume > 0) else latest.get("CLOSE", 0)
    
    # Calculate 1M VWAP (using 22 days)
    if len(df_1m) > 0:
        hist_1m_tottrdval = df_1m["TOTTRDVAL"].sum()
        hist_1m_volume = df_1m["TOTTRDQTY"].sum()
        vwap_1m = (hist_1m_tottrdval / hist_1m_volume) if (pd.notna(hist_1m_volume) and hist_1m_volume > 0) else df_1m["CLOSE"].mean()
    else:
        vwap_1m = latest_vwap

    latest_atw = latest.get("ATW", 0)
    atw_1m = df_1m["ATW"].mean() if len(df_1m) > 0 else latest_atw

    results.append({
        "DATE": latest_dt,
        "SYMBOL": symbol,
        "ISIN": latest.get("ISIN", None),
        "EXCHANGE": latest.get("EXCHANGE", "NSE"),
        "CLOSE": latest.get("CLOSE", 0),
        "DELIV_PER": latest.get("DELIV_PER", 0),
        "DELIVERY_TURNOVER": latest.get("DELIVERY_TURNOVER", 0),
        "TOTAL_TURNOVER": latest_tottrdval,
        "VOLUME": latest_volume,
        "VWAP": latest_vwap,
        "VWAP_1M": vwap_1m,
        "WHALE_DENSITY": (latest_atw / latest_vwap) if (latest_vwap > 0) else 0,
        "WHALE_DENSITY_1M": (atw_1m / vwap_1m) if (vwap_1m > 0) else 0,
        "ATW": latest_atw,
        "EVER_100_DELIV": has_100_deliv,
        "DELIV_PER_1W": df_1w["DELIV_PER"].mean() if len(df_1w) > 0 else latest.get("DELIV_PER", 0),
        "DELIV_PER_1M": df_1m["DELIV_PER"].mean() if len(df_1m) > 0 else latest.get("DELIV_PER", 0),
        "DELIV_PER_3M": df_3m["DELIV_PER"].mean() if len(df_3m) > 0 else latest.get("DELIV_PER", 0),
        "DELIVERY_TURNOVER_1W": df_1w["DELIVERY_TURNOVER"].mean() if len(df_1w) > 0 else latest.get("DELIVERY_TURNOVER", 0),
        "DELIVERY_TURNOVER_1M": df_1m["DELIVERY_TURNOVER"].mean() if len(df_1m) > 0 else latest.get("DELIVERY_TURNOVER", 0),
        "DELIVERY_TURNOVER_3M": df_3m["DELIVERY_TURNOVER"].mean() if len(df_3m) > 0 else latest.get("DELIVERY_TURNOVER", 0),
        "ATW_1W": df_1w["ATW"].mean() if len(df_1w) > 0 else latest_atw,
        "ATW_1M": atw_1m,
        "ATW_3M": df_3m["ATW"].mean() if len(df_3m) > 0 else latest_atw,
    })

    processed += 1
    if processed % 500 == 0:
        print(f"Processed {processed}/{len(symbols)} stocks...")

df_final = pd.DataFrame(results)

# -------------------------------
# Step 11: Save outputs
# -------------------------------
os.makedirs(os.path.dirname(Config.COMBINED_FILE), exist_ok=True)

dashboard_file = "data/combined_dashboard_live.csv"
df_final.to_csv(dashboard_file, index=False)
df_final.to_csv(Config.COMBINED_FILE, index=False)

import shutil
shutil.copy("data/combined_dashboard_live.csv", "data/dashboard_cloud.csv")

print("\n" + "="*70)
print("✅ SUCCESS!")
print(f"  Total Stocks: {len(df_final)} (NSE + BSE deduplicated by ISIN/DATE)")
print(f"  NSE Stocks: {len(df_final[df_final['EXCHANGE']=='NSE'])}")
print(f"  BSE Stocks: {len(df_final[df_final['EXCHANGE']=='BSE'])}")
if pd.notna(latest_date):
    print(f"  Latest Date: {latest_date.strftime('%d %b %Y')}")
print(f"  Dashboard (LIVE): {dashboard_file}")
print(f"  Legacy (Backtest): {Config.COMBINED_FILE}")
print("="*70)

print("Calculating institutional metrics...")
try:
    import subprocess
    result = subprocess.run(["python", "calculate_active_signals.py"],
                             timeout=300, capture_output=True, text=True)
    if result.returncode == 0:
        print("Institutional metrics calculated successfully")
    else:
        print(f"Institutional metrics had errors (non-critical): {result.stderr[:200]}")
except Exception as e:
    print(f"Institutional metrics failed (non-critical): {e}")

import sys
sys.exit(0)
