import os
import glob
import pandas as pd
import requests
import zipfile
from datetime import datetime, timedelta
from nse_downloader_fixed_nov2025 import NSEDownloaderFixed
from bse_downloader_working import BSEDownloaderWorking
from config import Config

pd.options.mode.chained_assignment = None

print("=" * 70)
print("SMART AUTO-UPDATE - NSE + BSE with Real Progressives (ENHANCED)")
print("=" * 70)

holidays = [
    "2025-01-26", "2025-03-14", "2025-03-29", "2025-04-10", "2025-04-14",
    "2025-05-01", "2025-08-15", "2025-10-02", "2025-10-22",
    "2025-11-01", "2025-11-05", "2025-12-25"
]

# ================================================================
# BACKFILL MISSING DATES (NEW)
# ================================================================

def get_missing_trading_dates(days_to_check=10):
    """Check which trading dates are missing from data/nse_raw/"""
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
        
        # Check if NSE bhav file exists
        date_str = check_date.strftime("%Y%m%d")
        pattern = f"data/nse_raw/nse_bhav_{date_str}.csv"
        
        if not glob.glob(pattern):
            missing_dates.append(check_date)
    
    return missing_dates

def backfill_missing_dates(missing_dates):
    """Download NSE + BSE data for all missing dates"""
    if not missing_dates:
        print("✅ No missing dates. Data is up to date.\n")
        return
    
    print(f"\n{'='*70}")
    print(f"⚠️  MISSING DATA DETECTED")
    print(f"{'='*70}")
    print(f"Found {len(missing_dates)} missing trading dates:")
    for date in missing_dates:
        print(f"   📅 {date.strftime('%Y-%m-%d (%A)')}")
    print(f"{'='*70}\n")
    
    print("📥 Starting backfill download...\n")
    
    for date_obj in missing_dates:
        date_str = date_obj.strftime("%Y%m%d")
        print(f"🔄 Downloading: {date_obj.strftime('%Y-%m-%d')}")
        
        try:
            # Download NSE
            nse_downloader = NSEDownloaderFixed()
            nse_downloader.download_data_for_date(date_obj)
            
            # Download BSE
            bse_downloader = BSEDownloaderWorking()
            bse_downloader.download_bhav_for_date(date_obj)
            
            print(f"   ✅ NSE + BSE downloaded")
            
        except Exception as e:
            print(f"   ⚠️  Error: {e}")
            continue
        
        print()
    
    print(f"{'='*70}")
    print("✅ BACKFILL COMPLETE")
    print(f"{'='*70}\n")

# Run backfill check
print(f"{'='*70}")
print("🔍 CHECKING FOR MISSING DATES...")
print(f"{'='*70}")

missing_dates = get_missing_trading_dates(days_to_check=10)
backfill_missing_dates(missing_dates)

print(f"{'='*70}")
print("▶️  PROCEEDING WITH MAIN UPDATE")
print(f"{'='*70}\n")


# -------------------------------
# Helpers
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
    }
    df = df.rename(columns=ren)
    # Keep ISIN and FinInstrmId for robust joins
    # Parse DATE
    if "DATE" in df.columns:
        df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
    # Numeric conversions
    for c in ["CLOSE","TOTTRDQTY","TOTTRDVAL"]:
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
    for c in ["CLOSE","TOTTRDQTY","TOTTRDVAL"]:
        if c in df.columns:
            df[c] = to_num(df[c]).fillna(0)
    # Ensure expected columns
    df = ensure_cols(df, {"ISIN": None, "SYMBOL": None})
    return df

def detect_bse_delivery_key(df_deliv):
    if "ISIN" in df_deliv.columns:
        return "ISIN"
    for c in ["SCRIP CODE","SECURITY_CODE","SCRIP_CODE"]:
        if c in df_deliv.columns:
            df_deliv.rename(columns={c:"FinInstrmId"}, inplace=True)
            return "FinInstrmId"
    if "SYMBOL" in df_deliv.columns:
        return "SYMBOL"
    return None

def normalize_bse_delivery(df_deliv):
    # standardize column labels
    ren = {
        "DELIV. PER.": "DELIV_PER",
        "DELIVERY QTY": "DELIV_QTY",
        "DELIV ": "DELIV_QTY",
        " DELIV_QTY": "DELIV_QTY",
        " DELIV_PER": "DELIV_PER",
    }
    df_deliv.rename(columns=ren, inplace=True)
    df_deliv = ensure_cols(df_deliv, {"DELIV_PER": 0, "DELIV_QTY": 0})
    df_deliv["DELIV_QTY"] = to_num(df_deliv["DELIV_QTY"]).fillna(0)
    df_deliv["DELIV_PER"] = to_num(df_deliv["DELIV_PER"]).fillna(0)
    return df_deliv

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

        # BSE delivery zip
        bse_deliv_ok = False
        try:
            y = cur.year
            ddmm = cur.strftime("%d%m")
            url = f"https://www.bseindia.com/BSEDATA/gross/{y}/SCBSEALL{ddmm}.zip"
            r = requests.get(url, timeout=20, verify=False)
            if r.status_code == 200:
                zip_path = f"data/temp_bse_del_{ddmm}.zip"
                with open(zip_path, "wb") as f:
                    f.write(r.content)
                with zipfile.ZipFile(zip_path, "r") as z:
                    z.extractall("data/")
                txts = glob.glob(f"data/SCBSEALL{ddmm}.TXT")
                if txts:
                    out_date = cur.strftime("%Y%m%d")
                    df_bse_del = safe_read_csv(txts[0], delimiter="|", dtype=str)
                    df_bse_del.to_csv(f"data/bse_delivery_{out_date}.csv", index=False)
                    bse_deliv_ok = True
                # cleanup
                if os.path.exists(zip_path): os.remove(zip_path)
                for t in txts:
                    if os.path.exists(t): os.remove(t)
        except Exception as e:
            pass

        if ok_bhav:
            msg = "✅ NSE"
            if ok_bse: msg += " + BSE"
            if bse_deliv_ok: msg += " (deliv)"
            print(msg)
            downloaded += 1
        else:
            print("❌")
        cur += timedelta(days=1)

    if downloaded > 0:
        print(f"\nDownloaded: {downloaded} days")
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
    df = normalize_bse_delivery(df)  # reuse standardizer for DELIV columns
    df["DATE"] = d
    nse_del_frames.append(df)
df_nse_deliv = pd.concat(nse_del_frames, ignore_index=True) if nse_del_frames else pd.DataFrame()
print(f"✅ NSE delivery rows: {len(df_nse_deliv)}")

# -------------------------------
# Step 5: Load BSE delivery (all days)
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
    df["DATE"] = d
    bse_del_frames.append(df)
df_bse_deliv = pd.concat(bse_del_frames, ignore_index=True) if bse_del_frames else pd.DataFrame()
print(f"✅ BSE delivery rows: {len(df_bse_deliv)}")

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
# Step 7: Merge delivery into BSE (auto-detect join key)
# -------------------------------
def merge_bse_day(bhav_all, deliv_all):
    """Merge BSE delivery into BSE bhav.

    bhav_all: normalized BSE bhav (has FinInstrmId, ISIN, SYMBOL, DATE, CLOSE, etc.)
    deliv_all: BSE delivery (has DATE, SYMBOL (BSE code), DELIV_QTY, DELIV_PER)
    """
    if bhav_all.empty:
        return bhav_all
    if deliv_all.empty:
        return ensure_cols(bhav_all, {"DELIV_PER": 0, "DELIV_QTY": 0})

    out = bhav_all.copy()
    deliv_all = deliv_all.copy()

    # Standardize delivery column names
    ren = {
        "DELIV. PER.": "DELIV_PER",
        "DELIVERY QTY": "DELIV_QTY",
        "DELIV ": "DELIV_QTY",
        " DELIV_QTY": "DELIV_QTY",
        " DELIV_PER": "DELIV_PER",
    }
    deliv_all.rename(columns=ren, inplace=True)
    deliv_all = ensure_cols(deliv_all, {"DELIV_PER": 0, "DELIV_QTY": 0})

    deliv_all["DELIV_QTY"] = pd.to_numeric(deliv_all["DELIV_QTY"], errors="coerce").fillna(0)
    deliv_all["DELIV_PER"] = pd.to_numeric(deliv_all["DELIV_PER"], errors="coerce").fillna(0)

    # Normalize DATE on both sides
    if "DATE" in out.columns:
        out["DATE"] = pd.to_datetime(out["DATE"], errors="coerce")
    if "DATE" in deliv_all.columns:
        deliv_all["DATE"] = pd.to_datetime(deliv_all["DATE"], errors="coerce")

    # IMPORTANT:
    # In bhav, numeric BSE code is in FinInstrmId
    # In delivery, numeric code is in SYMBOL
    # So: rename delivery.SYMBOL -> FinInstrmId and join on that + DATE
    if "FinInstrmId" in out.columns and "SYMBOL" in deliv_all.columns:
        deliv_all = deliv_all.rename(columns={"SYMBOL": "FinInstrmId"})

    # Coerce join key to string to avoid dtype mismatches
    key = "FinInstrmId"
    if key not in out.columns or key not in deliv_all.columns:
        # Fallback: no matching key, return with zero delivery
        return ensure_cols(out, {"DELIV_PER": 0, "DELIV_QTY": 0})

    out[key] = out[key].astype(str)
    deliv_all[key] = deliv_all[key].astype(str)

    cols_keep = [c for c in [key, "DATE", "DELIV_PER", "DELIV_QTY"] if c in deliv_all.columns]

    out = out.merge(
        deliv_all[cols_keep],
        on=[key, "DATE"],
        how="left",
        suffixes=("", "_del")
    )

    # Consolidate delivery columns
    if "DELIV_PER_del" in out.columns:
        out["DELIV_PER"] = out["DELIV_PER"].fillna(out["DELIV_PER_del"])
        out.drop(columns=["DELIV_PER_del"], inplace=True)
    if "DELIV_QTY_del" in out.columns:
        out["DELIV_QTY"] = out["DELIV_QTY"].fillna(out["DELIV_QTY_del"])
        out.drop(columns=["DELIV_QTY_del"], inplace=True)

    out = ensure_cols(out, {"DELIV_PER": 0, "DELIV_QTY": 0})
    out["DELIV_PER"] = pd.to_numeric(out["DELIV_PER"], errors="coerce").fillna(0)
    out["DELIV_QTY"] = pd.to_numeric(out["DELIV_QTY"], errors="coerce").fillna(0)

    return out
# Call the merge function before combining NSE and BSE data
print("\n🔀 Merging BSE delivery data...")
df_bse = merge_bse_day(df_bse, df_bse_deliv)

bse_merged_count = len(df_bse[df_bse["DELIV_PER"] > 0])
print(f"✅ BSE stocks with delivery data merged: {bse_merged_count}/{len(df_bse)}")

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
    print("⚠️ ISIN missing or empty, falling back to SYMBOL deduplication")
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
df_all = ensure_cols(df_all, {"CLOSE":0,"TOTTRDQTY":0,"TOTTRDVAL":0,"DELIV_QTY":0,"DELIV_PER":0})
for c in ["CLOSE","TOTTRDQTY","TOTTRDVAL","DELIV_QTY","DELIV_PER"]:
    df_all[c] = to_num(df_all[c]).fillna(0)

# Core metrics
df_all["DELIVERY_TURNOVER"] = df_all["DELIV_QTY"] * df_all["CLOSE"]
df_all["ATW"] = df_all["TOTTRDVAL"] / 1000

# Filter SERIES (keep NSE EQ & all BSE)
if "SERIES" in df_all.columns:
    before = len(df_all)
    df_all = df_all[
        ((df_all["EXCHANGE"]=="NSE") & (df_all["SERIES"]=="EQ")) |
        (df_all["EXCHANGE"]=="BSE")
    ].copy()
    after = len(df_all)
    print(f"\nFiltered by SERIES (NSE EQ + all BSE): {before} -> {after}")

# Symbol-based exclusions (broad noise filters)
if "SYMBOL" in df_all.columns:
    before = len(df_all)
    df_all = df_all[
        ~df_all["SYMBOL"].str.contains(
            "ETF|LIQUID|FUND|INDEX|NIFTY|SENSEX|GLOBE",
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
    print(f"Excluded 14 bond/NCD ISINs: {before} -> {after}")

# ===== EXCLUDE GOVERNMENT SECURITIES (GS) - NEW =====
if "SYMBOL" in df_all.columns:
    before = len(df_all)
    df_all = df_all[~df_all["SYMBOL"].str.contains(r'GS\d+', regex=True, na=False)].copy()
    after = len(df_all)
    print(f"Excluded Government Securities (GS bonds): {before} -> {after}")

# ===== CALCULATE PROGRESSIVE AVERAGES =====
print("\n📈 Calculating progressive averages...")

# Verify the source dataframe has DATE col
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

    results.append({
        "DATE": latest_dt,
        "SYMBOL": symbol,
        "ISIN": latest.get("ISIN", None),
        "EXCHANGE": latest.get("EXCHANGE", "NSE"),
        "CLOSE": latest.get("CLOSE", 0),
        "DELIV_PER": latest.get("DELIV_PER", 0),
        "DELIVERY_TURNOVER": latest.get("DELIVERY_TURNOVER", 0),
        "ATW": latest.get("ATW", 0),
        "DELIV_PER_1W": df_1w["DELIV_PER"].mean() if len(df_1w) > 0 else latest.get("DELIV_PER", 0),
        "DELIV_PER_1M": df_1m["DELIV_PER"].mean() if len(df_1m) > 0 else latest.get("DELIV_PER", 0),
        "DELIV_PER_3M": df_3m["DELIV_PER"].mean() if len(df_3m) > 0 else latest.get("DELIV_PER", 0),

        "DELIVERY_TURNOVER_1W": df_1w["DELIVERY_TURNOVER"].mean() if len(df_1w) > 0 else latest.get("DELIVERY_TURNOVER", 0),
        "DELIVERY_TURNOVER_1M": df_1m["DELIVERY_TURNOVER"].mean() if len(df_1m) > 0 else latest.get("DELIVERY_TURNOVER", 0),
        "DELIVERY_TURNOVER_3M": df_3m["DELIVERY_TURNOVER"].mean() if len(df_3m) > 0 else latest.get("DELIVERY_TURNOVER", 0),

        "ATW_1W": df_1w["ATW"].mean() if len(df_1w) > 0 else latest.get("ATW", 0),
        "ATW_1M": df_1m["ATW"].mean() if len(df_1m) > 0 else latest.get("ATW", 0),
        "ATW_3M": df_3m["ATW"].mean() if len(df_3m) > 0 else latest.get("ATW", 0),
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
print(f"   Total Stocks: {len(df_final)} (NSE + BSE deduplicated by ISIN/DATE)")
print(f"   NSE Stocks: {len(df_final[df_final['EXCHANGE']=='NSE'])}")
print(f"   BSE Stocks: {len(df_final[df_final['EXCHANGE']=='BSE'])}")
if pd.notna(latest_date):
    print(f"   Latest Date: {latest_date.strftime('%d %b %Y')}")
print(f"   Dashboard (LIVE): {dashboard_file}")
print(f"   Legacy (Backtest): {Config.COMBINED_FILE}")
print("="*70)

import sys
sys.exit(0)
