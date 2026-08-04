import os
import glob
import pandas as pd
import numpy as np
from datetime import datetime

print("=" * 70)
print("HISTORICAL FULL-UNIVERSE FEATURE BUILDER")
print("=" * 70)

def to_num(s):
    return pd.to_numeric(s, errors="coerce")

def normalize_nse(bhav_df, deliv_df):
    if bhav_df.empty: return pd.DataFrame()
    print("Normalizing NSE...")
    df = bhav_df.copy()
    if "TtlNbOfTxsExctd" in df.columns:
        df["NO_OF_TRADES"] = df["TtlNbOfTxsExctd"]
        
    for c in ["CLOSE", "TOTTRDQTY", "TOTTRDVAL", "NO_OF_TRADES"]:
        if c in df.columns:
            df[c] = to_num(df[c]).fillna(0)
            
    df = df[df["SERIES"] == "EQ"].copy()
    df["EXCHANGE"] = "NSE"
    if "DATE" in df.columns: df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
    
    if not deliv_df.empty:
        if " SYMBOL" in deliv_df.columns:
            deliv_df = deliv_df.rename(columns={" SYMBOL": "SYMBOL"})
        if "DATE" in deliv_df.columns: deliv_df["DATE"] = pd.to_datetime(deliv_df["DATE"], errors="coerce")
        
        # Merge delivery
        cols_keep = [c for c in ["SYMBOL", "DATE", "DELIV_PER", "DELIV_QTY"] if c in deliv_df.columns]
        df = df.merge(deliv_df[cols_keep], on=["SYMBOL", "DATE"], how="left")
    
    for c in ["DELIV_PER", "DELIV_QTY"]:
        if c not in df.columns:
            df[c] = 0
        df[c] = to_num(df[c]).fillna(0)
        
    return df

def normalize_bse(bhav_df, deliv_df):
    if bhav_df.empty: return pd.DataFrame()
    print("Normalizing BSE...")
    ren = {
        "BizDt": "DATE",
        "TckrSymb": "SYMBOL",
        "ClsPric": "CLOSE",
        "TtlTradgVol": "TOTTRDQTY",
        "TtlTrfVal": "TOTTRDVAL",
        "TtlNbOfTxsExctd": "NO_OF_TRADES",
    }
    df = bhav_df.rename(columns=ren).copy()
    
    for c in ["CLOSE", "TOTTRDQTY", "TOTTRDVAL", "NO_OF_TRADES"]:
        if c in df.columns:
            df[c] = to_num(df[c]).fillna(0)
            
    df["EXCHANGE"] = "BSE"
    df["SYMBOL"] = df["SYMBOL"].astype(str)
    if "DATE" in df.columns: df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
    
    if not deliv_df.empty:
        ren_del = {"BizDt": "DATE", "TckrSymb": "SYMBOL", "DlvrdQty": "DELIV_QTY", "DlvryPct": "DELIV_PER"}
        del_df = deliv_df.rename(columns=ren_del)
        del_df["SYMBOL"] = del_df["SYMBOL"].astype(str)
        if "DATE" in del_df.columns: del_df["DATE"] = pd.to_datetime(del_df["DATE"], errors="coerce")
        cols_keep = [c for c in ["SYMBOL", "DATE", "DELIV_PER", "DELIV_QTY", "SC_CODE"] if c in del_df.columns]
        
        if "SC_CODE" in df.columns and "SC_CODE" in del_df.columns:
            df["SC_CODE"] = df["SC_CODE"].astype(str)
            del_df["SC_CODE"] = del_df["SC_CODE"].astype(str)
            df = df.merge(del_df[cols_keep], on=["SC_CODE", "DATE"], how="left", suffixes=("", "_del"))
            if "SYMBOL_del" in df.columns:
                df["SYMBOL"] = df["SYMBOL"].fillna(df["SYMBOL_del"])
                df = df.drop(columns=["SYMBOL_del"])
        else:
            df = df.merge(del_df[cols_keep], on=["SYMBOL", "DATE"], how="left")

    for c in ["DELIV_PER", "DELIV_QTY"]:
        if c not in df.columns:
            df[c] = 0
        df[c] = to_num(df[c]).fillna(0)
        
    return df

def load_data():
    nse_raw_dir = "data/nse_raw"
    bse_raw_dir = "data/bse_raw"
    
    nse_bhav_files = sorted(glob.glob(os.path.join(nse_raw_dir, "nse_bhav_*.csv")))
    nse_deliv_files = sorted(glob.glob(os.path.join(nse_raw_dir, "nse_delivery_*.csv")))
    
    bse_bhav_files = sorted(glob.glob(os.path.join(bse_raw_dir, "bse_bhav_*.csv")))
    bse_deliv_files = sorted(glob.glob("data/bse_delivery_*.csv"))
    
    print(f"Loading {len(nse_bhav_files)} NSE days, {len(bse_bhav_files)} BSE days...")
    
    nse_bhavs = []
    nse_delivs = []
    bse_bhavs = []
    bse_delivs = []
    
    for f in nse_bhav_files:
        d_str = os.path.basename(f).replace("nse_bhav_", "").replace(".csv", "")
        df = pd.read_csv(f, low_memory=False)
        df["DATE"] = pd.to_datetime(d_str, format="%Y%m%d")
        nse_bhavs.append(df)
        
    for f in nse_deliv_files:
        d_str = os.path.basename(f).replace("nse_delivery_", "").replace(".csv", "")
        df = pd.read_csv(f, low_memory=False)
        df["DATE"] = pd.to_datetime(d_str, format="%Y%m%d")
        nse_delivs.append(df)
        
    for f in bse_bhav_files:
        d_str = os.path.basename(f).replace("bse_bhav_", "").replace(".csv", "")
        df = pd.read_csv(f, low_memory=False)
        if "DATE" not in df.columns and "BizDt" not in df.columns:
            df["DATE"] = pd.to_datetime(d_str, format="%Y%m%d")
        bse_bhavs.append(df)
        
    for f in bse_deliv_files:
        d_str = os.path.basename(f).replace("bse_delivery_", "").replace(".csv", "")
        df = pd.read_csv(f, low_memory=False)
        if "DATE" not in df.columns:
            df["DATE"] = pd.to_datetime(d_str, format="%Y%m%d")
        else:
            df["DATE"] = pd.to_datetime(df["DATE"])
        bse_delivs.append(df)

    df_nse_bhav = pd.concat(nse_bhavs, ignore_index=True) if nse_bhavs else pd.DataFrame()
    df_nse_del = pd.concat(nse_delivs, ignore_index=True) if nse_delivs else pd.DataFrame()
    df_bse_bhav = pd.concat(bse_bhavs, ignore_index=True) if bse_bhavs else pd.DataFrame()
    df_bse_del = pd.concat(bse_delivs, ignore_index=True) if bse_delivs else pd.DataFrame()
    
    df_nse = normalize_nse(df_nse_bhav, df_nse_del)
    df_bse = normalize_bse(df_bse_bhav, df_bse_del)
    
    # Exclude ETFs / Bonds
    df_all = pd.concat([df_nse, df_bse], ignore_index=True)
    df_all["SYMBOL"] = df_all["SYMBOL"].astype(str)
    df_all = df_all[~df_all["SYMBOL"].str.contains(r"ETF|LIQUID|FUND|INDEX|NIFTY|SENSEX|GLOBE|^GS\d|^\d{3,4}GS\d|^SGB|\d+TB$|SDL|MHSDL|ZC\d{2,}|PP$|^CS\d", case=False, regex=True, na=False)]
    
    # Deduplicate by ISIN + DATE
    if "ISIN" in df_all.columns:
        df_all["EXCH_PRIORITY"] = df_all["EXCHANGE"].apply(lambda x: 0 if x == "NSE" else 1)
        df_all = df_all.sort_values(["ISIN", "DATE", "EXCH_PRIORITY"])
        df_all = df_all.drop_duplicates(subset=["ISIN", "DATE"], keep="first")
        df_all = df_all.drop(columns=["EXCH_PRIORITY"])
    
    df_all["DELIVERY_TURNOVER"] = df_all["DELIV_QTY"] * df_all["CLOSE"]
    df_all["ATW"] = (df_all["TOTTRDVAL"] / df_all["NO_OF_TRADES"].replace(0, pd.NA)).fillna(0)
    
    return df_all

def compute_historical_features(df):
    print("Computing vectorized rolling features...")
    df = df.sort_values(["SYMBOL", "DATE"]).copy()
    
    # We need strictly trading days, so we group by symbol and roll on rows
    grouped = df.groupby("SYMBOL")
    
    # Shift(1) so today's metrics use strictly PAST data (no forward leakage)
    df["DELIV_PER_1W"] = grouped["DELIV_PER"].transform(lambda x: x.shift(1).rolling(5, min_periods=3).mean())
    df["DELIV_PER_1M"] = grouped["DELIV_PER"].transform(lambda x: x.shift(1).rolling(22, min_periods=10).mean())
    
    df["DELIVERY_TURNOVER_1W"] = grouped["DELIVERY_TURNOVER"].transform(lambda x: x.shift(1).rolling(5, min_periods=3).mean())
    df["DELIVERY_TURNOVER_1M"] = grouped["DELIVERY_TURNOVER"].transform(lambda x: x.shift(1).rolling(22, min_periods=10).mean())
    
    df["ATW_1W"] = grouped["ATW"].transform(lambda x: x.shift(1).rolling(5, min_periods=3).mean())
    df["ATW_1M"] = grouped["ATW"].transform(lambda x: x.shift(1).rolling(22, min_periods=10).mean())
    
    # Filter valid rows
    df = df.dropna(subset=["DELIV_PER_1M", "DELIVERY_TURNOVER_1M", "ATW_1M"])
    
    print("Scoring 0-100 Cross-Sectionally per Day...")
    # Calculate base ratios
    df["MOM_RATIO"] = (df["DELIV_PER_1W"] / df["DELIV_PER_1M"].replace(0, np.nan)).fillna(1.0)
    df["FOOT_RATIO"] = (df["DELIVERY_TURNOVER_1W"] / df["DELIVERY_TURNOVER_1M"].replace(0, np.nan)).fillna(1.0)
    df["STAB_RATIO"] = (df["ATW_1W"] / df["ATW_1M"].replace(0, np.nan)).fillna(1.0)
    
    # Cross-sectional ranking per day for 0-100 score
    def rank_0_100(series):
        return series.rank(pct=True) * 100
        
    date_grp = df.groupby(["DATE", "EXCHANGE"]) # Rank separately by exchange!
    df["MOMENTUM_SCORE"] = date_grp["MOM_RATIO"].transform(rank_0_100)
    df["FOOTPRINT_SCORE"] = date_grp["FOOT_RATIO"].transform(rank_0_100)
    df["STABILITY_SCORE"] = date_grp["STAB_RATIO"].transform(rank_0_100)
    
    # 0-100 Combined Score
    df["COMBINEDSCORE"] = (df["MOMENTUM_SCORE"] + df["FOOTPRINT_SCORE"] + df["STABILITY_SCORE"]) / 3.0
    
    # Coverage Flags
    df["HASMOMENTUMDATA"] = df["DELIV_PER_1W"].notna() & df["DELIV_PER_1M"].notna()
    df["HASFOOTPRINTDATA"] = df["DELIVERY_TURNOVER_1W"].notna() & df["DELIVERY_TURNOVER_1M"].notna()
    df["HASSTABILITYHISTORY20D"] = df["ATW_1W"].notna() & df["ATW_1M"].notna()
    
    return df

if __name__ == "__main__":
    df_raw = load_data()
    print(f"Total raw rows: {len(df_raw)}")
    
    df_features = compute_historical_features(df_raw)
    
    out_file = "data/historical_full_universe.csv"
    os.makedirs("data", exist_ok=True)
    df_features.to_csv(out_file, index=False)
    
    print(f"✅ Saved {len(df_features)} historical rows to {out_file}")
    print("Run `evaluate_institutional_edge.py` to backtest.")
