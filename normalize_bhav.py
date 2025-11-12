import pandas as pd, os, glob
from config import Config

def normalize_latest_bhav():
    files = sorted(glob.glob(os.path.join(Config.NSE_RAW_DIR, "cm_bhav_*.csv")), reverse=True)
    if not files:
        print("No cm_bhav files found"); return
    src = files[0]
    df = pd.read_csv(src)

    # Map SYMBOL
    if "SYMBOL" in df.columns:
        sym = df["SYMBOL"]
    elif "TckrSymb" in df.columns:
        sym = df["TckrSymb"].astype(str)
        if "SctySrs" in df.columns:
            sym = sym.where(df["SctySrs"].astype(str).str.upper().eq("EQ"), None)
    else:
        print("Could not map SYMBOL column"); return

    df["SYMBOL"] = sym

    # Ensure required numeric columns exist
    for col in ["CLOSE","DELIV_PER","DELIVERY_TURNOVER","ATW"]:
        if col not in df.columns:
            df[col] = 0

    # Keep EQ only when available
    if "SctySrs" in df.columns:
        df = df[df["SctySrs"].astype(str).str.upper().eq("EQ")].copy()

    out = os.path.join(Config.NSE_RAW_DIR, "cm_bhav_latest_normalized.csv")
    df.to_csv(out, index=False)
    print("Normalized saved:", out, "rows:", len(df), "symbols:", df["SYMBOL"].nunique())

if __name__ == "__main__":
    normalize_latest_bhav()
