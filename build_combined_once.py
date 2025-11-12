import pandas as pd, os
from config import Config

src = os.path.join(Config.NSE_RAW_DIR, "cm_bhav_latest_normalized.csv")
if not os.path.exists(src):
    print("Normalized file not found:", src)
    raise SystemExit(1)

df = pd.read_csv(src)

# Keep essential columns and create placeholders
keep = ["SYMBOL","CLOSE"]
for c in keep:
    if c not in df.columns: df[c]=0

df_out = pd.DataFrame({
    "SYMBOL": df["SYMBOL"].astype(str),
    "CLOSE": pd.to_numeric(df["CLOSE"], errors="coerce").fillna(0),
    "DELIV_PER": 60.0,                 # placeholder
    "DELIVERY_TURNOVER": 8000000.0,    # placeholder
    "ATW": 22000.0                     # placeholder
})

# Add rolling windows placeholders so screener can run
for col in ["DELIV_PER","DELIVERY_TURNOVER","ATW"]:
    df_out[f"{col}_1W"] = df_out[col] * 0.96
    df_out[f"{col}_1M"] = df_out[col] * 0.93
    df_out[f"{col}_3M"] = df_out[col] * 0.90

os.makedirs(os.path.dirname(Config.COMBINED_FILE), exist_ok=True)
df_out.to_csv(Config.COMBINED_FILE, index=False)
print("Combined built at:", Config.COMBINED_FILE, "rows:", len(df_out))
