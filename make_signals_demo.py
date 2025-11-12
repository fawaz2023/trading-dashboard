import pandas as pd, os, numpy as np
from config import Config

src = os.path.join(Config.NSE_RAW_DIR, "cm_bhav_latest_normalized.csv")
df = pd.read_csv(src)

# Start with neutral baseline
out = pd.DataFrame({
    "SYMBOL": df["SYMBOL"].astype(str),
    "CLOSE": np.random.uniform(100, 3000, len(df)),
    "DELIV_PER": np.random.uniform(55, 85, len(df)),
    "DELIVERY_TURNOVER": np.random.uniform(5_500_000, 50_000_000, len(df)),
    "ATW": np.random.uniform(21000, 30000, len(df)),
})

# Construct progression so a subset passes Today > 1W > 1M > 3M
ratio = np.random.uniform(1.03, 1.12, len(out))
out["DELIV_PER_1W"] = out["DELIV_PER"] / ratio
out["DELIV_PER_1M"] = out["DELIV_PER_1W"] / ratio
out["DELIV_PER_3M"] = out["DELIV_PER_1M"] / ratio

ratio2 = np.random.uniform(1.03, 1.12, len(out))
out["DELIVERY_TURNOVER_1W"] = out["DELIVERY_TURNOVER"] / ratio2
out["DELIVERY_TURNOVER_1M"] = out["DELIVERY_TURNOVER_1W"] / ratio2
out["DELIVERY_TURNOVER_3M"] = out["DELIVERY_TURNOVER_1M"] / ratio2

ratio3 = np.random.uniform(1.03, 1.12, len(out))
out["ATW_1W"] = out["ATW"] / ratio3
out["ATW_1M"] = out["ATW_1W"] / ratio3
out["ATW_3M"] = out["ATW_1M"] / ratio3

# Force roughly 1% to pass strictly
mask = np.random.rand(len(out)) < 0.01
out.loc[~mask, ["DELIV_PER","DELIVERY_TURNOVER","ATW"]] *= 0.8

os.makedirs(os.path.dirname(Config.COMBINED_FILE), exist_ok=True)
out.to_csv(Config.COMBINED_FILE, index=False)
print("Demo combined saved:", Config.COMBINED_FILE, "rows:", len(out), "expected signals ~", int(mask.sum()))
