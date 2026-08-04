import pandas as pd

df = pd.read_csv('data/historical_full_universe.csv', low_memory=False)
df['DATE'] = pd.to_datetime(df['DATE'])
df = df.sort_values(["SYMBOL", "DATE"])

grouped = df.groupby("SYMBOL")
df["FWD_RET_5"] = grouped["CLOSE"].shift(-5) / df["CLOSE"] - 1
df["FWD_RET_10"] = grouped["CLOSE"].shift(-10) / df["CLOSE"] - 1
df["FWD_RET_20"] = grouped["CLOSE"].shift(-20) / df["CLOSE"] - 1

print(df[['EXCHANGE','COMBINEDSCORE','FWD_RET_5','FWD_RET_10','FWD_RET_20']].describe(include='all').to_string())
print("\n")
print(df.groupby('EXCHANGE')[['FWD_RET_5','FWD_RET_10','FWD_RET_20']].quantile([0.99, 0.995, 0.999]).to_string())
