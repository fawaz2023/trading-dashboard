import pandas as pd
from config import Config

df = pd.read_csv(Config.COMBINED_FILE)

print(f"Total rows: {len(df)}")
print(f"\n--- Baseline Filters ---")
print(f"DELIV_PER >= 40: {len(df[df['DELIV_PER'] >= 40])}")
print(f"DELIVERY_TURNOVER >= 3M: {len(df[df['DELIVERY_TURNOVER'] >= 3000000])}")
print(f"ATW >= 15K: {len(df[df['ATW'] >= 15000])}")

# Apply all baseline
df2 = df[(df['DELIV_PER'] >= 40) & (df['DELIVERY_TURNOVER'] >= 3000000) & (df['ATW'] >= 15000)]
print(f"\nStocks passing baseline: {len(df2)}")

# Check progressive
if len(df2) > 0:
    print(f"\n--- Progressive Check ---")
    prog = df2[(df2["DELIV_PER"] > df2["DELIV_PER_1W"]) & 
               (df2["DELIV_PER_1W"] > df2["DELIV_PER_1M"]) & 
               (df2["DELIV_PER_1M"] > df2["DELIV_PER_3M"])]
    print(f"Passing delivery progression: {len(prog)}")
    
    if len(prog) > 0:
        print(f"\nSample signals:")
        print(prog[["SYMBOL", "CLOSE", "DELIV_PER"]].head())
