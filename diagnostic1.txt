import pandas as pd

df = pd.read_csv("data/combined_dashboard_live.csv")
print("Columns in CSV:", df.columns.tolist())

date_candidates = ["DATE", "Date", "date", "BizDt", "TradDt"]
found = next((c for c in date_candidates if c in df.columns), None)

if found:
    df[found] = pd.to_datetime(df[found], errors="coerce")
    df.rename(columns={found: "DATE"}, inplace=True)
    print(f"Using {found} as DATE column")
else:
    print(f"WARNING: No date column found among candidates {date_candidates}")

print(f"Total rows: {len(df)}")
print(f"Unique ISINs: {df['ISIN'].nunique() if 'ISIN' in df.columns else 'N/A'}")
print(f"Exchange counts:\n{df['EXCHANGE'].value_counts() if 'EXCHANGE' in df.columns else 'N/A'}")
print(f"Latest Date: {df['DATE'].max() if 'DATE' in df.columns else 'N/A'}")
