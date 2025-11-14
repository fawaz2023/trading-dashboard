import pandas as pd

print("=" * 80)
print("BSE STOCKS - CONDITION CHECK (SIMPLE)")
print("=" * 80)

# Load data
df = pd.read_csv('data/combined_2years.csv')

print(f"\n📊 DATA OVERVIEW:")
print(f"Total rows: {len(df)}")
print(f"Columns: {list(df.columns)}")

# Filter BSE stocks (all records, not just latest)
bse = df[df['EXCHANGE'] == 'BSE'].copy()
print(f"\nTotal BSE records: {len(bse)}")
print(f"Unique BSE stocks: {bse['SYMBOL'].nunique()}")

# Get one record per stock (latest data for each)
bse_latest = bse.groupby('SYMBOL').tail(1)
print(f"\nBSE stocks (one per symbol): {len(bse_latest)}")

print("\n" + "=" * 80)
print("CHECKING 12 PROGRESSIVE CONDITIONS")
print("=" * 80)

initial = len(bse_latest)

# Check each condition
try:
    c1 = bse_latest['DELIV_PER'] > bse_latest['DELIV_PER_1W']
    print(f"\n1. DELIV_PER > DELIV_PER_1W: {c1.sum()}/{initial} ({c1.sum()/initial*100:.1f}%)")
except KeyError as e:
    print(f"\n1. DELIV_PER > DELIV_PER_1W: ❌ Column missing: {e}")
    c1 = pd.Series([False] * len(bse_latest))

try:
    c2 = bse_latest['DELIV_PER_1W'] > bse_latest['DELIV_PER_1M']
    print(f"2. DELIV_PER_1W > DELIV_PER_1M: {c2.sum()}/{initial} ({c2.sum()/initial*100:.1f}%)")
except KeyError as e:
    print(f"2. DELIV_PER_1W > DELIV_PER_1M: ❌ Column missing: {e}")
    c2 = pd.Series([False] * len(bse_latest))

try:
    c3 = bse_latest['DELIV_PER_1M'] > bse_latest['DELIV_PER_3M']
    print(f"3. DELIV_PER_1M > DELIV_PER_3M: {c3.sum()}/{initial} ({c3.sum()/initial*100:.1f}%)")
except KeyError as e:
    print(f"3. DELIV_PER_1M > DELIV_PER_3M: ❌ Column missing: {e}")
    c3 = pd.Series([False] * len(bse_latest))

try:
    c4 = bse_latest['ATW'] > bse_latest['ATW_1W']
    print(f"4. ATW > ATW_1W: {c4.sum()}/{initial} ({c4.sum()/initial*100:.1f}%)")
except KeyError as e:
    print(f"4. ATW > ATW_1W: ❌ Column missing: {e}")
    c4 = pd.Series([False] * len(bse_latest))

try:
    c5 = bse_latest['ATW_1W'] > bse_latest['ATW_1M']
    print(f"5. ATW_1W > ATW_1M: {c5.sum()}/{initial} ({c5.sum()/initial*100:.1f}%)")
except KeyError as e:
    print(f"5. ATW_1W > ATW_1M: ❌ Column missing: {e}")
    c5 = pd.Series([False] * len(bse_latest))

try:
    c6 = bse_latest['ATW_1M'] > bse_latest['ATW_3M']
    print(f"6. ATW_1M > ATW_3M: {c6.sum()}/{initial} ({c6.sum()/initial*100:.1f}%)")
except KeyError as e:
    print(f"6. ATW_1M > ATW_3M: ❌ Column missing: {e}")
    c6 = pd.Series([False] * len(bse_latest))

try:
    c7 = bse_latest['DELIVERY_TURNOVER'] > bse_latest['DELIVERY_TURNOVER_1W']
    print(f"7. DELIVERY_TURNOVER > DELIVERY_TURNOVER_1W: {c7.sum()}/{initial} ({c7.sum()/initial*100:.1f}%)")
except KeyError as e:
    print(f"7. DELIVERY_TURNOVER > DELIVERY_TURNOVER_1W: ❌ Column missing: {e}")
    c7 = pd.Series([False] * len(bse_latest))

try:
    c8 = bse_latest['DELIVERY_TURNOVER_1W'] > bse_latest['DELIVERY_TURNOVER_1M']
    print(f"8. DELIVERY_TURNOVER_1W > DELIVERY_TURNOVER_1M: {c8.sum()}/{initial} ({c8.sum()/initial*100:.1f}%)")
except KeyError as e:
    print(f"8. DELIVERY_TURNOVER_1W > DELIVERY_TURNOVER_1M: ❌ Column missing: {e}")
    c8 = pd.Series([False] * len(bse_latest))

try:
    c9 = bse_latest['DELIVERY_TURNOVER_1M'] > bse_latest['DELIVERY_TURNOVER_3M']
    print(f"9. DELIVERY_TURNOVER_1M > DELIVERY_TURNOVER_3M: {c9.sum()}/{initial} ({c9.sum()/initial*100:.1f}%)")
except KeyError as e:
    print(f"9. DELIVERY_TURNOVER_1M > DELIVERY_TURNOVER_3M: ❌ Column missing: {e}")
    c9 = pd.Series([False] * len(bse_latest))

try:
    c10 = bse_latest['CLOSE'] > 20
    print(f"10. CLOSE > 20: {c10.sum()}/{initial} ({c10.sum()/initial*100:.1f}%)")
except KeyError as e:
    print(f"10. CLOSE > 20: ❌ Column missing: {e}")
    c10 = pd.Series([False] * len(bse_latest))

try:
    c11 = bse_latest['TOTTRDQTY'] > 100000
    print(f"11. TOTTRDQTY > 100000: {c11.sum()}/{initial} ({c11.sum()/initial*100:.1f}%)")
except KeyError as e:
    print(f"11. TOTTRDQTY > 100000: ❌ Column missing: {e}")
    c11 = pd.Series([False] * len(bse_latest))

try:
    c12 = bse_latest['DELIV_PER'] > 50
    print(f"12. DELIV_PER > 50: {c12.sum()}/{initial} ({c12.sum()/initial*100:.1f}%)")
except KeyError as e:
    print(f"12. DELIV_PER > 50: ❌ Column missing: {e}")
    c12 = pd.Series([False] * len(bse_latest))

# Check all conditions
all_pass = c1 & c2 & c3 & c4 & c5 & c6 & c7 & c8 & c9 & c10 & c11 & c12
bse_signals = bse_latest[all_pass]

print("\n" + "=" * 80)
print("FINAL RESULT")
print("=" * 80)
print(f"\nBSE stocks passing ALL 12 conditions: {len(bse_signals)}")
if initial > 0:
    print(f"Pass rate: {len(bse_signals)/initial*100:.2f}%")

if len(bse_signals) > 0:
    print(f"\n✅ BSE SIGNALS FOUND:")
    print(bse_signals[['SYMBOL', 'EXCHANGE', 'CLOSE']].to_string())
else:
    print(f"\n❌ NO BSE stocks pass all 12 conditions")

print("\n" + "=" * 80)
