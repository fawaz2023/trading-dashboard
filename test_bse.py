import pandas as pd
from progressive_screener import ProgressiveSpiker

# Load data
df = pd.read_csv("data/combined_dashboard_live.csv")
bse = df[df['EXCHANGE'] == 'BSE'].copy()
signals = ProgressiveSpiker(df).get_signals()

# Get top 20 BSE by delivery turnover
top20 = bse.nlargest(20, 'DELIVERY_TURNOVER')

print("="*70)
print("TOP 20 BSE - ARE THEY BONDS?")
print("="*70)

for idx, stock in top20.iterrows():
    symbol = stock['SYMBOL']
    
    # Check if it's a bond pattern
    is_bond = (
        symbol.endswith('28') or symbol.endswith('27') or symbol.endswith('29') or
        symbol.endswith('26') or symbol.endswith('-RE') or
        'IHFCL' in symbol or 'HUDCO' in symbol or 'PGCIL' in symbol or
        'HDFCB' in symbol or 'BHFL' in symbol or 'SBI' in symbol or
        'NTPC' in symbol
    )
    
    # Check progressive conditions
    c1 = stock['DELIV_PER'] > stock['DELIV_PER_1W']
    c2 = stock['DELIV_PER_1W'] > stock['DELIV_PER_1M']
    c3 = stock['DELIVERY_TURNOVER'] > stock['DELIVERY_TURNOVER_1W']
    c4 = stock['DELIVERY_TURNOVER_1W'] > stock['DELIVERY_TURNOVER_1M']
    c5 = stock['ATW'] > stock['ATW_1W']
    c6 = stock['ATW_1W'] > stock['ATW_1M']
    passed = sum([c1,c2,c3,c4,c5,c6])
    
    bond_flag = "🔴 BOND" if is_bond else "✅ EQUITY"
    
    print(f"{symbol:15} {bond_flag:12} Passed {passed}/6 conditions")
    if not is_bond:
        print(f"  DELIV: {stock['DELIV_PER']:.1f} > {stock['DELIV_PER_1W']:.1f} > {stock['DELIV_PER_1M']:.1f}")
        print(f"  C1={c1}, C2={c2}, C3={c3}, C4={c4}, C5={c5}, C6={c6}")

print("\n" + "="*70)
print("SUMMARY")
print("="*70)
bonds_in_top20 = sum([1 for _, s in top20.iterrows() if (
    s['SYMBOL'].endswith('28') or s['SYMBOL'].endswith('27') or 
    s['SYMBOL'].endswith('29') or s['SYMBOL'].endswith('26') or
    '-RE' in s['SYMBOL'] or 'IHFCL' in s['SYMBOL'] or 
    'HUDCO' in s['SYMBOL'] or 'PGCIL' in s['SYMBOL']
)])

print(f"Bonds in top 20: {bonds_in_top20}")
print(f"Real equities: {20 - bonds_in_top20}")
