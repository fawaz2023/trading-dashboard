import pandas as pd
import numpy as np

print("=" * 80)
print("BSE STOCKS - PROGRESSIVE CONDITIONS CHECK")
print("=" * 80)

# Load combined data
df = pd.read_csv('data/combined_2years.csv')

# Get latest date
latest_date = df['DATE'].max()
print(f"\nLatest Date: {latest_date}")

# Filter BSE stocks on latest date
bse = df[(df['EXCHANGE'] == 'BSE') & (df['DATE'] == latest_date)].copy()
print(f"\nTotal BSE Stocks on {latest_date}: {len(bse)}")

print("\n" + "=" * 80)
print("CHECKING EACH PROGRESSIVE CONDITION")
print("=" * 80)

# Track pass/fail for each condition
initial_count = len(bse)

# Condition 1: DELIV_PER > DELIV_PER_1W
c1 = bse['DELIV_PER'] > bse['DELIV_PER_1W']
print(f"\n1. DELIV_PER > DELIV_PER_1W")
print(f"   Pass: {c1.sum()} / {initial_count} ({c1.sum()/initial_count*100:.1f}%)")
print(f"   Sample values:")
print(f"   - DELIV_PER: {bse['DELIV_PER'].describe()[['mean', '50%', 'max']]}")
print(f"   - DELIV_PER_1W: {bse['DELIV_PER_1W'].describe()[['mean', '50%', 'max']]}")

# Condition 2: DELIV_PER_1W > DELIV_PER_1M
c2 = bse['DELIV_PER_1W'] > bse['DELIV_PER_1M']
print(f"\n2. DELIV_PER_1W > DELIV_PER_1M")
print(f"   Pass: {c2.sum()} / {initial_count} ({c2.sum()/initial_count*100:.1f}%)")

# Condition 3: DELIV_PER_1M > DELIV_PER_3M
c3 = bse['DELIV_PER_1M'] > bse['DELIV_PER_3M']
print(f"\n3. DELIV_PER_1M > DELIV_PER_3M")
print(f"   Pass: {c3.sum()} / {initial_count} ({c3.sum()/initial_count*100:.1f}%)")

# Condition 4: ATW > ATW_1W
c4 = bse['ATW'] > bse['ATW_1W']
print(f"\n4. ATW > ATW_1W")
print(f"   Pass: {c4.sum()} / {initial_count} ({c4.sum()/initial_count*100:.1f}%)")

# Condition 5: ATW_1W > ATW_1M
c5 = bse['ATW_1W'] > bse['ATW_1M']
print(f"\n5. ATW_1W > ATW_1M")
print(f"   Pass: {c5.sum()} / {initial_count} ({c5.sum()/initial_count*100:.1f}%)")

# Condition 6: ATW_1M > ATW_3M
c6 = bse['ATW_1M'] > bse['ATW_3M']
print(f"\n6. ATW_1M > ATW_3M")
print(f"   Pass: {c6.sum()} / {initial_count} ({c6.sum()/initial_count*100:.1f}%)")

# Condition 7: DELIVERY_TURNOVER > DELIVERY_TURNOVER_1W
c7 = bse['DELIVERY_TURNOVER'] > bse['DELIVERY_TURNOVER_1W']
print(f"\n7. DELIVERY_TURNOVER > DELIVERY_TURNOVER_1W")
print(f"   Pass: {c7.sum()} / {initial_count} ({c7.sum()/initial_count*100:.1f}%)")

# Condition 8: DELIVERY_TURNOVER_1W > DELIVERY_TURNOVER_1M
c8 = bse['DELIVERY_TURNOVER_1W'] > bse['DELIVERY_TURNOVER_1M']
print(f"\n8. DELIVERY_TURNOVER_1W > DELIVERY_TURNOVER_1M")
print(f"   Pass: {c8.sum()} / {initial_count} ({c8.sum()/initial_count*100:.1f}%)")

# Condition 9: DELIVERY_TURNOVER_1M > DELIVERY_TURNOVER_3M
c9 = bse['DELIVERY_TURNOVER_1M'] > bse['DELIVERY_TURNOVER_3M']
print(f"\n9. DELIVERY_TURNOVER_1M > DELIVERY_TURNOVER_3M")
print(f"   Pass: {c9.sum()} / {initial_count} ({c9.sum()/initial_count*100:.1f}%)")

# Condition 10: CLOSE > 20
c10 = bse['CLOSE'] > 20
print(f"\n10. CLOSE > 20")
print(f"    Pass: {c10.sum()} / {initial_count} ({c10.sum()/initial_count*100:.1f}%)")

# Condition 11: TOTTRDQTY > 100000
c11 = bse['TOTTRDQTY'] > 100000
print(f"\n11. TOTTRDQTY > 100000")
print(f"    Pass: {c11.sum()} / {initial_count} ({c11.sum()/initial_count*100:.1f}%)")

# Condition 12: DELIV_PER > 50
c12 = bse['DELIV_PER'] > 50
print(f"\n12. DELIV_PER > 50")
print(f"    Pass: {c12.sum()} / {initial_count} ({c12.sum()/initial_count*100:.1f}%)")

# Apply ALL conditions
all_conditions = c1 & c2 & c3 & c4 & c5 & c6 & c7 & c8 & c9 & c10 & c11 & c12
bse_signals = bse[all_conditions]

print("\n" + "=" * 80)
print("FINAL RESULT")
print("=" * 80)
print(f"\nBSE stocks passing ALL 12 conditions: {len(bse_signals)}")
print(f"Pass rate: {len(bse_signals)/initial_count*100:.2f}%")

if len(bse_signals) > 0:
    print(f"\n✅ BSE SIGNALS FOUND! Here they are:")
    print(bse_signals[['SYMBOL', 'CLOSE', 'DELIV_PER', 'ATW', 'DELIVERY_TURNOVER']].to_string())
else:
    print(f"\n❌ NO BSE stocks pass all 12 conditions")
    print(f"\nMost restrictive conditions (lowest pass rate):")
    
    # Find which conditions fail most
    pass_rates = {
        'C1 DELIV_PER': c1.sum(),
        'C2 DELIV_PER_1W': c2.sum(),
        'C3 DELIV_PER_1M': c3.sum(),
        'C4 ATW': c4.sum(),
        'C5 ATW_1W': c5.sum(),
        'C6 ATW_1M': c6.sum(),
        'C7 DELIV_TURN': c7.sum(),
        'C8 DELIV_TURN_1W': c8.sum(),
        'C9 DELIV_TURN_1M': c9.sum(),
        'C10 CLOSE>20': c10.sum(),
        'C11 QTY>100k': c11.sum(),
        'C12 DELIV>50': c12.sum()
    }
    
    sorted_rates = sorted(pass_rates.items(), key=lambda x: x[1])
    print("\nConditions sorted by pass rate (hardest first):")
    for cond, count in sorted_rates[:5]:
        print(f"   {cond}: {count}/{initial_count} ({count/initial_count*100:.1f}%)")

print("\n" + "=" * 80)

# Check for missing data
print("\nDATA QUALITY CHECK:")
print(f"BSE stocks with missing DELIV_PER: {bse['DELIV_PER'].isna().sum()}")
print(f"BSE stocks with missing ATW: {bse['ATW'].isna().sum()}")
print(f"BSE stocks with missing DELIVERY_TURNOVER: {bse['DELIVERY_TURNOVER'].isna().sum()}")

print("\n" + "=" * 80)
