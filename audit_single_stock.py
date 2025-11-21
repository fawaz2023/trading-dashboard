# Add this to a new file: audit_single_stock.py
import pandas as pd

# Load your output
df = pd.read_csv('data/combined_dashboard_live.csv')

# Pick a stock that passed all 12 conditions
stock = df[df['ALL_12_CONDITIONS'] == True].iloc[0]
symbol = stock['SYMBOL']

print(f"\n{'='*70}")
print(f"AUDITING: {symbol}")
print(f"{'='*70}")

# Show all raw values
print("\n📊 RAW VALUES:")
print(f"CLOSE: ₹{stock['CLOSE']}")
print(f"DELIV_PER: {stock['DELIV_PER']}%")
print(f"DELIV_QTY: {stock['DELIV_QTY']:,.0f}")
print(f"TTL_TRD_QNTY: {stock['TTL_TRD_QNTY']:,.0f}")
print(f"TOTTRDVAL: ₹{stock['TOTTRDVAL']:,.0f}")

# Manually calculate what SHOULD be
print("\n🧮 MANUAL CALCULATIONS:")
expected_delivery_turnover = stock['DELIV_QTY'] * stock['CLOSE']
expected_atw = stock['TOTTRDVAL'] / 1000

print(f"Expected DELIVERY_TURNOVER: ₹{expected_delivery_turnover:,.0f}")
print(f"Actual DELIVERY_TURNOVER: ₹{stock['DELIVERY_TURNOVER']:,.0f}")
print(f"✅ MATCH" if abs(expected_delivery_turnover - stock['DELIVERY_TURNOVER']) < 1 else "❌ MISMATCH!")

print(f"\nExpected ATW: ₹{expected_atw:,.0f}")
print(f"Actual ATW: ₹{stock['ATW']:,.0f}")
print(f"✅ MATCH" if abs(expected_atw - stock['ATW']) < 1 else "❌ MISMATCH!")

# Check progressive logic
print("\n📈 PROGRESSIVE CONDITIONS CHECK:")
print(f"DELIV_PER: {stock['DELIV_PER']:.2f} > {stock['DELIV_PER_1W']:.2f} > {stock['DELIV_PER_1M']:.2f} > {stock['DELIV_PER_3M']:.2f}")
is_progressive = (stock['DELIV_PER'] > stock['DELIV_PER_1W'] > stock['DELIV_PER_1M'] > stock['DELIV_PER_3M'])
print(f"✅ PROGRESSIVE" if is_progressive else "❌ NOT PROGRESSIVE!")
