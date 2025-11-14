import pandas as pd
import os

COMBINED_FILE = os.path.join(os.path.dirname(__file__), '..', 'data', 'combined_2years.csv')

def load_data():
    print(f"Loading combined data from {COMBINED_FILE} ...")
    df = pd.read_csv(COMBINED_FILE)
    print(f"Loaded {len(df)} rows with columns: {list(df.columns)}")
    return df

def run_live_scanner(df):
    symbols = df['SYMBOL'].unique()
    passed_count = 0

    for idx, symbol in enumerate(symbols, 1):
        stock = df[df['SYMBOL'] == symbol].iloc[0]

        conditions = [
            ("Delivery % ≥ 50%", stock['DELIV_PER'] >= 50),
            ("Delivery Turnover ≥ ₹5,000,000", stock['DELIVERY_TURNOVER'] >= 5_000_000),
            ("ATW ≥ ₹20,000", stock['ATW'] >= 20_000),
            ("Latest Day Delivery % > 1 Week Avg", stock['DELIV_PER'] > stock['DELIV_PER_1W']),
            ("1 Week Avg Delivery % > 1 Month Avg", stock['DELIV_PER_1W'] > stock['DELIV_PER_1M']),
            ("1 Month Avg Delivery % > 3 Month Avg", stock['DELIV_PER_1M'] > stock['DELIV_PER_3M']),
            ("Latest Day Delivery Turnover > 1 Week Avg", stock['DELIVERY_TURNOVER'] > stock['DELIVERY_TURNOVER_1W']),
            ("1 Week Avg Delivery Turnover > 1 Month Avg", stock['DELIVERY_TURNOVER_1W'] > stock['DELIVERY_TURNOVER_1M']),
            ("1 Month Avg Delivery Turnover > 3 Month Avg", stock['DELIVERY_TURNOVER_1M'] > stock['DELIVERY_TURNOVER_3M']),
            ("Latest Day ATW > 1 Week Avg", stock['ATW'] > stock['ATW_1W']),
            ("1 Week Avg ATW > 1 Month Avg", stock['ATW_1W'] > stock['ATW_1M']),
            ("1 Month Avg ATW > 3 Month Avg", stock['ATW_1M'] > stock['ATW_3M']),
        ]

        if all(cond for _, cond in conditions):
            print(f"Stock: {symbol} => PASS")
            passed_count += 1

        if idx % 50 == 0:
            print(f"Processed {idx}/{len(symbols)} stocks...\n")

    print(f"\nScan complete. Total stocks passed: {passed_count} out of {len(symbols)}")

if __name__ == "__main__":
    df = load_data()
    run_live_scanner(df)
