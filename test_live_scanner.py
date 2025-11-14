import pandas as pd
import os

# Change this to your combined CSV path
COMBINED_FILE = os.path.join(os.path.dirname(__file__), '..', 'data', 'combined_2years.csv')

def load_data():
    print(f"Loading data from {COMBINED_FILE}")
    df = pd.read_csv(COMBINED_FILE)
    print(f"Loaded {len(df)} rows with columns: {list(df.columns)}")
    return df

def live_scanner(df_all):
    symbols = df_all['SYMBOL'].unique()
    total = len(symbols)
    passed_count = 0

    print(f"Running live scanner on {total} stocks...\n")

    for i, symbol in enumerate(symbols, 1):
        stock = df_all[df_all['SYMBOL'] == symbol].iloc[0]  # latest data (assuming pre-filtered)

        # Define your 12 conditions here
        conditions = [
            ("Delivery Turnover > 1M", stock["DELIVERY_TURNOVER"] > 1_000_000),
            ("Delivery % > 50%", stock["DELIV_PER"] > 50),
            ("ATW > 100", stock["ATW"] > 100),
            ("Close Price > 100", stock["CLOSE"] > 100),
            ("1W delivery % > 1M delivery %", stock["DELIV_PER_1W"] > stock["DELIV_PER_1M"]),
            ("1M delivery % > 3M delivery %", stock["DELIV_PER_1M"] > stock["DELIV_PER_3M"]),
            ("1W Delivery Turnover > 1M", stock["DELIVERY_TURNOVER_1W"] > stock["DELIVERY_TURNOVER_1M"]),
            ("1M Delivery Turnover > 3M", stock["DELIVERY_TURNOVER_1M"] > stock["DELIVERY_TURNOVER_3M"]),
            ("1W ATW > 1M ATW", stock["ATW_1W"] > stock["ATW_1M"]),
            ("1M ATW > 3M ATW", stock["ATW_1M"] > stock["ATW_3M"]),
            ("Total Trade Qty > 1000", stock.get("TOTTRDQTY", 0) > 1000),
            ("Series is Equity", stock.get("SERIES", "EQ") == "EQ"),
        ]

        print(f"Stock: {symbol}")
        overall_pass = True
        for cond_name, cond_result in conditions:
            print(f"  {cond_name}: {'✅' if cond_result else '❌'}")
            if not cond_result:
                overall_pass = False
        print(f"  => Overall result: {'PASS' if overall_pass else 'FAIL'}\n")

        if overall_pass:
            passed_count += 1

        if i % 50 == 0:
            print(f"Processed {i}/{total} stocks...\n")

    print(f"Scan complete. Passed {passed_count} out of {total} stocks.")

if __name__ == "__main__":
    data = load_data()
    live_scanner(data)
