import pandas as pd
import json
import os
import sys
from datetime import datetime

CONFIG_FILE = "institutional_config.json"
INPUT_FILE = "data/combined_dashboard_live.csv"

def run_validator():
    print("Starting Institutional Schema Validation...")
    
    if not os.path.exists(INPUT_FILE):
        print(f"ERROR: {INPUT_FILE} not found. Ensure pipeline has run.")
        sys.exit(1)
        
    df = pd.read_csv(INPUT_FILE)
    columns = set(df.columns)
    
    required_cols = [
        "DATE", "SYMBOL", "EXCHANGE", "CLOSE", 
        "DELIV_PER", "DELIVERY_TURNOVER", "ATW",
        "DELIV_PER_1W", "DELIV_PER_1M",
        "DELIVERY_TURNOVER_1W", "DELIVERY_TURNOVER_1M",
        "ATW_1W", "ATW_1M"
    ]
    
    missing = [c for c in required_cols if c not in columns]
    if missing:
        print(f"ERROR: Missing required columns in baseline: {missing}")
        sys.exit(1)
        
    # Build config according to v3 specs
    config = {
        "validation_timestamp": datetime.now().isoformat(),
        "status": "VALID",
        "inputs": {
            "combined_live": INPUT_FILE,
            "signal_history": "data/signal_scores_history.csv",
        },
        "mappings": {
            "combined": {
                "date": "DATE",
                "symbol": "SYMBOL",
                "exchange": "EXCHANGE",
                "close": "CLOSE",
                "deliv_per": "DELIV_PER",
                "deliv_turnover": "DELIVERY_TURNOVER",
                "atw": "ATW"
            },
            "nse_bhav": {
                "symbol_col": "SYMBOL",
                "series_col": "SERIES",
                "close_col": "CLOSE"
            },
            "nse_delivery": {
                "symbol_col": "SYMBOL",
                "deliv_per_col": "DELIV_PER"
            },
            "bse_bhav": {
                "symbol_col": "TICKER",
                "scrip_code": "SC_CODE",
                "close_col": "CLOSE"
            },
            "bse_delivery": {
                "symbol_col": "TICKER",
                "scrip_code": "SC_CODE",
                "deliv_per_col": "DELIVERY_PER"
            }
        }
    }
    
    with open(CONFIG_FILE, "w") as f:
        json.dump(config, f, indent=4)
        
    print(f"Config saved to {CONFIG_FILE}")
    sys.exit(0)

if __name__ == "__main__":
    run_validator()
