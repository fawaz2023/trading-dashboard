import pandas as pd
from data_contract_loaders import load_engine_ledgers, load_fundamentals, TradeStatus
from datetime import datetime, timedelta

class DataValidationError(Exception):
    pass

def apply_20_percent_wrench(df):
    """
    Input validation stage. Forces failure on bad data before math runs.
    """
    print("[Harness] Applying 20%-wrench data validation...")
    required_cols = ['ENTRY_DATE', 'SYMBOL', 'ENTRY_PRICE', 'EXIT_PRICE', 'STATUS', 'OUTCOME_TAG']
    for col in required_cols:
        if col not in df.columns:
            raise DataValidationError(f"Missing required column: {col}")
            
    # Check for deduplication contract violation
    duplicates = df[df.duplicated(subset=['SYMBOL', 'ENTRY_DATE'], keep=False)]
    if not duplicates.empty:
        raise DataValidationError(f"Data contract violation: Found {len(duplicates)} duplicate rows for SYMBOL+ENTRY_DATE.")
        
    print("[Harness] 20%-wrench validation passed. Data contract is sound.")

def fetch_pit_fundamentals(symbol, trigger_date_str, funds_cache):
    """
    Honest-lag PIT fetcher. 
    In a true implementation, this fetches the quarter ending BEFORE (trigger_date - 45 days).
    Since we are using snapshot data for this scaffold, we flag the record.
    """
    fund = funds_cache.get(symbol)
    if not fund:
        return {"error": "NOT_FOUND"}
        
    # Mark as low confidence because we lack true historical time-series in the cache
    fund['confidence'] = "PARSED_LOW_CONFIDENCE"
    fund['lag_rule_applied'] = "45_DAY_PERIOD_END_LAG"
    
    return fund

def check_stats_guards(df):
    """
    Verifies that the dataset has enough statistical power.
    Requires at least 10 winners and 20 losers.
    """
    df_closed = df[df['OUTCOME_TAG'].isin([TradeStatus.WINNER.value, TradeStatus.LOSER.value])]
    
    winners_count = len(df_closed[df_closed['OUTCOME_TAG'] == TradeStatus.WINNER.value])
    losers_count = len(df_closed[df_closed['OUTCOME_TAG'] == TradeStatus.LOSER.value])
    
    print(f"[Stats Guard] Closed trades: {len(df_closed)} (Winners: {winners_count}, Losers: {losers_count})")
    
    if winners_count < 10 or losers_count < 20:
        print("⚠️  WARNING: Sample size below meaningful threshold (n < 10 winners or n < 20 losers).")
        print("⚠️  WARNING: p-values are descriptive only, not inferential.")
        return False
    
    print("[Stats Guard] Power threshold met. Inferential statistics may proceed.")
    return True

def run_scaffold():
    # 1. Load Data Contract
    df_trades = load_engine_ledgers()
    funds = load_fundamentals()
    
    # 2. Validation
    apply_20_percent_wrench(df_trades)
    
    # 3. Simulate PIT fetching for a few rows
    print("\n[Harness] Simulating PIT-aware fundamental mapping...")
    sample_trades = df_trades.head(3)
    for _, row in sample_trades.iterrows():
        fund = fetch_pit_fundamentals(row['SYMBOL'], row['ENTRY_DATE'], funds)
        status = fund.get('confidence', fund.get('error'))
        print(f"  {row['SYMBOL']} on {row['ENTRY_DATE']}: {status}")
        
    # 4. Stats Guards
    print("")
    check_stats_guards(df_trades)
    
if __name__ == "__main__":
    run_scaffold()
