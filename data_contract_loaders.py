import pandas as pd
import json
import os
from enum import Enum

class TradeStatus(Enum):
    WINNER = "WINNER"
    LOSER = "LOSER"
    ACTIVE = "ACTIVE"
    SUSPENDED = "SUSPENDED"

def load_engine_ledgers():
    """Loads all three engine ledgers into a single, deduplicated trades ledger dataframe."""
    ledgers = {
        "SBIA": "data/sbia_ledger.csv",
        "FlexGate": "data/flexgate_ledger.csv",
        "FlexGate2": "data/flexgate2_ledger.csv"
    }
    
    dfs = []
    for engine, path in ledgers.items():
        if os.path.exists(path):
            df = pd.read_csv(path)
            df['ENGINE'] = engine
            dfs.append(df)
            
    if not dfs:
        raise FileNotFoundError("No engine ledgers found.")
        
    df_all = pd.concat(dfs, ignore_index=True)
    
    # Task 1 Fix: Explicit deduplication by SYMBOL and ENTRY_DATE
    before_dedup = len(df_all)
    # We sort by ENGINE just to have a deterministic tie-breaker (e.g. SBIA preferred)
    df_all = df_all.sort_values(by=['ENTRY_DATE', 'SYMBOL', 'ENGINE'])
    df_all = df_all.drop_duplicates(subset=['SYMBOL', 'ENTRY_DATE'], keep='first')
    after_dedup = len(df_all)
    
    # Return-based winner rule (Fix from audit)
    # 1. Compute return %
    df_all['RETURN_PCT'] = ((df_all['EXIT_PRICE'] - df_all['ENTRY_PRICE']) / df_all['ENTRY_PRICE']) * 100
    
    def map_outcome(row):
        status = row.get('STATUS', '')
        if status in ['ACTIVE', 'SUSPENDED']:
            return TradeStatus[status].value
            
        ret = row.get('RETURN_PCT', 0)
        # return > 0 or HIT_TP = Winner (even if HIT_SL with profit)
        if status == 'HIT_TP' or ret > 0:
            return TradeStatus.WINNER.value
        else:
            return TradeStatus.LOSER.value
            
    df_all['OUTCOME_TAG'] = df_all.apply(map_outcome, axis=1)
    
    print(f"[Loader] Loaded {after_dedup} trades across {len(dfs)} engines (Deduplicated {before_dedup - after_dedup} rows).")
    return df_all

def load_fundamentals():
    """Loads fundamental cache data and normalizes keys."""
    path = "data/fundamental_analysis_cache.json"
    if not os.path.exists(path):
        raise FileNotFoundError("Fundamental cache not found.")
        
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
        
    normalized = {}
    for symbol, record in data.items():
        fund = record.get("data", {})
        # Task 1 Fix: Normalize promoter_holding field name explicitly
        # In case the source changes, we ensure 'promoter_holding' is safely extracted
        promo_holding = fund.get("promoter_holding")
        if promo_holding is None:
            # Fallbacks if it was named something else
            promo_holding = fund.get("Promoter Holding") or fund.get("promoter_holding_pct")
            
        fund['promoter_holding'] = promo_holding
        normalized[symbol] = fund
        
    return normalized

if __name__ == "__main__":
    df = load_engine_ledgers()
    funds = load_fundamentals()
    print(f"Fundamentals loaded for {len(funds)} symbols.")
    
    print("\nOutcome Tag Distribution:")
    print(df['OUTCOME_TAG'].value_counts())
