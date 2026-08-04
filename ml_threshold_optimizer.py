import pandas as pd
import numpy as np
from math import sqrt

# --- Configuration ---
TRAIN_FILE = 'data/ml/train.csv'
HOLDOUT_FILE = 'data/ml/holdout.csv'

FEATURES_TO_TEST = ['STABILITY_RAW', 'TRIGGER_COUNT_30D']
TARGET_CLF = 'IS_PROFITABLE'

# --- Helper Function ---
def wilson_score_interval(wins, total, z=1.96):
    if total == 0: return (0, 0)
    p = wins / total
    denominator = 1 + z**2 / total
    center = (p + z**2 / (2 * total)) / denominator
    interval = z * sqrt((p * (1 - p) + z**2 / (4 * total)) / total) / denominator
    return (max(0, center - interval), min(1, center + interval))

# --- Main Execution ---
def main():
    print("--- ML Threshold Optimizer ---")
    try:
        df = pd.read_csv(TRAIN_FILE)
        df_holdout = pd.read_csv(HOLDOUT_FILE)
    except FileNotFoundError as e:
        print(f"Error loading data: {e}")
        return

    # Clean data
    df = df.dropna(subset=FEATURES_TO_TEST + [TARGET_CLF])
    df_holdout = df_holdout.dropna(subset=FEATURES_TO_TEST + [TARGET_CLF])
    df[TARGET_CLF] = df[TARGET_CLF].astype(int)
    df_holdout[TARGET_CLF] = df_holdout[TARGET_CLF].astype(int)

    print(f"Training Data: {len(df)} trades | Holdout Data: {len(df_holdout)} trades\n")

    for feature in FEATURES_TO_TEST:
        print(f"=== Optimizing Threshold for {feature} ===")
        
        # Get all unique values to test as potential thresholds
        unique_values = sorted(df[feature].unique())
        
        best_threshold = None
        best_win_rate = 0
        best_ci_lower = 0
        best_n = 0
        
        print("Testing splits...")
        for val in unique_values:
            # Test "Greater Than" condition
            subset = df[df[feature] > val]
            n = len(subset)
            
            # Require at least 15 trades to consider it statistically valid
            if n < 15:
                continue
                
            wins = subset[TARGET_CLF].sum()
            win_rate = wins / n
            ci_low, ci_high = wilson_score_interval(wins, n)
            
            # We prioritize the lower bound of the CI to ensure we aren't fooled by small samples
            if ci_low > best_ci_lower:
                best_ci_lower = ci_low
                best_threshold = val
                best_win_rate = win_rate
                best_n = n
                best_ci_high = ci_high

        if best_threshold is not None:
            print(f"\n✅ OPTIMAL THRESHOLD FOUND:")
            print(f"Rule: IF {feature} > {best_threshold:.2f}")
            print(f"Training Win Rate: {best_win_rate*100:.1f}% (n={best_n})")
            print(f"Wilson CI: [{best_ci_lower*100:.1f}%, {best_ci_high*100:.1f}%]")
            
            # Test on holdout
            hold_subset = df_holdout[df_holdout[feature] > best_threshold]
            h_n = len(hold_subset)
            if h_n > 0:
                h_wins = hold_subset[TARGET_CLF].sum()
                h_wr = (h_wins / h_n) * 100
                print(f"Holdout Win Rate: {h_wr:.1f}% (n={h_n})")
            else:
                print("Holdout Win Rate: N/A (no trades matched in holdout)")
        else:
            print("No statistically valid threshold found (min 15 trades).")
        print("-" * 50)

    print("\n=== 2D Binning: STABILITY_RAW vs TRIGGER_COUNT_30D ===")
    # Create quartiles for both features to see how they interact
    df['stab_bin'] = pd.qcut(df['STABILITY_RAW'], q=3, labels=['Low Stab', 'Mid Stab', 'High Stab'])
    
    # Use rank first for TRIGGER_COUNT_30D since there might be many duplicated values
    df['trig_bin'] = pd.qcut(df['TRIGGER_COUNT_30D'].rank(method='first'), q=3, labels=['Low Trig', 'Mid Trig', 'High Trig'])
    
    pivot = pd.pivot_table(df, values=TARGET_CLF, index='stab_bin', columns='trig_bin', aggfunc=lambda x: f"{(x.sum()/len(x))*100:.0f}% ({len(x)})")
    
    print("Win Rates by Feature Buckets (Win% and Sample Size):")
    print(pivot)
    print("\nLook for the bucket with the highest Win% and a decent sample size (n>10).")

if __name__ == "__main__":
    main()
