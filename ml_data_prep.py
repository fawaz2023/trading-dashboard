import pandas as pd
import numpy as np
import os

def main():
    print("=== PHASE 1: DATA PREPARATION & EDA ===")
    
    # 1. Load Datasets
    print("[*] Loading datasets...")
    df_features = pd.read_csv('data/active_signals_ranked.csv')
    df_targets = pd.read_csv('data/forward_performance.csv')
    
    df_features['DATE'] = pd.to_datetime(df_features['DATE'])
    df_targets['ENTRY_DATE'] = pd.to_datetime(df_targets['ENTRY_DATE'])
    
    # 2. Merge Datasets
    print("[*] Merging datasets...")
    df = pd.merge(
        df_targets,
        df_features,
        left_on=['ENTRY_DATE', 'SYMBOL', 'EXCHANGE'],
        right_on=['DATE', 'SYMBOL', 'EXCHANGE'],
        how='inner'
    )
    
    print(f"    Merged dataset has {len(df)} rows.")
    
    # 3. Create Targets
    df['IS_PROFITABLE'] = (df['PNL'] > 0).astype(int)
    
    # Handle PEAK_ROI_PCT Skewness
    # Adding a small constant if there are negative values, though Peak ROI is usually >= 0.
    df['PEAK_ROI_LOG'] = np.log1p(df['PEAK_ROI_PCT'] / 100) # Ensure it handles percentages well
    
    print("\n[*] Class Balance (IS_PROFITABLE):")
    wins = df['IS_PROFITABLE'].sum()
    losses = len(df) - wins
    print(f"    Wins: {wins} ({wins/len(df)*100:.1f}%)")
    print(f"    Losses: {losses} ({losses/len(df)*100:.1f}%)")
    
    # 4. Correlation Matrix
    print("\n[*] Analyzing Feature Correlations...")
    feature_cols = [
        'MOMENTUM_RAW', 'FOOTPRINT_RAW', 'STABILITY_RAW', 
        'DELIV_PER', 'ATW', 'TRIGGER_COUNT_30D'
    ]
    
    # Handle missing or NaN in features
    df[feature_cols] = df[feature_cols].fillna(0)
    
    corr = df[feature_cols].corr()
    print("    Correlation Matrix:")
    print(corr.to_string(float_format=lambda x: f"{x:.2f}"))
    
    # Drop highly correlated features (>0.75)
    to_drop = set()
    for i in range(len(corr.columns)):
        for j in range(i):
            if abs(corr.iloc[i, j]) > 0.75:
                colname = corr.columns[i]
                print(f"    ⚠️ Dropping {colname} because it is highly correlated with {corr.columns[j]} ({corr.iloc[i, j]:.2f})")
                to_drop.add(colname)
    
    final_features = [c for c in feature_cols if c not in to_drop]
    print(f"    Final Features for Training: {final_features}")
    
    # 5. Temporal Holdout (20%)
    print("\n[*] Creating Chronological Holdout Set...")
    df = df.sort_values('ENTRY_DATE')
    
    holdout_size = int(len(df) * 0.20)
    train_size = len(df) - holdout_size
    
    df_train = df.iloc[:train_size]
    df_holdout = df.iloc[train_size:]
    
    print(f"    Training Set: {len(df_train)} rows")
    print(f"    Holdout Set:  {len(df_holdout)} rows")
    
    # Save datasets
    os.makedirs('data/ml', exist_ok=True)
    df_train.to_csv('data/ml/train.csv', index=False)
    df_holdout.to_csv('data/ml/holdout.csv', index=False)
    print("\n[+] Saved to data/ml/train.csv and data/ml/holdout.csv")

if __name__ == '__main__':
    main()
