import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import StratifiedKFold, cross_val_predict

def analyze_patterns():
    # Load dataset
    df_train = pd.read_csv('data/ml/train.csv')
    df_holdout = pd.read_csv('data/ml/holdout.csv')
    df = pd.concat([df_train, df_holdout], ignore_index=True)
    
    # Calculate features
    df['SIS'] = ((df['STABILITY_SCORE'] + 1)**0.50 * 
                 (df['FOOTPRINT_SCORE'] + 1)**0.30 * 
                 (df['MOMENTUM_SCORE'] + 1)**0.20) - 1
                 
    df['Whale_Density'] = (df['ATW'] / df['DELIVERY_TURNOVER'].replace(0, np.nan)).fillna(0) * 100000
    df['Implied_Trades'] = (df['DELIVERY_TURNOVER'] / df['ATW'].replace(0, np.nan)).fillna(0)
    
    features = ['SIS', 'Whale_Density', 'Implied_Trades', 'STABILITY_SCORE', 'FOOTPRINT_SCORE', 'MOMENTUM_SCORE', 'ATW', 'DELIVERY_TURNOVER']
    target = 'IS_PROFITABLE'
    
    df = df.dropna(subset=features + ['ROI_PCT', target])
    
    X = df[['SIS', 'Whale_Density', 'Implied_Trades']]
    y = df[target]
    
    model = RandomForestClassifier(n_estimators=100, class_weight='balanced', max_depth=4, random_state=42)
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    
    df['AI_WIN_PROBABILITY'] = cross_val_predict(model, X, y, cv=cv, method='predict_proba')[:, 1] * 100
    
    # Categorize trades
    executed = df[df['AI_WIN_PROBABILITY'] >= 60.0]
    rejected = df[df['AI_WIN_PROBABILITY'] < 60.0]
    
    wins = executed[executed['ROI_PCT'] > 0]
    losses = executed[executed['ROI_PCT'] <= 0]
    
    rejected_wins = rejected[rejected['ROI_PCT'] > 0]
    rejected_losses = rejected[rejected['ROI_PCT'] <= 0]
    
    print("\n--- AVERAGE METRICS BY CATEGORY ---")
    
    def print_stats(name, group):
        if len(group) == 0:
            print(f"{name}: 0 trades")
            return
        print(f"\n{name} ({len(group)} trades):")
        print(f"  Avg SIS: {group['SIS'].mean():.2f}")
        print(f"  Avg Whale Density: {group['Whale_Density'].mean():.2f}")
        print(f"  Avg Implied Trades: {group['Implied_Trades'].mean():.0f}")
        print(f"  Avg STABILITY_SCORE (Block Expansion): {group['STABILITY_SCORE'].mean():.2f}")
        print(f"  Avg FOOTPRINT_SCORE (Capital): {group['FOOTPRINT_SCORE'].mean():.2f}")
        print(f"  Avg ATW (Avg Trade Value): ₹{group['ATW'].mean():,.0f}")
        print(f"  Avg Delivery Turnover: ₹{group['DELIVERY_TURNOVER'].mean():,.0f}")
        print(f"  Avg AI Prob: {group['AI_WIN_PROBABILITY'].mean():.1f}%")

    print_stats("EXECUTED WINS (The Ideal Pattern)", wins)
    print_stats("EXECUTED LOSSES (The AI's Blindspots)", losses)
    print_stats("REJECTED LOSSES (Traps Successfully Avoided)", rejected_losses)
    print_stats("REJECTED WINS (Missed Opportunities)", rejected_wins)
    
    print("\n--- TOP 3 BIGGEST WINS (Executed) ---")
    print(wins[['SYMBOL', 'ROI_PCT', 'SIS', 'Whale_Density', 'Implied_Trades']].sort_values('ROI_PCT', ascending=False).head(3).to_string())

    print("\n--- TOP 3 BIGGEST LOSSES (Executed) ---")
    print(losses[['SYMBOL', 'ROI_PCT', 'SIS', 'Whale_Density', 'Implied_Trades']].sort_values('ROI_PCT', ascending=True).head(3).to_string())

if __name__ == "__main__":
    analyze_patterns()
