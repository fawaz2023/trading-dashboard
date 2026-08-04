import pandas as pd
import numpy as np
import json
import warnings
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.tree import DecisionTreeClassifier, _tree
from sklearn.model_selection import TimeSeriesSplit, cross_val_score
from sklearn.inspection import permutation_importance
from collections import Counter
from math import sqrt

warnings.filterwarnings('ignore')

# --- Configuration ---
TRAIN_FILE = 'data/ml/train.csv' # Adjusted to match ml_data_prep.py output
HOLDOUT_FILE = 'data/ml/holdout.csv'
OUTPUT_RULES_FILE = 'data/ai_trading_rules.json'

FEATURES = ['MOMENTUM_RAW', 'FOOTPRINT_RAW', 'STABILITY_RAW', 'DELIV_PER', 'ATW', 'TRIGGER_COUNT_30D']
TARGET_CLF = 'IS_PROFITABLE'
TARGET_REG = 'PEAK_ROI_PCT'

# --- Helper Functions ---
def wilson_score_interval(wins, total, z=1.96):
    if total == 0: return (0, 0)
    p = wins / total
    denominator = 1 + z**2 / total
    center = (p + z**2 / (2 * total)) / denominator
    interval = z * sqrt((p * (1 - p) + z**2 / (4 * total)) / total) / denominator
    return (max(0, center - interval), min(1, center + interval))

def extract_rules_from_tree(tree, feature_names):
    tree_ = tree.tree_
    feature_name = [
        feature_names[i] if i != _tree.TREE_UNDEFINED else "undefined!"
        for i in tree_.feature
    ]
    rules = []
    
    def recurse(node, path):
        if tree_.feature[node] != _tree.TREE_UNDEFINED:
            name = feature_name[node]
            threshold = tree_.threshold[node]
            recurse(tree_.children_left[node], path + [f"{name} <= {threshold:.2f}"])
            recurse(tree_.children_right[node], path + [f"{name} > {threshold:.2f}"])
        else:
            # Leaf node
            val = tree_.value[node][0]
            wins = int(val[1]) if len(val) > 1 else 0
            losses = int(val[0])
            total = wins + losses
            if total >= 10: # Enforce minimum sample size per leaf
                rule_str = " AND ".join(path)
                rules.append({"rule": rule_str, "wins": wins, "total": total})
                
    recurse(0, [])
    return rules

# --- Main Execution ---
def main():
    print("Loading training data...")
    try:
        df = pd.read_csv(TRAIN_FILE)
        df_holdout = pd.read_csv(HOLDOUT_FILE)
    except FileNotFoundError as e:
        print(f"Error loading data: {e}")
        print("Please ensure ml_data_prep.py has saved train.csv and holdout.csv.")
        return

    # Clean data (drop NaNs in features or targets)
    df = df.dropna(subset=FEATURES + [TARGET_CLF, TARGET_REG])
    df_holdout = df_holdout.dropna(subset=FEATURES + [TARGET_CLF, TARGET_REG])
    
    # Ensure Target is 0/1 integer
    df[TARGET_CLF] = df[TARGET_CLF].astype(int)
    df_holdout[TARGET_CLF] = df_holdout[TARGET_CLF].astype(int)

    X = df[FEATURES]
    y_clf = df[TARGET_CLF]
    y_reg = df[TARGET_REG]

    print("\n--- 1. Model Cross-Validation ---")
    tscv = TimeSeriesSplit(n_splits=5)
    
    # Classification CV
    rf_clf = RandomForestClassifier(n_estimators=100, max_depth=3, min_samples_leaf=10, random_state=42)
    cv_scores = cross_val_score(rf_clf, X, y_clf, cv=tscv, scoring='f1')
    print(f"Classification F1 (TimeSeriesSplit): {np.mean(cv_scores):.2f} (+/- {np.std(cv_scores):.2f})")
    
    # Regression CV
    rf_reg = RandomForestRegressor(n_estimators=100, max_depth=4, min_samples_leaf=5, random_state=42)
    cv_mae = -cross_val_score(rf_reg, X, y_reg, cv=tscv, scoring='neg_mean_absolute_error')
    print(f"Regression MAE (TimeSeriesSplit): {np.mean(cv_mae):.2f}% ROI")

    print("\n--- 2. Unbiased Feature Importance ---")
    rf_clf.fit(X, y_clf)
    perm_importance = permutation_importance(rf_clf, X, y_clf, n_repeats=10, random_state=42, scoring='f1')
    
    importance_df = pd.DataFrame({
        'Feature': FEATURES,
        'Importance_Mean': perm_importance.importances_mean,
        'Importance_Std': perm_importance.importances_std
    }).sort_values(by='Importance_Mean', ascending=False)
    
    print("Top Predictive Features for IS_PROFITABLE:")
    for idx, row in importance_df.iterrows():
        print(f"  {row['Feature']}: {row['Importance_Mean']:.4f} (+/- {row['Importance_Std']:.4f})")

    print("\n--- 3. Bootstrap Stability & Rule Extraction ---")
    print("Running 50 bootstrap resamples to find statistically stable rules...")
    
    rule_counter = Counter()
    rule_stats = {}
    
    for i in range(50):
        # Resample with replacement
        boot_df = df.sample(frac=1.0, replace=True, random_state=i)
        X_boot = boot_df[FEATURES]
        y_boot = boot_df[TARGET_CLF]
        
        # Fit shallow tree
        dt = DecisionTreeClassifier(max_depth=3, min_samples_leaf=10, random_state=i)
        dt.fit(X_boot, y_boot)
        
        # Extract rules
        rules = extract_rules_from_tree(dt, FEATURES)
        for r in rules:
            rule_str = r['rule']
            # Aggregate stats across all bootstraps
            if rule_str not in rule_stats:
                rule_stats[rule_str] = {'total_wins': 0, 'total_samples': 0}
            rule_stats[rule_str]['total_wins'] += r['wins']
            rule_stats[rule_str]['total_samples'] += r['total']
            rule_counter[rule_str] += 1

    # Filter rules that appear in >60% of bootstraps
    stable_rules = []
    for rule_str, count in rule_counter.items():
        if count >= 30: # 30 out of 50 = 60%
            wins = rule_stats[rule_str]['total_wins']
            total = rule_stats[rule_str]['total_samples']
            avg_win_rate = wins / total
            low_ci, high_ci = wilson_score_interval(wins, total)
            
            stable_rules.append({
                'rule': rule_str,
                'appearances': count,
                'avg_win_rate': round(avg_win_rate * 100, 1),
                'wilson_lower': round(low_ci * 100, 1),
                'wilson_upper': round(high_ci * 100, 1),
                'total_samples_evaluated': total
            })

    # Sort stable rules by highest lower-bound Wilson CI (most statistically reliable high win rates)
    stable_rules.sort(key=lambda x: x['wilson_lower'], reverse=True)

    print(f"\nFound {len(stable_rules)} stable rules appearing in >60% of tests.")
    
    print("\n--- 4. Final Holdout Truth Test ---")
    final_champion_rules = []
    
    for r in stable_rules:
        # Apply rule to holdout set
        conditions = r['rule'].replace('AND', '&')
        # Translate "<=" and ">" to pandas query format
        conditions = conditions.replace('<=', '<=').replace('>', '>')
        
        try:
            holdout_subset = df_holdout.query(conditions)
            n_holdout = len(holdout_subset)
            
            if n_holdout > 0:
                holdout_wins = holdout_subset[TARGET_CLF].sum()
                holdout_win_rate = (holdout_wins / n_holdout) * 100
                holdout_avg_roi = holdout_subset[TARGET_REG].mean()
                
                r['holdout_n'] = n_holdout
                r['holdout_win_rate'] = round(holdout_win_rate, 1)
                r['holdout_avg_roi'] = round(holdout_avg_roi, 2)
                
                # Only keep if it maintains >50% win rate OR positive ROI in holdout
                if holdout_win_rate >= 50 or holdout_avg_roi > 0:
                    final_champion_rules.append(r)
            else:
                r['holdout_n'] = 0
                r['holdout_win_rate'] = 0
                r['holdout_avg_roi'] = 0
        except Exception as e:
            print(f"Could not evaluate rule on holdout: {r['rule']} - {e}")

    print(f"\n--- FINAL CHAMPION RULES ({len(final_champion_rules)}) ---")
    for i, rule in enumerate(final_champion_rules, 1):
        print(f"\nRule #{i}: IF {rule['rule']}")
        print(f"  Training: {rule['avg_win_rate']}% Win Rate (n={rule['total_samples_evaluated']}, CI: {rule['wilson_lower']}%-{rule['wilson_upper']}%)")
        print(f"  Holdout:  {rule['holdout_win_rate']}% Win Rate (n={rule['holdout_n']}, Avg ROI: {rule['holdout_avg_roi']}%)")
        print("-" * 60)

    # Save to JSON
    output_data = {
        'feature_importance': importance_df.to_dict(orient='records'),
        'champion_rules': final_champion_rules
    }
    
    with open(OUTPUT_RULES_FILE, 'w') as f:
        json.dump(output_data, f, indent=4)
    
    print(f"\n✅ Success! Champion rules saved to {OUTPUT_RULES_FILE}")

if __name__ == "__main__":
    main()
