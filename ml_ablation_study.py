import pandas as pd
import numpy as np
import os
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import StratifiedKFold
from sklearn.metrics import accuracy_score, precision_score, f1_score
from statsmodels.stats.outliers_influence import variance_inflation_factor
import shap
import json

def calculate_market_percentiles():
    print("Loading full market universe...")
    # Load historical universe
    df_univ = pd.read_csv("data/historical_full_universe.csv")
    df_univ["DATE"] = pd.to_datetime(df_univ["DATE"])
    df_univ = df_univ.sort_values(["SYMBOL", "DATE"])
    
    # Calculate 3M (66 trading days) rolling averages for the full universe
    print("Calculating 3M rolling features for full universe...")
    grp_sym = df_univ.groupby("SYMBOL")
    df_univ["ATW_3M"] = grp_sym["ATW"].transform(lambda x: x.shift(1).rolling(66, min_periods=20).mean())
    df_univ["DELIVERY_TURNOVER_3M"] = grp_sym["DELIVERY_TURNOVER"].transform(lambda x: x.shift(1).rolling(66, min_periods=20).mean())
    df_univ["DELIV_PER_3M"] = grp_sym["DELIV_PER"].transform(lambda x: x.shift(1).rolling(66, min_periods=20).mean())
    
    # Calculate base ratios (Handle division by zero)
    print("Calculating base ratios...")
    df_univ["BLOCK_RATIO"] = (df_univ["ATW"] / df_univ["ATW_3M"].replace(0, np.nan)).fillna(1.0)
    df_univ["FOOT_RATIO"] = (df_univ["DELIVERY_TURNOVER"] / df_univ["DELIVERY_TURNOVER_3M"].replace(0, np.nan)).fillna(1.0)
    df_univ["HOARD_RATIO"] = (df_univ["DELIV_PER"] / df_univ["DELIV_PER_3M"].replace(0, np.nan)).fillna(1.0)
    
    print("Ranking full universe cross-sectionally per day and exchange...")
    # Rank 0-100
    def rank_pct(series):
        return series.rank(pct=True) * 100
        
    grp = df_univ.groupby(["DATE", "EXCHANGE"])
    df_univ["BLOCK_P"] = grp["BLOCK_RATIO"].transform(rank_pct)
    df_univ["FOOT_P"] = grp["FOOT_RATIO"].transform(rank_pct)
    df_univ["HOARD_P"] = grp["HOARD_RATIO"].transform(rank_pct)
    
    return df_univ[["DATE", "SYMBOL", "EXCHANGE", "BLOCK_P", "FOOT_P", "HOARD_P"]]

def prep_dataset(df_percentiles):
    print("Loading N=82 dataset...")
    df_train = pd.read_csv("data/ml/train.csv")
    df_holdout = pd.read_csv("data/ml/holdout.csv")
    df = pd.concat([df_train, df_holdout], ignore_index=True)
    df["DATE"] = pd.to_datetime(df["DATE"])
    
    print("Merging true percentiles onto dataset to avoid cross-sectional bias...")
    df = pd.merge(df, df_percentiles, on=["DATE", "SYMBOL", "EXCHANGE"], how="left")
    
    # Fill NA just in case
    df["BLOCK_P"] = df["BLOCK_P"].fillna(50)
    df["FOOT_P"] = df["FOOT_P"].fillna(50)
    df["HOARD_P"] = df["HOARD_P"].fillna(50)
    
    # Feature 1: SIS
    df["SIS"] = ((df["BLOCK_P"] + 1)**0.50 * (df["FOOT_P"] + 1)**0.30 * (df["HOARD_P"] + 1)**0.20) - 1
    
    # Feature 2: Whale Density
    df["Whale_Density"] = (df["ATW"] / df["DELIVERY_TURNOVER"].replace(0, np.nan)).fillna(0) * 100000
    
    # Feature 3: Implied Trades
    df["Implied_Trades"] = (df["DELIVERY_TURNOVER"] / df["ATW"].replace(0, np.nan)).fillna(0)
    
    return df

def run_ablation(df):
    target = "IS_PROFITABLE"
    
    tests = {
        "Test A (SIS alone)": ["SIS"],
        "Test B (Whale Density alone)": ["Whale_Density"],
        "Test C (SIS + Whale Density)": ["SIS", "Whale_Density"],
        "Test D (SIS + Implied Trades)": ["SIS", "Implied_Trades"],
        "Test E (SIS + Whale Density + Implied Trades)": ["SIS", "Whale_Density", "Implied_Trades"]
    }
    
    skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    
    results = []
    
    for test_name, features in tests.items():
        X = df[features]
        y = df[target]
        
        accs, precs, f1s = [], [], []
        
        for train_idx, test_idx in skf.split(X, y):
            X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
            y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]
            
            clf = RandomForestClassifier(n_estimators=100, max_depth=4, random_state=42)
            clf.fit(X_train, y_train)
            
            preds = clf.predict(X_test)
            accs.append(accuracy_score(y_test, preds))
            precs.append(precision_score(y_test, preds, zero_division=0))
            f1s.append(f1_score(y_test, preds, zero_division=0))
            
        results.append({
            "Test": test_name,
            "Accuracy": np.mean(accs),
            "Precision": np.mean(precs),
            "F1-Score": np.mean(f1s)
        })
        
    return pd.DataFrame(results)

def calculate_vif(df):
    features = ["SIS", "Whale_Density", "Implied_Trades"]
    X = df[features]
    vif_data = pd.DataFrame()
    vif_data["Feature"] = features
    vif_data["VIF"] = [variance_inflation_factor(X.values, i) for i in range(len(features))]
    
    corr = X.corr().round(3)
    return vif_data, corr

def generate_shap(df):
    features = ["SIS", "Whale_Density", "Implied_Trades"]
    X = df[features]
    y = df["IS_PROFITABLE"]
    
    clf = RandomForestClassifier(n_estimators=100, max_depth=4, random_state=42)
    clf.fit(X, y)
    
    explainer = shap.TreeExplainer(clf)
    shap_values = explainer.shap_values(X)
    
    if isinstance(shap_values, list):
        # Older SHAP returns a list
        sv = shap_values[1]
    elif len(shap_values.shape) == 3:
        # Newer SHAP returns a 3D array (n_samples, n_features, n_classes)
        sv = shap_values[:, :, 1]
    else:
        sv = shap_values
    
    sv_df = pd.DataFrame(sv, columns=features)
    
    rules = []
    for feat in features:
        # Determine SHAP impact correlation
        # We can look at the 75th percentile of the feature values when SHAP > 0
        positive_impact_idx = sv_df[sv_df[feat] > 0].index
        if len(positive_impact_idx) > 0:
            median_val_positive = X.loc[positive_impact_idx, feat].median()
            rules.append(f"When {feat} is > {median_val_positive:.2f}, the model strongly associates it with a winning trade (Positive SHAP impact).")
            
    return rules

def main():
    print("Running Ablation Study...")
    df_pct = calculate_market_percentiles()
    df = prep_dataset(df_pct)
    
    df_results = run_ablation(df)
    vif_data, corr = calculate_vif(df)
    shap_rules = generate_shap(df)
    
    print("\n--- RESULTS ---")
    print(df_results)
    
    print("\n--- VIF ---")
    print(vif_data)
    
    with open("ablation_output.json", "w") as f:
        json.dump({
            "results": df_results.to_dict(orient="records"),
            "vif": vif_data.to_dict(orient="records"),
            "corr": corr.to_dict(),
            "shap_rules": shap_rules
        }, f)
        
    print("Done!")

if __name__ == "__main__":
    main()
