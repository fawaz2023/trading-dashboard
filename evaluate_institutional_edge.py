import pandas as pd
import numpy as np

def evaluate_edge():
    print("=" * 70)
    print("INSTITUTIONAL EDGE EVALUATOR (FULL UNIVERSE)")
    print("=" * 70)

    try:
        df = pd.read_csv("data/historical_full_universe.csv")
    except FileNotFoundError:
        print("❌ data/historical_full_universe.csv not found. Run build_historical_features.py first.")
        return

    df["DATE"] = pd.to_datetime(df["DATE"])
    df = df.sort_values(["SYMBOL", "DATE"]).reset_index(drop=True)

    print(f"Loaded {len(df)} historical rows across {df['SYMBOL'].nunique()} symbols.")

    # Calculate forward returns
    print("Calculating forward returns (T+5, T+10, T+20)...")
    grouped = df.groupby("SYMBOL")
    df["RET_T5"] = grouped["CLOSE"].shift(-5) / df["CLOSE"] - 1
    df["RET_T10"] = grouped["CLOSE"].shift(-10) / df["CLOSE"] - 1
    df["RET_T20"] = grouped["CLOSE"].shift(-20) / df["CLOSE"] - 1

    # Drop rows where we don't have enough future data to calculate the return
    df = df.dropna(subset=["RET_T5", "RET_T10", "RET_T20"])
    
    # Calculate daily universe mean return per exchange
    print("Calculating excess returns vs daily exchange universe...")
    day_exch_mean = df.groupby(["DATE", "EXCHANGE"])[["RET_T5", "RET_T10", "RET_T20"]].transform("mean")
    df["EXCESS_T5"] = df["RET_T5"] - day_exch_mean["RET_T5"]
    df["EXCESS_T10"] = df["RET_T10"] - day_exch_mean["RET_T10"]
    df["EXCESS_T20"] = df["RET_T20"] - day_exch_mean["RET_T20"]
    df["avg_excess_vs_day_universe"] = (df["EXCESS_T5"] + df["EXCESS_T10"] + df["EXCESS_T20"]) / 3.0

    results = []

    for exchange in ["NSE", "BSE"]:
        df_ex = df[df["EXCHANGE"] == exchange].copy()
        if df_ex.empty:
            continue
            
        print(f"\nProcessing {exchange} ({len(df_ex)} rows)...")

        # Create Fixed Bands
        df_ex["BAND"] = pd.cut(df_ex["COMBINEDSCORE"], bins=[-np.inf, 40, 60, 80, np.inf], labels=["<40", "40-60", "60-80", "80+"])
        
        # Create Deciles (0-100 score divided into 10 buckets)
        # Using qcut ensures equal-sized buckets if the distribution is skewed
        try:
            df_ex["DECILE"] = pd.qcut(df_ex["COMBINEDSCORE"], q=10, labels=[f"D{i}" for i in range(1, 11)])
        except ValueError:
            # Fallback if qcut fails due to identical edges
            df_ex["DECILE"] = pd.cut(df_ex["COMBINEDSCORE"], bins=10, labels=[f"D{i}" for i in range(1, 11)])
            
        # Optional 0-3 Compression mapping
        df_ex["COMPRESSED_0_3"] = pd.cut(df_ex["COMBINEDSCORE"], bins=[-np.inf, 25, 50, 75, np.inf], labels=["0", "1", "2", "3"])

        # Define metric aggregation function
        def aggregate_metrics(group_df, group_name, group_type):
            n_obs = len(group_df)
            if n_obs == 0: return None
            
            return {
                "EXCHANGE": exchange,
                "EVAL_TYPE": group_type,
                "BUCKET": group_name,
                "n_obs": n_obs,
                "hit_rate_gt_0_t5": (group_df["RET_T5"] > 0).mean() * 100,
                "hit_rate_gt_0_t10": (group_df["RET_T10"] > 0).mean() * 100,
                "hit_rate_gt_0_t20": (group_df["RET_T20"] > 0).mean() * 100,
                "avg_return_t5": group_df["RET_T5"].mean() * 100,
                "avg_return_t10": group_df["RET_T10"].mean() * 100,
                "avg_return_t20": group_df["RET_T20"].mean() * 100,
                "median_return_t5": group_df["RET_T5"].median() * 100,
                "median_return_t10": group_df["RET_T10"].median() * 100,
                "median_return_t20": group_df["RET_T20"].median() * 100,
                "avg_excess_vs_day_universe": group_df["avg_excess_vs_day_universe"].mean() * 100,
                "coverage_valid_momentum": group_df["HASMOMENTUMDATA"].mean() * 100,
                "coverage_valid_footprint": group_df["HASFOOTPRINTDATA"].mean() * 100,
                "coverage_valid_stability": group_df["HASSTABILITYHISTORY20D"].mean() * 100
            }

        # Evaluate Deciles
        for d in [f"D{i}" for i in range(1, 11)]:
            res = aggregate_metrics(df_ex[df_ex["DECILE"] == d], d, "1_DECILE")
            if res: results.append(res)
            
        # Evaluate Fixed Bands
        for b in ["<40", "40-60", "60-80", "80+"]:
            res = aggregate_metrics(df_ex[df_ex["BAND"] == b], b, "2_FIXED_BAND")
            if res: results.append(res)
            
        # Evaluate Compressed 0-3
        for c in ["0", "1", "2", "3"]:
            res = aggregate_metrics(df_ex[df_ex["COMPRESSED_0_3"] == c], c, "3_COMPRESSED_0_3")
            if res: results.append(res)

    df_res = pd.DataFrame(results)
    
    # Sort for logical reading
    df_res = df_res.sort_values(["EXCHANGE", "EVAL_TYPE", "BUCKET"])
    
    # Format floats for readability
    float_cols = [c for c in df_res.columns if c not in ["EXCHANGE", "EVAL_TYPE", "BUCKET", "n_obs"]]
    df_res[float_cols] = df_res[float_cols].round(2)
    
    out_file = "data/institutional_edge_report.csv"
    df_res.to_csv(out_file, index=False)
    
    print(f"\n✅ SUCCESS: Evaluated edge and saved report to {out_file}")

if __name__ == "__main__":
    evaluate_edge()
