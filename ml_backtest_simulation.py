import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import StratifiedKFold, cross_val_predict

def run_ml_backtest():
    print("Loading historical data (N=82)...")
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
    
    features = ['SIS', 'Whale_Density', 'Implied_Trades']
    target = 'IS_PROFITABLE'
    
    # Drop rows missing critical features
    df = df.dropna(subset=features + ['ROI_PCT', target])
    
    X = df[features]
    y = df[target]
    
    print("Running Stratified 5-Fold Cross-Validation Predictions...")
    model = RandomForestClassifier(n_estimators=100, class_weight='balanced', max_depth=4, random_state=42)
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    
    # Get OUT-OF-SAMPLE predicted probabilities for class 1
    y_pred_proba = cross_val_predict(model, X, y, cv=cv, method='predict_proba')[:, 1]
    
    df['AI_WIN_PROBABILITY'] = y_pred_proba * 100
    
    print("Filtering for AI_WIN_PROBABILITY >= 60.0%...")
    executed_trades = df[df['AI_WIN_PROBABILITY'] >= 60.0].copy()
    
    # Metrics Calculation
    total_executed = len(executed_trades)
    
    if total_executed > 0:
        wins = len(executed_trades[executed_trades['ROI_PCT'] > 0])
        win_rate = (wins / total_executed) * 100
        
        investment_per_trade = 5000
        capital_deployed = total_executed * investment_per_trade
        
        executed_trades['Simulated_PNL'] = investment_per_trade * (executed_trades['ROI_PCT'] / 100)
        total_simulated_pnl = executed_trades['Simulated_PNL'].sum()
        
        portfolio_return = (total_simulated_pnl / capital_deployed) * 100
        
        best_trade = executed_trades.loc[executed_trades['ROI_PCT'].idxmax()]
        worst_trade = executed_trades.loc[executed_trades['ROI_PCT'].idxmin()]
        
        print("\n=========================================")
        print("🤖 OUT-OF-SAMPLE DYNAMIC BACKTEST RESULTS")
        print("=========================================")
        print(f"Total Executed Trades: {total_executed} (out of {len(df)} total signals)")
        print(f"Win Rate: {win_rate:.2f}% ({wins} Wins, {total_executed - wins} Losses)")
        print(f"Highest Profit: {best_trade['ROI_PCT']:.2f}% on {best_trade['SYMBOL']}")
        print(f"Highest Loss: {worst_trade['ROI_PCT']:.2f}% on {worst_trade['SYMBOL']}")
        print("-----------------------------------------")
        print(f"Capital Deployed: ₹{capital_deployed:,.2f}")
        print(f"Total Simulated P&L: ₹{total_simulated_pnl:,.2f}")
        print(f"Overall Portfolio Return: {portfolio_return:.2f}%")
        print("=========================================\n")
    else:
        print("No trades met the 60.0% probability threshold out-of-sample.")

if __name__ == "__main__":
    run_ml_backtest()
