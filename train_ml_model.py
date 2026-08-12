import pandas as pd
from sklearn.ensemble import RandomForestClassifier
import joblib
import numpy as np

def train_and_save_model():
    print("Loading historical data...")
    # Load your historical training data (the N=82 dataset)
    df_train = pd.read_csv('data/ml/train.csv')
    df_holdout = pd.read_csv('data/ml/holdout.csv')
    df = pd.concat([df_train, df_holdout], ignore_index=True)

    print("Engineering features (SIS, Whale Density, Implied Trades)...")
    # SIS derived from the existing cross-sectionally ranked scores in the dataset
    df['SIS'] = ((df['STABILITY_SCORE'] + 1)**0.50 * 
                 (df['FOOTPRINT_SCORE'] + 1)**0.30 * 
                 (df['MOMENTUM_SCORE'] + 1)**0.20) - 1
                 
    df['Whale_Density'] = (df['ATW'] / df['DELIVERY_TURNOVER'].replace(0, np.nan)).fillna(0) * 100000
    df['Implied_Trades'] = (df['DELIVERY_TURNOVER'] / df['ATW'].replace(0, np.nan)).fillna(0)

    # Define the exact features
    features = ['SIS', 'Whale_Density', 'Implied_Trades']
    target = 'IS_PROFITABLE' # Assuming 1 is win, 0 is loss

    # Drop any rows with missing data in our critical columns
    df = df.dropna(subset=features + [target])

    X = df[features]
    y = df[target]

    print("Training Random Forest Classifier...")
    # We use 100 trees, balanced class weights, and a fixed random state for reproducibility
    model = RandomForestClassifier(n_estimators=100, class_weight='balanced', max_depth=4, random_state=42)
    model.fit(X, y)

    # Save the trained model to your directory
    model_filename = 'shadow_box_model.pkl'
    joblib.dump(model, model_filename)
    print(f"Model successfully trained and saved as '{model_filename}'!")

if __name__ == "__main__":
    train_and_save_model()
