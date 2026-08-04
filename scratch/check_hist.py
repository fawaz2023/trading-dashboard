import pandas as pd
df = pd.read_csv('data/signal_scores_history.csv')
print(f'Total signals: {len(df)}')
print(f'Min date: {df["DATE"].min()}')
print(f'Max date: {df["DATE"].max()}')
