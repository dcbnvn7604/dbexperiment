import pandas as pd


es_df = pd.read_csv('_report/es.csv') \
    .rename(columns={'spent_time': 'es_spent_time'}) \
    .groupby('label')['es_spent_time'] \
    .mean()
pg_df = pd.read_csv('_report/pg.csv') \
    .rename(columns={'spent_time': 'pg_spent_time'}) \
    .groupby('label')['pg_spent_time'] \
    .mean()
df = es_df.to_frame().merge(pg_df, on='label')
df['es/pg'] = df['es_spent_time'] / df['pg_spent_time']
df.to_csv('_report/all.csv')