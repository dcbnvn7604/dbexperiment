import argparse
import pandas as pd
import csv


REPORT_DIR = '_report'


def main():
    parser = argparse.ArgumentParser(description='Report')
    parser.add_argument('-t', dest='topic', required=True, choices=['aggs', 'ts', 'join', 'geo'], help='topic')
    args = parser.parse_args()

    if args.topic == 'aggs':
        report_aggregation()
    if args.topic == 'ts':
        report_textsearch()
    if args.topic == 'join':
        report_join()
    if args.topic == 'geo':
        report_geo()

def report_aggregation():
    es_df = pd.read_csv(f'{REPORT_DIR}/es.aggs.csv') \
        .rename(columns={'spent_time': 'es_spent_time'}) \
        .groupby('label')['es_spent_time'] \
        .mean()
    pg_df = pd.read_csv(f'{REPORT_DIR}/pg.aggs.csv') \
        .rename(columns={'spent_time': 'pg_spent_time'}) \
        .groupby('label')['pg_spent_time'] \
        .mean()
    mg_df = pd.read_csv(f'{REPORT_DIR}/mg.aggs.csv') \
        .rename(columns={'spent_time': 'mg_spent_time'}) \
        .groupby('label')['mg_spent_time'] \
        .mean()
    df = es_df.to_frame().merge(pg_df, on='label') \
        .merge(mg_df, on='label')
    df['es/pg'] = df['es_spent_time'] / df['pg_spent_time']
    df['es/mg'] = df['es_spent_time'] / df['mg_spent_time']
    df['pg/mg'] = df['pg_spent_time'] / df['mg_spent_time']
    df.to_csv(f'{REPORT_DIR}/all.aggs.csv')


def report_textsearch():
    es_spent_time = pd.read_csv(f'{REPORT_DIR}/es.ts.csv') \
        .rename(columns={'spent_time': 'es_spent_time'})['es_spent_time'] \
        .mean()
    pg_spent_time = pd.read_csv(f'{REPORT_DIR}/pg.ts.csv') \
        .rename(columns={'spent_time': 'pg_spent_time'})['pg_spent_time'] \
        .mean()
    es_pg = es_spent_time / pg_spent_time
    df = pd.DataFrame(data={
        'es_spent_time': [es_spent_time],
        'pg_spent_time': [pg_spent_time],
        'es/pg': [es_pg],
    })
    df.to_csv(f'{REPORT_DIR}/all.ts.csv')


def report_join():
    pg_df = pd.read_csv(f'{REPORT_DIR}/pg.join.csv') \
        .rename(columns={'spent_time': 'pg_spent_time'}) \
        .groupby('period')['pg_spent_time'] \
        .mean()
    mg_df = pd.read_csv(f'{REPORT_DIR}/mg.join.csv') \
        .rename(columns={'spent_time': 'mg_spent_time'}) \
        .groupby('period')['mg_spent_time'] \
        .mean()
    df = pg_df.to_frame().merge(mg_df, on='period')
    df['pg/mg'] = df['pg_spent_time'] / df['mg_spent_time']
    df.to_csv(f'{REPORT_DIR}/all.join.csv')


def report_geo():
    es_spent_time = pd.read_csv(f'{REPORT_DIR}/es.geo.csv') \
        .rename(columns={'spent_time': 'es_spent_time'})['es_spent_time'] \
        .mean()
    pg_spent_time = pd.read_csv(f'{REPORT_DIR}/pg.geo.csv') \
        .rename(columns={'spent_time': 'pg_spent_time'})['pg_spent_time'] \
        .mean()
    mg_spend_time = pd.read_csv(f'{REPORT_DIR}/mg.geo.csv') \
        .rename(columns={'spent_time': 'mg_spent_time'})['mg_spent_time'] \
        .mean()
    df = pd.DataFrame(data={
        'es_spent_time': [es_spent_time],
        'pg_spent_time': [pg_spent_time],
        'mg_spent_time': [mg_spend_time],
        'es/pg': [es_spent_time/pg_spent_time],
        'es/mg': [es_spent_time/mg_spend_time],
        'pg/mg': [pg_spent_time/mg_spend_time],
    })
    df.to_csv(f'{REPORT_DIR}/all.geo.csv')


if __name__ == '__main__':
    main()
