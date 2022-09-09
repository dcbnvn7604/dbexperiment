import argparse
import pandas as pd
import csv


REPORT_DIR = '_report'


def main():
    parser = argparse.ArgumentParser(description='Report')
    parser.add_argument('-t', dest='topic', required=True, choices=['aggs', 'ts'], help='topic')
    args = parser.parse_args()

    if args.topic == 'aggs':
        report_aggregation()
    if args.topic == 'ts':
        report_textsearch()


def report_aggregation():
    es_df = pd.read_csv(f'{REPORT_DIR}/es.aggs.csv') \
        .rename(columns={'spent_time': 'es_spent_time'}) \
        .groupby('label')['es_spent_time'] \
        .mean()
    pg_df = pd.read_csv(f'{REPORT_DIR}/pg.aggs.csv') \
        .rename(columns={'spent_time': 'pg_spent_time'}) \
        .groupby('label')['pg_spent_time'] \
        .mean()
    df = es_df.to_frame().merge(pg_df, on='label')
    df['es/pg'] = df['es_spent_time'] / df['pg_spent_time']
    df.to_csv(f'{REPORT_DIR}/all.aggs.csv')


def report_textsearch():
    es_spent_time = pd.read_csv(f'{REPORT_DIR}/es.ts.csv') \
        .rename(columns={'spent_time': 'es_spent_time'})['es_spent_time'] \
        .mean()
    pg_spent_time = pd.read_csv(f'{REPORT_DIR}/pg.ts.csv') \
        .rename(columns={'spent_time': 'pg_spent_time'})['pg_spent_time'] \
        .mean()
    ratio = es_spent_time / pg_spent_time
    df = pd.DataFrame(data={'es_spent_time': [es_spent_time], 'pg_spent_time': [pg_spent_time], 'es/pg': [ratio]})
    df.to_csv(f'{REPORT_DIR}/all.ts.csv')


if __name__ == '__main__':
    main()
