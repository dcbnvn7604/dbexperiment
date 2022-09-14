from datetime import timedelta
from collections import defaultdict
import arrow
from arrow.arrow import Arrow
import random

import argparse
from os import path, mkdir

from settings import DATE_START, DATE_END, ROUND
from measure.aggregate.es import Elasticsearch as ESAggregateMeasurer
from measure.aggregate.pg import Postgres as PGAggregateMeasurer
from measure.aggregate.mg import Mongo as MGAggregateMeasurer
from measure.textsearch import make_query_parameters
from measure.textsearch.es import ES as ESTextSearchMeasurer
from measure.textsearch.pg import PG as PGTextSearchMeasurer
from measure.textsearch.mg import Mongo as MGTextSearchMeasurer


REPORT_DIR = '_report'


def main():
    parser = argparse.ArgumentParser(description='Measure')
    parser.add_argument('-t', dest='topic', required=True, choices=['aggs', 'ts'], help='topic')
    parser.add_argument('-e', help='explain', dest='explain', action='store_const', const=True, default=False)
    parser.add_argument('db_type', nargs='+', help='db_type to measure', choices=['es', 'pg', 'mg'])

    args = parser.parse_args()

    if not path.exists(REPORT_DIR):
        mkdir(REPORT_DIR)

    if args.topic == 'aggs':
        measure_aggregation(args)
    if args.topic == 'ts':
        measure_textsearch(args)


def measure_aggregation(args):
    start_date = arrow.get(DATE_START)
    end_date = arrow.get(DATE_END)
    deltas = {
        "1_day": timedelta(days=1),
        "1_week": timedelta(days=7),
        '1_month': timedelta(days=30),
        '1_year': timedelta(days=365),
    }

    start_dates = defaultdict(set)
    _round = ROUND if not args.explain else 1
    for (label, delta) in deltas.items():
        for i in range(_round):
            start_int = start_date.int_timestamp
            end_int = (end_date - delta).int_timestamp
            rand_date = Arrow.fromtimestamp(random.randint(start_int, end_int))
            start_dates[label].add(rand_date)

    if 'es' in args.db_type:
        es_measurer = ESAggregateMeasurer(REPORT_DIR, explain=args.explain)
        es_measurer.measure(start_dates, deltas)
    if 'pg' in args.db_type:
        pg_measurer = PGAggregateMeasurer(REPORT_DIR, explain=args.explain)
        pg_measurer.measure(start_dates, deltas)
        pg_measurer.clear()
    if 'mg' in args.db_type:
        mg_measurer = MGAggregateMeasurer(REPORT_DIR, explain=args.explain)
        mg_measurer.measure(start_dates, deltas)


def measure_textsearch(args):
    parameters = make_query_parameters(args.explain)
    if 'es' in args.db_type:
        es_measurer = ESTextSearchMeasurer(REPORT_DIR, explain=args.explain)
        es_measurer.measure(parameters)
    if 'pg' in args.db_type:
        pg_measurer = PGTextSearchMeasurer(REPORT_DIR, explain=args.explain)
        pg_measurer.measure(parameters)
        pg_measurer.clear()
    if 'mg' in args.db_type:
        mg_measurer = MGTextSearchMeasurer(REPORT_DIR, explain=args.explain)
        mg_measurer.measure(parameters)


if __name__ == '__main__':
    main()
