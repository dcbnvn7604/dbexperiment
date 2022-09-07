from datetime import timedelta
from collections import defaultdict
import arrow
from arrow.arrow import Arrow
import random

import argparse
from os import path, mkdir

from settings import DATE_START, DATE_END, ROUND
from measure.es import ES as ESMeasurer
from measure.pg import PG as PGMeasurer

parser = argparse.ArgumentParser(description='Measure')
parser.add_argument('-e', help='explain', dest='explain', action='store_const', const=True, default=False)
parser.add_argument('db_type', nargs='+', help='db_type to measure', choices=['es', 'pg'])

args = parser.parse_args()

REPORT_DIR = '_report'
if not path.exists(REPORT_DIR):
    mkdir(REPORT_DIR)

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
    es_measurer = ESMeasurer(REPORT_DIR, explain=args.explain)
    es_measurer.measure(start_dates, deltas)
if 'pg' in args.db_type:
    pg_measurer = PGMeasurer(REPORT_DIR, explain=args.explain)
    pg_measurer.measure(start_dates, deltas)
