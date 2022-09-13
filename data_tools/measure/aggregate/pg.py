import os
import glob
import csv
import psycopg

from measure import _collect_time
from settings import PG_URI
from measure.aggregate import AggregateMixin


class Postgres(AggregateMixin):
    def __init__(self, report_dir, explain=False):
        self.report_dir = report_dir
        self.explain = explain
        self.times = []
        self.conn = psycopg.connect(PG_URI)
        self.cur = self.conn.cursor()

    def _create_entry(self, start_date, end_date, label, resp=None, spent_time=None):
        return {
            'label': label,
            'start_date': start_date,
            'result': resp[0][0],
            'spent_time': spent_time,
        }

    @_collect_time(_create_entry)
    def query(self, start_date, end_date, label):
        sql = '''
            select avg(int_field) from entry where date_field >= %s and date_field <= %s
        '''
        if self.explain:
            sql = f'explain {sql}'
        self.cur.execute(sql, (start_date.date(), end_date.date()))
        return self.cur.fetchall()

    def _get_explain_glob(self):
        return f'{self.report_dir}/pg.aggs.*.txt'

    def _get_time_report_path(self):
        return f'{self.report_dir}/pg.aggs.csv'

    def _report_explain(self, label, start_date, content):
        with open(f'{self.report_dir}/pg.aggs.{label}.{start_date}.txt', 'w') as f:
            for (line,) in content:
                f.write(f'{line}\n')

    def clear(self):
        self.cur.close()
        self.conn.close()
