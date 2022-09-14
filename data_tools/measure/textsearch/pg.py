import os
import glob
import csv
import psycopg

from settings import PG_URI
from measure import _collect_time
from measure.textsearch import TextsearchMixin


class Postgres(TextsearchMixin):
    def __init__(self, report_dir, explain=False):
        self.report_dir = report_dir
        self.explain = explain
        self.times = []
        self.conn = psycopg.connect(PG_URI)
        self.cur = self.conn.cursor()

    def _create_entry(self, text, resp=None, spent_time=None):
        return {
            'id': resp[0][0],
            'spent_time': spent_time,
        }

    @_collect_time(_create_entry)
    def query(self, text):
        sql = '''
            select uuid, ts_rank(to_tsvector('english', text_field), plainto_tsquery('english', %s)) as rank
            from entry
            where to_tsvector('english', text_field) @@ plainto_tsquery('english', %s)
            order by rank desc
        '''
        if self.explain:
            sql = f'explain {sql}'
        self.cur.execute(sql, (text, text))
        return self.cur.fetchall()

    def _report_explain(self, id, content):
        with open(f'{self.report_dir}/pg.ts.{id}.txt', 'w') as f:
            for (line,) in content:
                f.write(f'{line}\n')

    def _get_explain_glob(self):
        return f'{self.report_dir}/pg.ts.*.txt'

    def _get_time_report_path(self):
        return f'{self.report_dir}/pg.ts.csv'

    def clear(self):
        self.cur.close()
        self.conn.close()
