import os
import glob
import csv
import psycopg

from settings import PG_URI
from measure import _collect_time


class PG():
    def __init__(self, report_dir, explain=False):
        self.report_dir = report_dir
        self.explain = explain
        self.times = []
        self.conn = psycopg.connect(PG_URI)
        self.cur = self.conn.cursor()

    def measure(self, parameters):
        if self.explain:
            self._clear_explain()
        for (id, text) in parameters:
            resp = self.query(text)
            if self.explain:
                self._report_explain(id, resp)
        if not self.explain:
            self._report_time()

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

    def _clear_explain(self):
        files = glob.glob(f'{self.report_dir}/pg.ts.*.txt')
        for file in files:
            os.remove(file)

    def _report_time(self):
        with open(f'{self.report_dir}/pg.ts.csv', 'w') as f:
            fields = ["id", "spent_time"]
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            for entry in self.times:
                writer.writerow(entry)

    def clear(self):
        self.cur.close()
        self.conn.close()
