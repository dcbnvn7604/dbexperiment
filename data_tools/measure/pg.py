import os
import glob
import csv
import psycopg

from measure import _collect_time


class PG():
    def __init__(self, report_dir, explain=False):
        self.report_dir = report_dir
        self.explain = explain
        self.times = []
        self.conn = psycopg.connect("postgresql://root:abc%401234@postgres/dbexperiment")
        self.cur = self.conn.cursor()

    def measure(self, start_dates, deltas):
        if self.explain:
            self._clear_explain()
        for (label, _start_dates) in start_dates.items():
            for start_date in _start_dates:
                resp = self.query(start_date.date(), (start_date + deltas[label]).date(), label=label)
                if self.explain:
                    self._report_explain(label, start_date, resp)
        if not self.explain:
            self._report_time()

    def _create_entry(self, start_date, end_date, label=None, resp=None, spent_time=None):
        return {
            'label': label,
            'start_date': start_date,
            'result': resp[0][0],
            'spent_time': spent_time,
        }

    @_collect_time(_create_entry)
    def query(self, start_date, end_date, label=None):
        sql = '''
            select avg(price) from books where publication_date >= %s and publication_date <= %s
        '''
        if self.explain:
            sql = f'explain {sql}'
        self.cur.execute(sql, (start_date, end_date))
        return self.cur.fetchall()

    def _clear_explain(self):
        files = glob.glob(f'{self.report_dir}/pg.*.txt')
        for file in files:
            os.remove(file)

    def _report_explain(self, label, start_date, content):
        with open(f'{self.report_dir}/pg.{label}.{start_date}.txt', 'w') as f:
            for (line,) in content:
                f.write(f'{line}\n')

    def _report_time(self):
        with open(f'{self.report_dir}/pg.csv', 'w') as f:
            fields = ["label", "start_date", "result", "spent_time"]
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            for entry in self.times:
                writer.writerow(entry)

    def clear(self):
        self.cur.close()
        self.conn.close()
