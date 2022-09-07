import os
import glob
import psycopg


class PG():
    def __init__(self, report_dir, explain=False):
        self.report_dir = report_dir
        self.explain = explain
        self.conn = psycopg.connect("postgresql://root:abc%401234@postgres/dbexperiment")
        self.cur = self.conn.cursor()

    def measure(self, start_dates, deltas):
        self._clear_reports()
        for (label, _start_dates) in start_dates.items():
            for start_date in _start_dates:
                resp = self.query(start_date.date(), (start_date + deltas[label]).date(), label=label)
                if self.explain:
                    self._report(label, start_date, resp)

    def query(self, start_date, end_date, label=None):
        sql = '''
            select avg(price) from books where publication_date >= %s and publication_date <= %s
        '''
        if self.explain:
            sql = f'explain {sql}'
        self.cur.execute(sql, (start_date, end_date))
        return self.cur.fetchall()

    def _clear_reports(self):
        files = glob.glob(f'{self.report_dir}/pg.*.txt')
        for file in files:
            os.remove(file)

    def _report(self, label, start_date, content):
        with open(f'{self.report_dir}/pg.{label}.{start_date}.txt', 'w') as f:
            for (line,) in content:
                f.write(f'{line}\n')

    def clear(self):
        self.cur.close()
        self.conn.close()
