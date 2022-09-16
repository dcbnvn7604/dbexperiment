import psycopg

from measure import _collect_time
from settings import PG_URI
from measure.join import JoinMixin


class Postgre(JoinMixin):
    def __init__(self, report_dir, explain=False):
        self.report_dir = report_dir
        self.explain = explain
        self.times = []
        self.conn = psycopg.connect(PG_URI)
        self.cur = self.conn.cursor()

    def _create_entry(self, period, start_date, end_date, resp=None, spent_time=None):
        result = '|'.join(sorted([
            str(item[0])
            for item in resp
        ])[:5])
        return {
            'period': period,
            'start_date': start_date,
            'result': result,
            'spent_time': spent_time,
        }

    @_collect_time(_create_entry)
    def query(self, period, start_date, end_date):
        sql = '''
            select ec.uuid, e.int_field 
            from entry as e
            inner join entry_child as ec on ec.parrent_uuid = e.uuid
            where ec.date_field >= %s and ec.date_field <= %s
        '''
        if self.explain:
            sql = f'explain {sql}'
        self.cur.execute(sql, (start_date.date(), end_date.date()))
        return self.cur.fetchall()

    def _get_explain_glob(self):
        return f'{self.report_dir}/pg.join.*.txt'

    def _get_time_report_path(self):
        return f'{self.report_dir}/pg.join.csv'

    def _report_explain(self, label, start_date, content):
        with open(f'{self.report_dir}/pg.join.{label}.{start_date}.txt', 'w') as f:
            for (line,) in content:
                f.write(f'{line}\n')

    def clear(self):
        self.cur.close()
        self.conn.close()
