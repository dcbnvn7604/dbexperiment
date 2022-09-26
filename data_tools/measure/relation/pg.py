import psycopg

from settings import PG_URI
from measure import _collect_time
from measure.relation import RelationMixin

class Postgre(RelationMixin):
    def __init__(self, report_dir, explain=False):
        self.report_dir = report_dir
        self.explain = explain
        self.times = []
        self.conn = psycopg.connect(PG_URI)
        self.cur = self.conn.cursor()

    def _create_entry(self, parameter, resp=None, spent_time=None):
        result = '|'.join(sorted([
            str(item[0])
            for item in resp
        ]))
        return {
            'uuids': f'{parameter[0]}_{parameter[1]}',
            'length': parameter[2],
            'result': result,
            'spent_time': spent_time,
        }

    @_collect_time(_create_entry)
    def query(self, parameter):
        sql = '''
            select er0.uuid2
            from entry_rel as er0
        '''
        for length in range(parameter[2]):
            sql = '''
                %s
                inner join entry_rel as er%s on er%s.uuid1 = er%s.uuid2
            ''' % (sql, length+1, length+1, length)
        sql = '''
            %s
            where er0.uuid1 = '%s'
            and er%s.uuid2 = '%s'
        ''' % (sql, parameter[0], parameter[2], parameter[1])
        if self.explain:
            sql = f'explain {sql}'
        self.cur.execute(sql)
        return self.cur.fetchall()

    def _report_explain(self, id, content):
        with open(f'{self.report_dir}/pg.ts.{id}.txt', 'w') as f:
            for (line,) in content:
                f.write(f'{line}\n')

    def _get_explain_glob(self):
        return f'{self.report_dir}/rel.pg.*.txt'

    def _get_time_report_path(self):
        return f'{self.report_dir}/rel.pg.csv'

    def _report_explain(self, parameter, content):
        with open(f'{self.report_dir}/rel.pg.{parameter[0]}_{parameter[1]}_{parameter[2]}.txt', 'w') as f:
            for (line,) in content:
                f.write(f'{line}\n')

    def clear(self):
        self.cur.close()
        self.conn.close()
