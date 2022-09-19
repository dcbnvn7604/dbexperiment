import psycopg
import geojson

from measure import _collect_time
from settings import PG_URI
from measure.geo import GeoMixin

class Postgre(GeoMixin):
    def __init__(self, report_dir, explain=False):
        self.report_dir = report_dir
        self.explain = explain
        self.times = []
        self.conn = psycopg.connect(PG_URI)
        self.cur = self.conn.cursor()

    def _create_entry(self, point, resp=None, spent_time=None):
        result = '|'.join(sorted([
            str(item[0])
            for item in resp
        ])[:5])
        return {
            'point': point,
            'result': result,
            'spent_time': spent_time,
        }

    @_collect_time(_create_entry)
    def query(self, point):
        sql = '''
            select uuid from entry order by point_field <-> %s asc limit 2
        '''
        if self.explain:
            sql = f'explain {sql}'
        self.cur.execute(sql, (geojson.dumps(geojson.Point((float(point[1]), float(point[0])))),))
        return self.cur.fetchall()

    def _get_explain_glob(self):
        return f'{self.report_dir}/pg.geo.*.txt'

    def _get_time_report_path(self):
        return f'{self.report_dir}/pg.geo.csv'

    def _report_explain(self, point, content):
        with open(f'{self.report_dir}/pg.geo.{point[0]}_{point[1]}.txt', 'w') as f:
            for (line,) in content:
                f.write(f'{line}\n')

    def clear(self):
        self.cur.close()
        self.conn.close()
