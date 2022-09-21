import json
import geojson
from pymongo import MongoClient

from settings import MG_URI
from measure.geo import GeoMixin
from measure import _collect_time


class Mongo(GeoMixin):
    def __init__(self, report_dir, explain=False):
        self.mg = MongoClient(MG_URI)['dbexperiment']
        self.report_dir = report_dir
        self.explain = explain
        self.times = []

    def _create_entry(self, point, resp=None, spent_time=None):
        result = '|'.join(sorted([
            str(item['uuid'])
            for item in resp["cursor"]["firstBatch"]
        ]))
        return {
            'point': point,
            'result': result,
            'spent_time': spent_time,
        }

    @_collect_time(_create_entry)
    def query(self, point):
        query = {
            'find': 'entry',
            'filter': {
                'mg_point_field': {
                    '$nearSphere': {
                        '$geometry': geojson.Point((float(point[1]), float(point[0])))
                    }
                }
            },
            'limit': 2,
            'allowDiskUse': True
        }
        if self.explain:
            query = {
                'explain': query
            }
        return self.mg.command(command=query)

    def _get_explain_glob(self):
        return f'{self.report_dir}/mg.geo.*.txt'

    def _get_time_report_path(self):
        return f'{self.report_dir}/mg.geo.csv'

    def _report_explain(self, point, content):
        with open(f'{self.report_dir}/mg.geo.{point[0]}_{point[1]}.txt', 'w') as f:
            f.write(json.dumps(content, indent=4))
