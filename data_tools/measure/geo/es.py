import json
import geojson
from elasticsearch import Elasticsearch as ElasticsearchClient

from settings import ES_URI
from measure.geo import GeoMixin
from measure import _collect_time


class Elasticsearch(GeoMixin):
    def __init__(self, report_dir, explain=False):
        self.es = ElasticsearchClient(ES_URI)
        self.report_dir = report_dir
        self.explain = explain
        self.times = []

    def _create_entry(self, point, resp=None, spent_time=None):
        result = '|'.join(sorted([
            str(item['_id'])
            for item in resp["hits"]["hits"]
        ]))
        return {
            'point': point,
            'result': result,
            'spent_time': spent_time,
        }

    @_collect_time(_create_entry)
    def query(self, point):
        kwargs = {
            "index": "entry",
            "from_": 0,
            "size": 2,
            "profile": self.explain,
            "sort": [{
                "_geo_distance": {
                    "point_field": [point[1], point[0]],
                    "order": "asc"
                }
            }]
        }
        return self.es.search(**kwargs)

    def _get_explain_glob(self):
        return f'{self.report_dir}/es.geo.*.txt'

    def _get_time_report_path(self):
        return f'{self.report_dir}/es.geo.csv'

    def _report_explain(self, point, content):
        with open(f'{self.report_dir}/es.geo.{point[0]}_{point[1]}.txt', 'w') as f:
            f.write(json.dumps(content, indent=4))
