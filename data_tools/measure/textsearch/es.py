import glob
import os
import json
import csv
from elasticsearch import Elasticsearch as ElasticsearchClient

from settings import ES_URI
from measure.textsearch import TextsearchMixin
from measure import _collect_time


class Elasticsearch(TextsearchMixin):
    def __init__(self, report_dir, explain=False):
        self.es = ElasticsearchClient(ES_URI)
        self.report_dir = report_dir
        self.explain = explain
        self.times = []

    def _create_entry(self, text, resp=None, spent_time=None):
        return {
            'id': resp["hits"]["hits"][0]['_id'],
            'spent_time': spent_time,
        }

    @_collect_time(_create_entry)
    def query(self, text):
        kwargs = {
            "index": "entry",
            "query": {
                "match": {
                    "text_field": text
                }
            },
            "profile": self.explain,
            "sort": [
                "_score"
            ]
        }
        return self.es.search(**kwargs)

    def _get_explain_glob(self):
        return f'{self.report_dir}/es.ts.*.txt'

    def _get_time_report_path(self):
        return f'{self.report_dir}/es.ts.csv'

    def _report_explain(self, id, content):
        with open(f'{self.report_dir}/es.ts.{id}.txt', 'w') as f:
            f.write(json.dumps(content, indent=4))
