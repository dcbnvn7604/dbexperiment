import glob
import os
import json
import csv
from elasticsearch import Elasticsearch

from settings import ES_URI
from measure import _collect_time


class ES():
    def __init__(self, report_dir, explain=False):
        self.es = Elasticsearch(ES_URI)
        self.report_dir = report_dir
        self.explain = explain
        self.times = []

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
            'id': resp["hits"]["hits"][0]['_id'],
            'spent_time': spent_time,
        }

    @_collect_time(_create_entry)
    def query(self, text):
        kwargs = {
            "index": "books",
            "query": {
                "match": {
                    "description": text
                }
            },
            "profile": self.explain,
            "sort": [
                "_score"
            ]
        }
        return self.es.search(**kwargs)

    def _report_explain(self, id, content):
        with open(f'{self.report_dir}/es.ts.{id}.txt', 'w') as f:
            f.write(json.dumps(content, indent=4))

    def _clear_explain(self):
        files = glob.glob(f'{self.report_dir}/es.ts.*.txt')
        for file in files:
            os.remove(file)

    def _report_time(self):
        with open(f'{self.report_dir}/es.ts.csv', 'w') as f:
            fields = ["id", "spent_time"]
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            for entry in self.times:
                writer.writerow(entry)
