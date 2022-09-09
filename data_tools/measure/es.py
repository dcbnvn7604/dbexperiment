import json
import os
import csv
import glob
from elasticsearch import Elasticsearch

from measure import _collect_time


class ES():
    def __init__(self, report_dir, explain=False):
        self.es = Elasticsearch('http://elasticsearch:9200')
        self.report_dir = report_dir
        self.explain = explain
        self.times = []

    def measure(self, start_dates, deltas):
        if self.explain:
            self._clear_explain()
        for (label, _start_dates) in start_dates.items():
            for start_date in _start_dates:
                resp = self.query(start_date.date(), (start_date + deltas[label]).date(), label)
                
                if self.explain:
                    self._report_explain(label, start_date, resp)
        if not self.explain:
            self._report_time()

    def _create_entry(self, start_date, end_date, label, resp=None, spent_time=None):
        return {
            'label': label,
            'start_date': start_date,
            'result': resp["aggregations"]["avg_price"]["value"],
            'spent_time': spent_time,
        }

    @_collect_time(_create_entry)
    def query(self, start_date, end_date, label):
        kwargs = {
            "index": "books",
            "query": { "range": { "publication_date": { "gte": start_date, "lte": end_date} } },
            "aggs": { "avg_price": { "avg" : { "field": "price"} } },
            "profile": self.explain,

        }
        if not self.explain:
            kwargs["size"] = 0
        return self.es.search(**kwargs)

    def _clear_explain(self):
        files = glob.glob(f'{self.report_dir}/es.aggs.*.txt')
        for file in files:
            os.remove(file)

    def _report_explain(self, label, start_date, content):
        with open(f'{self.report_dir}/es.aggs.{label}.{start_date}.txt', 'w') as f:
            f.write(json.dumps(content, indent=4))

    def _report_time(self):
        with open(f'{self.report_dir}/es.aggs.csv', 'w') as f:
            fields = ["label", "start_date", "result", "spent_time"]
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            for entry in self.times:
                writer.writerow(entry)
