import json
import os
import glob
from elasticsearch import Elasticsearch


class ES():
    def __init__(self, report_dir, explain=False):
        self.es = Elasticsearch('http://elasticsearch:9200')
        self.report_dir = report_dir
        self.explain = explain

    def measure(self, start_dates, deltas):
        self._clear_reports()
        for (label, _start_dates) in start_dates.items():
            for start_date in _start_dates:
                resp = self.query(start_date.date(), (start_date + deltas[label]).date(), label=label)
                if self.explain:
                    self._report(label, start_date, resp)

    def query(self, start_date, end_date, label=None):
        return self.es.search(index="books",
            query={ "range": { "publication_date": { "gte": start_date, "lte": end_date} } },
            aggs={ "avg_price": { "avg" : { "field": "price"} } },
            profile=self.explain
        )

    def _clear_reports(self):
        files = glob.glob(f'{self.report_dir}/es.*.txt')
        for file in files:
            os.remove(file)

    def _report(self, label, start_date, content):
        with open(f'{self.report_dir}/es.{label}.{start_date}.txt', 'w') as f:
            f.write(json.dumps(content, indent=4))
