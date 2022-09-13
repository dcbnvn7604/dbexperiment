import json
import os
import csv
import glob
from elasticsearch import Elasticsearch as ElasticsearchClient

from measure import _collect_time
from measure.aggregate import AggregateMixin
from settings import ES_URI


class Elasticsearch(AggregateMixin):
    def __init__(self, report_dir, explain=False):
        self.es = ElasticsearchClient(ES_URI)
        self.report_dir = report_dir
        self.explain = explain
        self.times = []

    def _create_entry(self, start_date, end_date, label, resp=None, spent_time=None):
        return {
            'label': label,
            'start_date': start_date,
            'result': resp["aggregations"]["avg_int_field"]["value"],
            'spent_time': spent_time,
        }

    @_collect_time(_create_entry)
    def query(self, start_date, end_date, label):
        kwargs = {
            "index": "entry",
            "query": { "range": { "date_field": { "gte": start_date.date(), "lte": end_date.date() } } },
            "aggs": { "avg_int_field": { "avg" : { "field": "int_field"} } },
            "profile": self.explain,

        }
        if not self.explain:
            kwargs["size"] = 0
        return self.es.search(**kwargs)

    def _get_explain_glob(self):
        return f'{self.report_dir}/es.aggs.*.txt'

    def _get_time_report_path(self):
        return f'{self.report_dir}/es.aggs.csv'

    def _report_explain(self, label, start_date, content):
        with open(f'{self.report_dir}/es.aggs.{label}.{start_date}.txt', 'w') as f:
            f.write(json.dumps(content, indent=4))
