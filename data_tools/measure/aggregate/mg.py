import json
from pymongo import MongoClient

from settings import MG_URI
from measure.aggregate import AggregateMixin
from measure import _collect_time


class Mongo(AggregateMixin):
    def __init__(self, report_dir, explain=False):
        self.mg = MongoClient(MG_URI)['dbexperiment']
        self.report_dir = report_dir
        self.explain = explain
        self.times = []

    def _create_entry(self, start_date, end_date, label, resp=None, spent_time=None):
        return {
            'label': label,
            'start_date': start_date,
            'result': resp["cursor"]["firstBatch"][0]["avg_int_field"] if resp["cursor"]["firstBatch"] else None,
            'spent_time': spent_time,
        }

    @_collect_time(_create_entry)
    def query(self, start_date, end_date, label):
        query = {
            'aggregate': 'entry',
            'pipeline': [
                { '$match': { 'date_field': { '$gte': start_date.format('YYYY-MM-DD'), '$lte': end_date.format('YYYY-MM-DD') } } },
                { '$group': { '_id' : None, 'avg_int_field': { '$avg': '$price' } } },
            ],
            'cursor': {}
        }
        if self.explain:
            query = {
                'explain': query
            }
        return self.mg.command(command=query)

    def _get_explain_glob(self):
        return f'{self.report_dir}/mg.aggs.*.txt'

    def _get_time_report_path(self):
        return f'{self.report_dir}/mg.aggs.csv'

    def _report_explain(self, label, start_date, content):
        with open(f'{self.report_dir}/mg.aggs.{label}.{start_date}.txt', 'w') as f:
            f.write(json.dumps(content, indent=4))
