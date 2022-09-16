import json
from pymongo import MongoClient

from settings import MG_URI
from measure.join import JoinMixin
from measure import _collect_time

class Mongo(JoinMixin):
    def __init__(self, report_dir, explain=False):
        self.mg = MongoClient(MG_URI)['dbexperiment']
        self.report_dir = report_dir
        self.explain = explain
        self.times = []

    def _create_entry(self, period, start_date, end_date, resp=None, spent_time=None):
        result = '|'.join(sorted([
            item['uuid']
            for item in resp["cursor"]["firstBatch"]
        ])[:5])
        return {
            'period': period,
            'start_date': start_date,
            'result': result,
            'spent_time': spent_time,
        }

    @_collect_time(_create_entry)
    def query(self, period, start_date, end_date):
        query = {
            'aggregate': 'entry_child',
            'pipeline': [
                { '$match': { 'date_field': { '$gte': start_date.format('YYYY-MM-DD'), '$lte': end_date.format('YYYY-MM-DD') } } },
                { '$lookup': { 'from': 'entry', 'localField': 'parent_uuid', 'foreignField': 'uuid', 'as': 'entry' } },
            ],
            'cursor': {}
        }
        if self.explain:
            query = {
                'explain': query
            }
        return self.mg.command(command=query)

    def _get_explain_glob(self):
        return f'{self.report_dir}/mg.join.*.txt'

    def _get_time_report_path(self):
        return f'{self.report_dir}/mg.join.csv'

    def _report_explain(self, label, start_date, content):
        with open(f'{self.report_dir}/mg.join.{label}.{start_date}.txt', 'w') as f:
            f.write(json.dumps(content, indent=4))
