import json
from pymongo import MongoClient

from settings import MG_URI
from measure.textsearch import TextsearchMixin
from measure import _collect_time


class Mongo(TextsearchMixin):
    def __init__(self, report_dir, explain=False):
        self.mg = MongoClient(MG_URI)['dbexperiment']
        self.report_dir = report_dir
        self.explain = explain
        self.times = []

    def _create_entry(self, text, resp=None, spent_time=None):
        return {
            'id': resp["cursor"]["firstBatch"][0]["uuid"] if resp["cursor"]["firstBatch"] else None,
            'spent_time': spent_time,
        }

    @_collect_time(_create_entry)
    def query(self, text):
        query = {
            'find': 'entry',
            'filter': { '$text': { '$search':  text } },
            'sort': { 'score': { '$meta': "textScore" } },
            'projection': { 'score': { '$meta': "textScore" } },
        }
        if self.explain:
            query = {
                'explain': query
            }
        return self.mg.command(command=query)

    def _get_explain_glob(self):
        return f'{self.report_dir}/mg.ts.*.txt'

    def _get_time_report_path(self):
        return f'{self.report_dir}/mg.ts.csv'

    def _report_explain(self, id, content):
        with open(f'{self.report_dir}/mg.ts.{id}.txt', 'w') as f:
            f.write(json.dumps(content, indent=4))
