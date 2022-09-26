import glob
import os
import csv
import random

from settings import TOTAL_RECORD, CLOSEST_RELATION_TOTAL, ROUND


def make_query_parameters(explain):
    def _make_parameters(length):
        _round = ROUND if not explain else 1
        record_indexs = set()
        start_index = random.randint(1, TOTAL_RECORD - ((CLOSEST_RELATION_TOTAL + length) * _round))
        for i in range(_round + 1):
            record_indexs.add(start_index + i * (CLOSEST_RELATION_TOTAL + length))
        parameters = []
        previous_uuid = None
        with open('../data/entry.csv', 'r') as file:
            reader = csv.DictReader(file)
            for (index, item) in enumerate(reader):
                if (index + 1) not in record_indexs:
                    continue
                if previous_uuid:
                    parameters.append((previous_uuid, item['uuid'], length))
                previous_uuid = item['uuid']
        return parameters

    parameters = []
    for length in [1, 2]:
        parameters += _make_parameters(length)
    return parameters

class RelationMixin():
    def measure(self, parameters):
        if self.explain:
            self._clear_explain()
        for parameter in parameters:
            resp = self.query(parameter)

            if self.explain:
                self._report_explain(parameter, resp)
                
        if not self.explain:
            self._report_time()

    def _clear_explain(self):
        files = glob.glob(self._get_explain_glob())
        for file in files:
            os.remove(file)

    def _report_time(self):
        with open(self._get_time_report_path(), 'w') as f:
            fields = ["uuids", "length", "result", "spent_time"]
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            for entry in self.times:
                writer.writerow(entry)
