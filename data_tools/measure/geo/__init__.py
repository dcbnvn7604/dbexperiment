import glob
import os
import csv

from faker import Faker
from faker.providers import geo
from settings import ROUND


def make_query_parameters(explain):
    _round = ROUND if not explain else 1

    fake = Faker()
    fake.add_provider(geo)

    return list([
        fake.latlng()
        for i in range(_round)
    ])


class GeoMixin():
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
            fields = ["point", "result", "spent_time"]
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            for entry in self.times:
                writer.writerow(entry)
