import glob
import os
import csv
import random
import arrow
from arrow.arrow import Arrow
from datetime import timedelta


from settings import DATE_START, DATE_END, ROUND


def make_query_parameters(explain):
    _round = ROUND if not explain else 1

    start_date = arrow.get(DATE_START)
    end_date = arrow.get(DATE_END)
    deltas = {
        "1_day": timedelta(days=1),
        "1_week": timedelta(days=7),
        '1_month': timedelta(days=30),
        '1_year': timedelta(days=365),
    }

    parameters = []
    for (period, delta) in deltas.items():
        for i in range(_round):
            start_int = start_date.int_timestamp
            end_int = (end_date - delta).int_timestamp
            rand_date = Arrow.fromtimestamp(random.randint(start_int, end_int))
            parameters.append({
                'start_date': rand_date,
                'delta': delta,
                'period': period
            })
    return parameters


class JoinMixin():
    def measure(self, parameters):
        if self.explain:
            self._clear_explain()
        for parameter in parameters:
            resp = self.query(parameter['period'], parameter['start_date'], parameter['start_date'] + parameter['delta'])

            if self.explain:
                self._report_explain(parameter['period'], parameter['start_date'], resp)
                
        if not self.explain:
            self._report_time()

    def _clear_explain(self):
        files = glob.glob(self._get_explain_glob())
        for file in files:
            os.remove(file)

    def _report_time(self):
        with open(self._get_time_report_path(), 'w') as f:
            fields = ["period", "start_date", "result", "spent_time"]
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            for entry in self.times:
                writer.writerow(entry)
