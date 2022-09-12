import glob
import os
import csv


class AggregateMixin():
    def measure(self, start_dates, deltas):
        if self.explain:
            self._clear_explain()
        for (label, _start_dates) in start_dates.items():
            for start_date in _start_dates:
                resp = self.query(start_date, (start_date + deltas[label]), label)
                
                if self.explain:
                    self._report_explain(label, start_date, resp)
        if not self.explain:
            self._report_time()

    def _clear_explain(self):
        files = glob.glob(self._get_explain_glob())
        for file in files:
            os.remove(file)

    def _report_time(self):
        with open(self._get_time_report_path(), 'w') as f:
            fields = ["label", "start_date", "result", "spent_time"]
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            for entry in self.times:
                writer.writerow(entry)
