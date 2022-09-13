import csv
import uuid
import random
import datetime
import arrow
from faker import Faker

from settings import TOTAL_RECORD, DATE_START, DATE_END


fake = Faker()


with open('../data/entry.csv', 'w+') as file:
    fieldnames = ['id', 'uuid', 'text_field', 'int_field', 'date_field']
    writer = csv.DictWriter(file, fieldnames=fieldnames)
    writer.writeheader()

    date_start = arrow.get(DATE_START).date()
    date_end = arrow.get(DATE_END).date()
    for i in range(1, TOTAL_RECORD):
        writer.writerow({
            'id': i,
            'uuid': uuid.uuid4(),
            'text_field': fake.text(max_nb_chars=2000),
            'int_field': random.randint(50, 1500),
            'date_field': fake.date_between_dates(date_start=date_start, date_end=date_end).strftime('%Y-%m-%d')
        })