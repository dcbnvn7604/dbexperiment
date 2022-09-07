import csv
import uuid
import random
import datetime
import arrow
from faker import Faker

from settings import TOTAL_RECORD, DATE_START, DATE_END


fake = Faker()


with open('../data/book.csv', 'w+') as file:
    fieldnames = ['id', 'uuid', 'name', 'description', 'price', 'print_length', 'file_size', 'publication_date']
    writer = csv.DictWriter(file, fieldnames=fieldnames)
    writer.writeheader()

    date_start = arrow.get(DATE_START).date()
    date_end = arrow.get(DATE_END).date()
    for i in range(1, TOTAL_RECORD):
        writer.writerow({
            'id': i,
            'uuid': uuid.uuid4(),
            'name': fake.text(max_nb_chars=100),
            'description': fake.text(max_nb_chars=2000),
            'price': random.randint(50, 1500),
            'print_length': random.randint(50, 3000),
            'file_size': random.randint(700, 36000),
            'publication_date': fake.date_between_dates(date_start=date_start, date_end=date_end).strftime('%Y-%m-%d')
        })