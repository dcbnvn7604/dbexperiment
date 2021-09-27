import csv
import uuid
import random
import datetime
from faker import Faker


fake = Faker()

number_row = 10000000

with open('../data/book.csv', 'w') as file:
    fieldnames = ['id', 'uuid', 'name', 'description', 'price', 'print_length', 'file_size', 'publication_date']
    writer = csv.DictWriter(file, fieldnames=fieldnames)
    writer.writeheader()

    date_start = datetime.date(2000, 1, 1)
    date_end = datetime.date(2020, 12, 31)
    for i in range(1, number_row):
        writer.writerow({
            'id': i,
            'uuid': uuid.uuid4(),
            'name': fake.text(max_nb_chars=100),
            'description': fake.text(max_nb_chars=2000),
            'price': random.randint(50, 1500),
            'print_length': random.randint(50, 3000),
            'file_size': random.randint(700, 36000),
            'publication_date': fake.date_between_dates(date_start=date_start, date_end=date_end).strftime('%Y/%m/%d')
        })