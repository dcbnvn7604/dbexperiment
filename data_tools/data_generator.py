import csv
import uuid
import random
import datetime
import arrow
from faker import Faker
from faker.providers import geo
import geojson

from settings import TOTAL_RECORD, DATE_START, DATE_END


fake = Faker()
fake.add_provider(geo)


with open('../data/entry.csv', 'w+') as file, \
    open('../data/entry_child.csv', 'w+') as child_file:

    fieldnames = ['id', 'uuid', 'text_field', 'int_field', 'date_field', 'point_field', 'lat', 'long']
    writer = csv.DictWriter(file, fieldnames=fieldnames)
    writer.writeheader()

    child_fieldnames = ['uuid', 'parent_uuid', 'date_field']
    child_writer = csv.DictWriter(child_file, fieldnames=child_fieldnames)
    child_writer.writeheader()

    date_start = arrow.get(DATE_START).date()
    date_end = arrow.get(DATE_END).date()
    total_child = 0
    for i in range(1, TOTAL_RECORD):
        _uuid = uuid.uuid4()
        point = fake.latlng()
        writer.writerow({
            'id': i,
            'uuid': _uuid,
            'text_field': fake.text(max_nb_chars=2000),
            'int_field': random.randint(50, 1500),
            'date_field': fake.date_between_dates(date_start=date_start, date_end=date_end).strftime('%Y-%m-%d'),
            'point_field': geojson.dumps(geojson.Point((float(point[1]), float(point[0])))),
            'lat': point[0],
            'long': point[1]
        })

        if total_child > TOTAL_RECORD:
            continue

        for j in range(0, 3):
            child_writer.writerow({
                'uuid': uuid.uuid4(),
                'parent_uuid': _uuid,
                'date_field': fake.date_between_dates(date_start=date_start, date_end=date_end).strftime('%Y-%m-%d')
            })
            total_child += 1
