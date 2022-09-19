import csv
import json
import geojson

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk


def generate_action():
    with open('../data/entry.csv', mode="r") as f:
        reader = csv.DictReader(f)

        for row in reader:
            yield {
                "_id": row["uuid"],
                "text_field": row["text_field"],
                "int_field": int(row["int_field"]),
                "date_field": row["date_field"],
                "point_field": [float(row['long']), float(row['lat'])],
            }

def main():
    client = Elasticsearch(['elasticsearch'])
    with open('../data/entry.json', mode="r") as f:
        index_body = json.loads(f.read())
    if client.indices.exists(index="entry"):
        client.indices.delete(index="entry")
    client.indices.create(index="entry", mappings=index_body['mappings'])

    successes = 0
    for ok, action in streaming_bulk(client=client, index="entry", actions=generate_action()):
        successes += ok
    print(f"Index {successes} documents")


main()
