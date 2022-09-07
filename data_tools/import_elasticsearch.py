import csv
import json

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk


def generate_action():
    with open('../data/book.csv', mode="r") as f:
        reader = csv.DictReader(f)

        for row in reader:
            yield {
                "_id": row["uuid"],
                "name": row["name"],
                "description": row["description"],
                "price": int(row["price"]),
                "print_length": int(row["print_length"]),
                "file_size": int(row["file_size"]),
                "publication_date": row["publication_date"]
            }

def main():
    client = Elasticsearch(['elasticsearch'])
    with open('../data/book.json', mode="r") as f:
        index_body = json.loads(f.read())
    if client.indices.exists(index="books"):
        client.indices.delete(index="books")
    client.indices.create(index="books", mappings=index_body['mappings'])

    successes = 0
    for ok, action in streaming_bulk(client=client, index="books", actions=generate_action()):
        successes += ok
    print(f"Index {successes} documents")


main()
