from pymongo import MongoClient

from settings import MG_URI


def main():
    mongo = MongoClient(MG_URI)
    db = mongo.get_database('dbexperiment')
    db.drop_collection('entry')
    collection = db.get_collection('entry')
    collection.create_index('date_field')

if __name__ == '__main__':
    main()