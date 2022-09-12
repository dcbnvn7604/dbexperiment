from pymongo import MongoClient

from settings import MG_URI


def main():
    mongo = MongoClient(MG_URI)
    db = mongo.get_database('dbexperiment')
    db.drop_collection('books')
    collection = db.get_collection('books')
    collection.create_index('publication_date')

if __name__ == '__main__':
    main()