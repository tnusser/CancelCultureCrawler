import os
from pymongo import MongoClient
import json
import time
from helper import logger


def create_collection(collection):
    if collection not in db.collection_names():
        db.create_collection(collection)


def insert(json_data, collection):
    tweets = db[collection]
    try:
        tweets.insert_many(json_data)
    except Exception as e:
        logger.error("Error writing results to DB: %s", e)


client = MongoClient('mongodb://127.0.0.1:27017/')
db = client['twitter_db']
create_collection("cc_users")
create_collection("cc_tweets")
db["cc_users"].create_index("id")
db["cc_tweets"].create_index("id")


def read():
    pass


def pretty(d, indent=0):
    for key, value in d.items():
        print('\t' * indent + str(key))
        if isinstance(value, dict):
            pretty(value, indent + 1)
        else:
            print('\t' * (indent + 1) + str(value))

# print(tweets.estimated_document_count())
#
# t = ["a", "blessed", "green", "hello"]
#
# for tt in t:
#     query = {"text": {"$regex": ".*" + tt + ".*"}}
#     before = time.time()
#     results = tweets.find(query)
#     print(len(list(results)))
#     for res in results:
#         pass
#     after = time.time()
#
#     print(tt + " took " + str(after - before))
