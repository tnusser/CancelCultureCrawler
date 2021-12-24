import os
from pymongo import MongoClient
import json
import time
from helper import logger


def create_collection(collection):
    if collection not in db.collection_names():
        db.create_collection(collection)


def modify(identifier, attributes, collection_name):
    collection = db[collection_name]
    try:
        collection.find_one_and_update({**identifier}, {**attributes}, return_document=False)
    except Exception as e:
        logger.error("Error writing results to DB: %s", e)


def update_array(user_id, field, value, collection_name):
    try:
        collection = db[collection_name]
        collection.update({'id': user_id}, {'$push': {f'{field}': value}})
    except Exception as e:
        logger.error("Error writing results to DB: %s", e)


def insert(json_data, collection_name):
    collection = db[collection_name]
    try:
        collection.insert_many(json_data)
    except Exception as e:
        logger.error("Error writing results to DB: %s", e)


def read(query_attr, collection_name, return_attr=None):
    if return_attr is None:
        return_attr = {"id": 1}
    collection = db[collection_name]
    return collection.find({**query_attr}, {**return_attr})


def create_indexes(collection, index_name="id"):
    if index_name not in collection.index_information():
        collection.create_index(index_name, unique=True)


client = MongoClient('mongodb://127.0.0.1:27017/')
db = client['twitter_db_DE']
create_collection("cc_users")
create_collection("cc_tweets")
create_collection("cc_timelines")
create_collection("cc_follows")

create_indexes(db["cc_users"])
create_indexes(db["cc_tweets"])
create_indexes(db["cc_timelines"])
create_indexes(db["cc_follows"])


def pretty(d, indent=0):
    for key, value in d.items():
        print('\t' * indent + str(key))
        if isinstance(value, dict):
            pretty(value, indent + 1)
        else:
            print('\t' * (indent + 1) + str(value))
