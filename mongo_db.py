import pymongo.errors
from pymongo import MongoClient
from helper import logger
from bson.objectid import ObjectId


def create_collection(collection):
    if collection not in db.list_collection_names():
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
        try:
            collection.create_index(index_name, unique=True)
        except pymongo.errors.OperationFailure:
            logger.warning("Index already exists, creation not possible")


def del_duplicate_follows(collection_name, attribute):
    collection = db[collection_name]
    response = collection.aggregate(
        [{'$group': {'_id': '$' + attribute, 'count': {'$sum': 1}}}, {'$match': {'count': {'$gt': 1}}}])
    for el in response:
        res = read({"id": el["_id"]}, "cc_users", {"id": 1, "followers_crawled": 1})
        for re in res:
            if not re["followers_crawled"]:
                result = collection.delete_one({'_id': ObjectId(re["_id"])})
                print(result.raw_result)
    response = read({}, "cc_follows", {"id": 1, "following": 1, "followed_by": 1})
    for res in response:
        if "following" in res:
            print(res["following"])
        if "followed_by" in res:
            print(res["followed_by"])
        break


def update_dup(id, arr, dups):
    collection = db["cc_follows"]
    collection.update_one({"_id": id}, {"$set": {arr: dups}})


def del_dup_array(collection_name, attribute):
    c = 0
    response = read({}, "cc_follows", {"id": 1, "following": 1, "followed_by": 1})
    for res in response:
        if c % 1000 == 0:
            print(c)
        c += 1
        if "following" in res:
            original = res["following"]
            dup_free = list(set(original))
            if not len(dup_free) == len(original):
                update_dup(res["_id"], "following", dup_free)
                # print(res["_id"])
        if "followed_by" in res:
            original = res["followed_by"]
            dup_free = list(set(original))
            if not len(dup_free) == len(original):
                update_dup(res["_id"], "followed_by", dup_free)


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
