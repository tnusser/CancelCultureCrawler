import pymongo.errors
from pymongo import MongoClient
from helper import logger
from bson.objectid import ObjectId
import configparser


def pretty(d, indent=0):
    """
    Pretty prints JSON output
    @param d: json data
    @param indent: for printing of lines
    """
    for key, value in d.items():
        print('\t' * indent + str(key))
        if isinstance(value, dict):
            pretty(value, indent + 1)
        else:
            print('\t' * (indent + 1) + str(value))


def create_collection(collection):
    """
    Creates collection if it doesn't exist and calls index creation function
    @param collection: name of the collection
    """
    if collection not in db.list_collection_names():
        new_collection = db.create_collection(collection)
        create_indexes(new_collection)
    else:
        logger.warning(f"Collection {collection} already exists")


def create_indexes(collection, index_name="id"):
    """
    Creates index (if it not exists yet) on attribute index_name for this collection
    @param collection: to create index
    @param index_name: attribute for index creation
    @return:
    """
    if index_name not in collection.index_information():
        try:
            collection.create_index(index_name, unique=True)
        except pymongo.errors.OperationFailure:
            logger.warning("Index already exists, creation not possible")
    else:
        logger.warning(f"Index {index_name} already exists in collection {collection}")


def modify(identifier, attributes, collection_name):
    """
    Finds document in collection based on identifier parameter and updates it according to the given attributes
    @param identifier: for certain documents in the collection that need to be updated
    @param attributes: update attribute and values
    @param collection_name: name of collection
    """
    collection = db[collection_name]
    try:
        collection.find_one_and_update({**identifier}, {**attributes}, return_document=False)
    except Exception as e:
        logger.error(f"Error writing results to DB: {e}")


def push_to_array(unique_id, field, value, collection_name):
    """
    Pushes value to array for a certain attribute based on a unique id given
    @param unique_id: either unique user or tweet id
    @param field: name that contains the array to be updated
    @param value: that should be pushed onto the array
    @param collection_name: name of the collection
    """
    try:
        collection = db[collection_name]
        collection.update({'id': unique_id}, {'$push': {f'{field}': value}})
    except Exception as e:
        logger.error("Error writing results to DB: %s", e)


def insert(json_data, collection_name):
    """
    Inserts data into collection
    @param json_data: to be inserted
    @param collection_name: name of collection
    """
    collection = db[collection_name]
    try:
        collection.insert_many(json_data)
    except Exception as e:
        logger.error(f"Error writing results to DB: {e}")


def read(query_attr, collection_name, return_attr=None):
    """
    Reads data according to query attributes from db and returns results
    @param query_attr: dictionary of query attributes
    @param collection_name: name of collection
    @param return_attr: optional dictionary of return attributes that should be present in result
    @return:
    """
    if return_attr is None:
        return_attr = {"id": 1}
    collection = db[collection_name]
    return collection.find({**query_attr}, {**return_attr})


def del_duplicate(collection_name, attribute):
    """
    Deletes duplicate documents based on a given attribute in a collection
    @param collection_name: name of collection
    @param attribute: attribute that should be unique for all documents in collection
    """
    collection = db[collection_name]
    known_duplicates = collection.aggregate(
        [{'$group': {'_id': '$' + attribute, 'count': {'$sum': 1}}}, {'$match': {'count': {'$gt': 1}}}],
        allowDiskUse=True)
    counter = 0
    for el in known_duplicates:
        counter += 1
        if counter % 5000 == 0:
            logger.info(f"Processed {counter} entries")
        duplicates = read({"id": el["_id"]}, collection_name, {"id": 1})
        for i, duplicate in enumerate(duplicates):
            if i != 0:
                # duplicate --> delete
                collection.delete_one({'_id': ObjectId(duplicate["_id"])})


def delete_duplicates_arr(collection_name, attribute):
    """
    Deletes duplicates out of arrays in for a specified attribute in all documents
    @param collection_name: name of collection
    @param attribute: name of attribute with an array that should be duplicate free
    """
    counter = 0
    response = read({}, collection_name, {"id": 1, attribute: 1})
    for document in response:
        if counter % 5000 == 0:
            logger.info(f"Processed {counter} entries")
        counter += 1
        if attribute in document:
            original = document[attribute]
            dup_free = list(set(original))
            if not len(dup_free) == len(original):
                # duplicates eliminated --> update document
                db[collection_name].update_one({"_id": document["_id"]}, {"$set": {attribute: dup_free}})


config = configparser.ConfigParser()
config.read("config.ini")
mongo_config = config["mongoDB"]

client = MongoClient(f'mongodb://{mongo_config["IP"]}:{mongo_config["Port"]}/')
db = client[mongo_config["DatabaseName"]]
create_collection(mongo_config["TweetCollection"])
create_collection(mongo_config["UserCollection"])
create_collection(mongo_config["TimelineCollection"])
create_collection(mongo_config["FollowerCollection"])
