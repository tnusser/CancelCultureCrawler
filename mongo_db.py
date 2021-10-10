import os
from pymongo import MongoClient
import json
import time

client = MongoClient('mongodb://127.0.0.1:27017/')
db = client['twitter_db']
tweets = db['sample_tweets']

temp_directory = "/home/tobias/Projects/MasterProject/twitter-stream-2021-01-01"

def insert_json_to_db():
    for i, subdir in enumerate(os.listdir(temp_directory)):
        print("File counter {}".format(i))
        json_data = []
        with open(temp_directory + "/" + subdir, "r") as file:
            for line in file.readlines():
                json_data.append(json.loads(line))
        tweets.insert_many(json_data)


def pretty(d, indent=0):
    for key, value in d.items():
        print('\t' * indent + str(key))
        if isinstance(value, dict):
            pretty(value, indent + 1)
        else:
            print('\t' * (indent + 1) + str(value))

print(tweets.estimated_document_count())

t = ["a", "blessed", "green", "hello"]

for tt in t:
    query = {"text": {"$regex": ".*" + tt + ".*"}}
    before = time.time()
    results = tweets.find(query)
    print(len(list(results)))
    for res in results:
        pass
    after = time.time()

    print(tt + " took " + str(after-before))



