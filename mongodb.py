import pymongo
from pymongo import MongoClient
import json
import time
start_time = time.time()

cluster = MongoClient("mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&ssl=false")
db = cluster["RedditComments"]
collection_redpill = db["RedpillTest"]


with open('data_1.json') as f:
    file_data = json.load(f)


collection_redpill.insert_many(file_data)

cluster.close()
print("--- %s seconds ---" % (time.time() - start_time))
