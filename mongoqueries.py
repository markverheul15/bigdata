import pymongo
from pymongo import MongoClient
import json
import pymongo
import pandas as pd
myclient = pymongo.MongoClient()

# df = pd.read_csv('yourcsv.csv',encoding = 'ISO-8859-1')   # loading csv file
# df.to_json('yourjson.json')                               # saving to json file
# jdf = open('yourjson.json').read()                        # loading the json file
# data = json.loads(jdf)

cluster = MongoClient("mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&ssl=false")
db = cluster["RedditComments"]
collection_redpill = db["RedpillTest"]

results = collection_redpill.find({"author":"ReasonFreak"})
# results = collection_redpill.find({"_id":"605c65a21c25ca4be4d3a09e"})

for result in results:
    print(result["_id"])
    # print(result["author"])
