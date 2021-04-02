import os
import pandas as pd
import time
from pymongo import MongoClient


def mongo_import(db_name, directory):
    # Connect to local Mongo_DB
    client = MongoClient("mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&ssl=false")

    for file in os.listdir(directory):
        print("{} will now be inserted into the Mongo dataset".format(file))
        start_time = time.time()
        table = file[:-4]

        chunk = pd.read_csv('IMDB_movie_2020/{}'.format(file), sep='\t', chunksize=1000000,
                            low_memory=False)
        data = pd.concat(chunk)
        db = client[db_name]
        collection = db[table]
        data.reset_index(inplace=True)
        data_dict = data.to_dict("records")

        # Insert collection
        collection.insert_many(data_dict)
        print("{} dataset has been inserted".format(table))
        print("--- %s seconds ---" % (time.time() - start_time))

mongo_import("IMDB_movie_2020", "IMDB_movie_2020/")