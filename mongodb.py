import pymongo
from pymongo import MongoClient
import json
import time
import pandas as pd


start_time = time.time()

def mongoimport(csv_path, db_name, coll_name):
    """ Imports a csv file at path csv_name to a mongo colection
    returns: count of the documants in the new collection
    """
    client = MongoClient("mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&ssl=false")
    db = client[db_name]
    coll = db[coll_name]
    # Create a dataframe from the input CSV. As the file takes a lot of memory, delete variable after json conversion
    df = pd.read_csv(csv_path, sep='\t', header=0)
    df[''] = df['DataFrame Column'].astype(int)

    if csv_path == "title_akas.tsv" or csv_path == "name_basics.tsv":
        df.drop_duplicates(subset=[df.columns[0]], keep='first', inplace=True)
    payload = json.loads(df.to_json(orient='records'))
    del df
    # Emtpy the collection if one existed already
    coll.remove()
    # Create new start time just for the upload of this collection
    start_time = time.time()
    coll.insert(payload)
    print(f"{csv_path} took this long:\n")
    print("--- %s seconds ---" % (time.time() - start_time))

    print('Amount of columns:',coll.count())

# Create a collection for each of the dataset file under the database name
db_name = 'IMDB'

# csv_path = 'Data/name_basics.tsv'
# coll_name = 'Name_Basics'
# mongoimport(csv_path, db_name, coll_name)
#
# csv_path = 'Data/title_akas.tsv'
# coll_name = 'Title_Akas'
# mongoimport(csv_path, db_name, coll_name)

csv_path = 'Data/title_basic.tsv'
coll_name = 'Title_Basic'
mongoimport(csv_path, db_name, coll_name)

csv_path = 'Data/title_crew.tsv'
coll_name = 'Title_Crew'
mongoimport(csv_path, db_name, coll_name)

# csv_path = 'Data/title_rating.tsv'
# coll_name = 'Title_Rating'
# mongoimport(csv_path, db_name, coll_name)
