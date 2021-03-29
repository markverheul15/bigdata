import pandas as pd
from pymongo import MongoClient

# Load csv dataset
data = pd.read_csv('<<INSERT NAME OF DATASET>>.csv')
# Connect to MongoDB
client = MongoClient(
    "mongodb+srv://<<YOUR USERNAME>>:<<PASSWORD>>@clustertest-icsum.mongodb.net/test?retryWrites=true&w=majority")
db = client['<<INSERT NAME OF DATABASE>>']
collection = db['<<INSERT NAME OF COLLECTION>>']
data.reset_index(inplace=True)
data_dict = data.to_dict("records")
# Insert collection
collection.insert_many(data_dict)





directory = "IMDB_movie_2020/"

for file in os.listdir(directory):
    table = file[:-4]
    chunk = pd.read_csv('IMDB_movie_2020/{}'.format(file), sep='\t', chunksize=1000000, low_memory=False)
    df = pd.concat(chunk)
    if file == "title_akas.tsv" or file == "name_basics.tsv":
        df.drop_duplicates(subset=[df.columns[0]], keep='first', inplace=True)
