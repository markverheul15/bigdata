# Requires the PyMongo package.
# https://api.mongodb.com/python/current
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&ssl=false')
result = client['IMDB']['Title_rating'].aggregate([
    {
        '$match': {
            'numVotes': {
                '$gt': 100000
            }
        }
    }, {
        '$sort': {
            'averageRating': -1
        }
    }, {
        '$limit': 250
    }, {
        '$lookup': {
            'from': 'Title_Basic',
            'localField': 'tconst',
            'foreignField': 'tconst',
            'as': 'titleid'
        }
    }, {
        '$unwind': {
            'path': '$titleid'
        }
    }, {
        '$project': {
            'title': '$titleid.primaryTitle'
        }
    }
])
