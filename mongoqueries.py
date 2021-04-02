
import pymongo
from pymongo import MongoClient
import json
import pymongo
import pandas as pd
myclient = pymongo.MongoClient()
from pprint import pprint

cluster = MongoClient("mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&ssl=false")
db = cluster["IMDB"]
coll_name_basics = db["Name_basics"]
coll_title_akas = db["Title_Akas"]
coll_title_basic = db["Title_Basic"]
coll_title_crew = db["Title_Crew"]
coll_title_rating = db["Title_rating"]

# ✅Query 1: Change "\N" values from name_basics table to "0"

# coll_name_basics.update_many(
#     {"birthYear": { "$eq": '\\N'}},
#     { "$set": {"birthYear": '0'}}
# )
# coll_name_basics.update_many(
#     {"deathYear": { "$eq": '\\N'}},
#     { "$set": {"deathYear": '0'}}
# )



# ✅Query 2: Which IMDB info is from the Netherlands, order by title?

# resultq2 = coll_title_akas.aggregate([
#     {
#         '$match': {
#             'region': 'NL'
#         }
#     }, {
#         '$sort': {
#             'title': 1
#         }
#     }, {
#         '$project': {
#             'title': 1
#         }
#     }
# ])
# for x in resultq2:
#     pprint(x)

# ✅Query 3: Which movies are in Dutch, order by title?


# ✅Query 4: How many movies are rom-com?


# results4 = coll_title_basic.aggregate([
#     {
#         '$match': {
#             'genres': {
#                 '$regex': '.*Comedy.*'
#             }
#         }
#     }, {
#         '$match': {
#             'genres': {
#                 '$regex': '.*Romance.*'
#             }
#         }
#     }, {
#         '$count': 'primaryTitle'
#     }
# ])

# pprint(resultq4)

# ✅Query 5: Which actors are a minor?

# q5 = coll_name_basics.find({ "$expr": { "$gte": [ { "$toInt": "$birthYear" }, 2003 ] } })
# for x in q5:
#     print('{0}'.format(x['primaryName']))

# Query 6: I want top 250 of mainstream imdb movies, defined by num_rating set at 100.000 ratings
query6 = [
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
]

results = coll_title_rating.aggregate(query6)

for movie in results:
    print('{0}'.format(movie['title']))

# ✅Query 9: Add four new rows in name_basics (personal information)

# docs =	[{"nconst":"uva1", "primaryName":"Julia Holst", "birthYear": "1997", "primaryProfession":"student data science"},
#            {"nconst":"uva2", "primaryName":"Graeme Bruijn", "birthYear":"1995", "primaryProfession":"student data science"},
#            {"nconst":"uva3", "primaryName":"Lex Poon", "birthYear":"1996", "primaryProfession":"student data science"},
#            {"nconst":"uva4","primaryName": "Mark Verheul", "birthYear":"1994", "primaryProfession":"student data science"}]
# for doc in docs:
#     coll_name_basics.save(doc)

resultsq9 = coll_name_basics.find({"primaryProfession":"student data science"})
for x in resultsq9:
    print('{0}'.format(x['primaryName']))


# ✅Query 10: Add recommended column based on rating 8+

# coll_title_rating.update_many({"averageRating": {'$gte': 8}}, {"$set": {"Recommended": "Yes"}}, upsert=False, array_filters=None)
# coll_title_rating.update_many({"averageRating": {'$lt': 8}}, {"$set": {"Recommended": "No"}}, upsert=False, array_filters=None)
# x =coll_title_rating.find_one({"averageRating": {"$gt":8}})
# print(x)

# ✅Query 11: Remove all non original from akas table
# coll_title_akas.delete_many({ "isOriginalTitle":0})


# ✅Query 1: Change "\N" values from name_basics table to "0"
# ✅Query 2: Which IMDB info is from the Netherlands, order by title?
# Query 3: Which movies are in Dutch, order by title?
# ✅Query 4: How many movies are comedy (removed romantic)?
# ✅Query 5: Which actors are a minor?
# ✅Query 6: I want top 250 of mainstream imdb movies, defined by num_rating set at 100.000 ratings
# Query 7: Which actors play in Inception?
# Query 8: Change all the words 'Lake' into 'Sea' in the title
# ✅Query 9: Add four new rows in name_basics (personal information)
# ✅Query 10: Add recommended column based on rating 8+
# Extra? Count the recommended movies and series?
# ✅Query 11: Remove all non original from akas table


# NIETS -0-0
# coll_name_basics.aggregate([
#     {
#         '$addFields': {
#             'birthYear': {
#                 '$convert': {
#                     'input': '$birthYear',
#                     'to': 'int',
#                     'onError': 0
#                 }
#             }
#         }
#     }, {
#         '$addFields': {
#             'deathYear': {
#                 '$convert': {
#                     'input': '$deathYear',
#                     'to': 'int',
#                     'onError': 0
#                 }
#             }
#         }
#     }
# ])
# pprint(coll_title_basic.find({"nconst":"nm0000001"}))
