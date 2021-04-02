import time

import pymongo
from pymongo import MongoClient
import json
import pymongo
import pandas as pd
myclient = pymongo.MongoClient()
from pprint import pprint

cluster = MongoClient("mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&ssl=false")
db = cluster["IMDB"]
coll_name_basics = db["name_basics"]
coll_title_akas = db["title_akas"]
coll_title_basic = db["title_basic"]
coll_title_crew = db["title_crew"]
coll_title_rating = db["title_rating"]

# ✅Query 1: Change "\N" values from name_basics table to "0"
def query1():
    coll_name_basics.update_many(
        {"birthYear": { "$eq": '\\N'}},
        { "$set": {"birthYear": '0'}}
    )
    coll_name_basics.update_many(
        {"deathYear": { "$eq": '\\N'}},
        { "$set": {"deathYear": '0'}}
    )

# ✅Query 2: Which IMDB info is from the Netherlands, order by title?
def query2():
    resultq2 = coll_title_akas.aggregate([
        {
            '$match': {
                'region': 'NL'
            }
        }, {
            '$sort': {
                'title': 1
            }
        }
    ])
    # for x in resultq2:
    #     pprint(x)

# ✅Query 3: Which movies are in Dutch, order by title?
def query3():
    pass

# ✅Query 4: How many movies are rom-com?
def query4():
    results4 = coll_title_basic.aggregate([
        {
            '$match': {
                'genres': {
                    '$regex': '.*Comedy.*'
                }
            }
        }, {
            '$match': {
                'genres': {
                    '$regex': '.*Romance.*'
                }
            }
        }, {
            '$count': 'primaryTitle'
        }
    ])

    # pprint(results4)

# ✅Query 5: Which all minors in name basics?
def query5():
    q5 = coll_name_basics.find({ "$expr": { "$gte": [ { "$toInt": "$birthYear" }, 2003 ] } })
    # for x in q5:
    #     print('{0}'.format(x['primaryName']))

# Query 6: I want top 250 of mainstream imdb movies, defined by num_rating set at 100.000 ratings
def query6():
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
                'from': 'title_basic',
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
                'title': '$titleid.primaryTitle',
                'averageRating':1
            }
        }
    ]

    results = coll_title_rating.aggregate(query6)
    #
    # for movie in results:
    #     print('{0}{1}'.format(movie['title'],movie['averageRating']))

# Query 7: Which actors play in Inception?
def query7():
    x = coll_title_basic.aggregate([
        {
            '$match': {
                'titleType': 'movie'}},
        {
            '$match': {
                'primaryTitle': 'Inception'}
            }])
    for iets in x:
        titel = iets['tconst']
        print(titel)
        output = coll_name_basics.aggregate([{
            "$match": {"primaryProfession": { "$regex" : ".*actor.*"}}},
        {
            "$match": {"knownForTitles":{ "$regex" : ".*%s.*"% titel}}}])

        for y in output:
            pprint(y['primaryName'])

# Query 8: Change all the words 'Lake' into 'Sea' in the title
def query8():
    coll_title_basic.update_many(
        {"originalTitle": { "$regex" : ".*Lake.*"}},
        { "$set": {"originalTitle": "sea"}}
    )

# ✅Query 9: Add four new rows in name_basics (personal information)
def query9():

    docs =	[{"nconst":"uva1", "primaryName":"Julia Holst", "birthYear": "1997", "primaryProfession":"student data science"},
               {"nconst":"uva2", "primaryName":"Graeme Bruijn", "birthYear":"1995", "primaryProfession":"student data science"},
               {"nconst":"uva3", "primaryName":"Lex Poon", "birthYear":"1996", "primaryProfession":"student data science"},
               {"nconst":"uva4","primaryName": "Mark Verheul", "birthYear":"1994", "primaryProfession":"student data science"}]
    # for doc in docs:
    #     coll_name_basics.save(doc)

    # resultsq9 = coll_name_basics.find({"primaryProfession":"student data science"})
    # for x in resultsq9:
    #     print('{0}'.format(x['primaryName']))

# ✅Query 10: Add recommended column based on rating 8+
def query10():

    coll_title_rating.update_many({"averageRating": {'$gte': 8}}, {"$set": {"tip": "Watch this"}}, upsert=False, array_filters=None)
    coll_title_rating.update_many({"averageRating": {'$lt': 8}}, {"$set": {"tip": "Skip this"}}, upsert=False, array_filters=None)
    x =coll_title_rating.find()
    # for y in x:
    #     print(y['averageRating'], y['tip'])

# ✅Query 11: Remove all non original from akas table
def query11():
    coll_title_akas.delete_many({ "isOriginalTitle":0})

def run_all():
    queryx = 1
    with open('times_mongodb.txt', mode='a') as f:
        start_time = time.time()
        query1()
        new_time = ("Query%s %s seconds ---\n" %(queryx, (time.time() - start_time)))
        queryx +=1
        f.writelines(new_time)
        start_time = time.time()
        query2()
        new_time = ("Query%s %s seconds ---\n" %(queryx, (time.time() - start_time)))
        queryx +=1
        f.writelines(new_time)
        start_time = time.time()
        # query3()
        new_time = ("Query%s %s seconds ---\n" %(queryx, (time.time() - start_time)))
        queryx +=1
        f.writelines(new_time)
        start_time = time.time()
        query4()
        new_time = ("Query%s %s seconds ---\n" %(queryx, (time.time() - start_time)))
        queryx +=1
        f.writelines(new_time)
        start_time = time.time()
        query5()
        new_time = ("Query%s %s seconds ---\n" %(queryx, (time.time() - start_time)))
        queryx +=1
        f.writelines(new_time)
        start_time = time.time()
        query6()
        new_time = ("Query%s %s seconds ---\n" %(queryx, (time.time() - start_time)))
        queryx +=1
        f.writelines(new_time)
        start_time = time.time()
        query7()
        new_time = ("Query%s %s seconds ---\n" %(queryx, (time.time() - start_time)))
        queryx +=1
        f.writelines(new_time)
        start_time = time.time()
        query8()
        new_time = ("Query%s %s seconds ---\n" %(queryx, (time.time() - start_time)))
        queryx +=1
        f.writelines(new_time)
        start_time = time.time()
        query9()
        new_time = ("Query%s %s seconds ---\n" %(queryx, (time.time() - start_time)))
        queryx +=1
        f.writelines(new_time)
        start_time = time.time()
        query10()
        new_time = ("Query%s %s seconds ---\n" %(queryx, (time.time() - start_time)))
        queryx +=1
        f.writelines(new_time)
        start_time = time.time()
        query11()
        new_time = ("Query%s %s seconds ---\n" %(queryx, (time.time() - start_time)))
        queryx +=1
        f.writelines(new_time)
        f.close()
run_all()
# query10()
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
# ✅Query 11: Remove all non original from akas table

