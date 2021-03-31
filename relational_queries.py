import mysql.connector
from mysql.connector import Error
import time

def create_connection_db(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("Connection to MySQL DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")

    return connection


# Execute queries in specific tables
def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query executed successfully")
    except Error as e:
        print(f"The error '{e}' occurred")


# Initialize MySQL server and connect to local database
password = "mysql123"
# password = "ilemlbseicmzt2501527"
connection = create_connection_db("localhost", "root", password, "IMDB_movie_2020")


def execute_read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as e:
        print(f"The error '{e}' occurred")


# Query 1: Remove \N values from name_basics table and change column types to correct datatype
remove_values = """
"""


# Query 2: Which actors are a minor?
minor_actors = """
SELECT primaryName
FROM IMDB_movie_2020.name_basics
WHERE IMDB_movie_2020.name_basics.birthYear >  EXTRACT(YEAR FROM CURRENT_DATE) - 18
AND (IMDB_movie_2020.name_basics.primaryProfession LIKE "actor" 
    OR  IMDB_movie_2020.name_basics.primaryProfession LIKE "actress");
"""
# --  and IMDB_movie_2020.name_basics.deathYear is not null;

start_time = time.time()
minor_actors = execute_read_query(connection, minor_actors)

for minors in minor_actors:
    print(minors)
print("--- %s seconds ---" % (time.time() - start_time))


# Query 3: I want top 250 of mainstream imdb movies, defined by num_rating set at 100.000 ratings
movies_top250 = """
SELECT originalTitle, averageRating
FROM IMDB_movie_2020.title_rating
LEFT JOIN IMDB_movie_2020.title_basic
ON IMDB_movie_2020.title_rating.tconst = IMDB_movie_2020.title_basic.tconst
WHERE IMDB_movie_2020.title_basic.titleType = "movie" and IMDB_movie_2020.title_rating.numVotes > 100000
ORDER BY IMDB_movie_2020.title_rating.averageRating DESC
LIMIT 250;
"""

start_time = time.time()
movies_top250 = execute_read_query(connection, movies_top250)

for movie in movies_top250:
    print(movie)
print("--- %s seconds ---" % (time.time() - start_time))

# Query 4: Which director has directed the most movies?
# Query 5: How many movies are rom-coms?
# Query 6: Find all unique genres?
# Query 7: Which genre is represented the most?


# Query 8: Which actors play in Inception?
inception = """
SELECT tconst, originalTitle
FROM IMDB_movie_2020.title_basic
WHERE titleType = "movie" AND originalTitle = "Inception";
"""

start_time = time.time()
inception = execute_read_query(connection, inception)

for actors in inception:
    print(actors)
print("--- %s seconds ---" % (time.time() - start_time))


# Query 9: Remove non original titles from dataset title_akas
originals = """
SELECT title
FROM IMDB_movie_2020.title_akas
WHERE isOriginalTitle = 1;
"""

start_time = time.time()
originals = execute_read_query(connection, originals)

for original in originals:
    print(original)
print("--- %s seconds ---" % (time.time() - start_time))


# Query 10: Change all the words 'Lake' into 'Sea' in the title

lake_sea = """
SELECT originalTitle, REPLACE( originalTitle, 'Lake', 'Sea' ) as new_originalTitle
FROM IMDB_movie_2020.title_basic
WHERE originalTitle LIKE "%Lake%";
"""

start_time = time.time()
lake_sea = execute_read_query(connection, lake_sea)

for x in lake_sea:
    print(x)
print("--- %s seconds ---" % (time.time() - start_time))