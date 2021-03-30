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


# Query 1:
title_akas = "SELECT * FROM title_akas limit 10"
title_akas = execute_read_query(connection, title_akas)

# for x in title_akas:
#     print(x)

# Query 2: I want top 250 of mainstream imdb movies, defined by num_rating set at 100.000 ratings
movies_top250 = """SELECT originalTitle, averageRating
                    FROM IMDB_movie_2020.title_rating
                    LEFT JOIN IMDB_movie_2020.title_basic
                    ON IMDB_movie_2020.title_rating.tconst = IMDB_movie_2020.title_basic.tconst
                    WHERE IMDB_movie_2020.title_basic.titleType = "movie" and IMDB_movie_2020.title_rating.numVotes > 100000
                    ORDER BY IMDB_movie_2020.title_rating.averageRating DESC
                    LIMIT 10;"""

start_time = time.time()
movies_top250 = execute_read_query(connection, movies_top250)

for movie in movies_top250:
    print(movie)
print("--- %s seconds ---" % (time.time() - start_time))