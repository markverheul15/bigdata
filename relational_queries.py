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


# Query 2: Which IMDB info is from the Netherlands, order by title?
imdb_NL = """
SELECT *
FROM IMDB_movie_2020.title_akas
WHERE region = "NL"
ORDER BY title;
"""

# Query 3: How many movies are rom-coms?
rom_com = """
SELECT count(*)
FROM IMDB_movie_2020.title_basic
WHERE genres LIKE "%Romance%"
AND genres LIKE "%Comedy%"
AND titleType = "movie";
"""

# Query 4: Select all minors in name_basics.
minors = """
SELECT primaryName
FROM IMDB_movie_2020.name_basics
WHERE IMDB_movie_2020.name_basics.birthYear >  EXTRACT(YEAR FROM CURRENT_DATE) - 18;
"""

# Query 5: I want top 250 of mainstream imdb movies, defined by num_rating set at 100.000 ratings
movies_top250 = """
SELECT originalTitle, averageRating
FROM IMDB_movie_2020.title_rating
LEFT JOIN IMDB_movie_2020.title_basic
ON IMDB_movie_2020.title_rating.tconst = IMDB_movie_2020.title_basic.tconst
WHERE IMDB_movie_2020.title_basic.titleType = "movie" and IMDB_movie_2020.title_rating.numVotes > 100000
ORDER BY IMDB_movie_2020.title_rating.averageRating DESC
LIMIT 250;
"""

# Query 6: Which (male) actors play in Inception?
inception = """
SELECT primaryName
FROM IMDB_movie_2020.name_basics
WHERE primaryProfession LIKE "%actor%"
AND knownForTitles LIKE CONCAT("%", (
SELECT  tconst
FROM IMDB_movie_2020.title_basic
WHERE titleType = "movie" AND originalTitle = "Inception"
AND tconst is not null),"%");
"""

# Query 7: Change all the words 'Lake' into 'Sea' in the title
lake_to_sea = """
SELECT originalTitle, REPLACE( originalTitle, 'Lake', 'Sea' ) as new_originalTitle
FROM IMDB_movie_2020.title_basic
WHERE originalTitle LIKE "%Lake%";
"""

# Query 8: Add four new rows in name_basics (personal information)
add_students = """
INSERT INTO IMDB_movie_2020.name_basics (nconst, primaryName, birthYear, primaryProfession)
VALUES	("uva1", "Julia Holst", 1997, "student data science"),
				("uva2", "Graeme Bruijn", 1995, "student data science"),
                ("uva3", "Lex Poon", 1996, "student data science"),
                ("uva4", "Mark Verheul", 1994, "student data science");
"""

# Query 9: Add recommended column based on rating 8+
recommended = """
SELECT averageRating,
IF (averageRating >= 8,'Watch this','Skip this') AS tip 
FROM IMDB_movie_2020.title_rating;
"""

# Query 10: Remove all non original from akas table
remove_non_originals = """
DELETE FROM IMDB_movie_2020.title_akas WHERE isOriginalTitle = 0;
"""

# Query 2 = imdb_NL
# Query 3 = rom_com
# Query 4 = minors
# Query 5 = movies_top250
# Query 6 = inception
# Query 7 = lake_to_sea
# Query 8 = add_students
# Query 9 = recommended
# Query 10 = remove_non_originals

all_queries = [imdb_NL, rom_com, minors, movies_top250, inception,
               lake_to_sea, add_students, recommended, remove_non_originals]

# Execute single query
def run_single_query(query_x):
    start_time = time.time()
    query = execute_read_query(connection, query_x)
    print("--- %s seconds ---" % (time.time() - start_time))

    for row in query:
        print(row)

    return 0

# run_single_query(movies_top250)

def run_all_queries(list_of_queries):

    f = open("times_mysql.txt", "a")
    queryx = 2
    for query in list_of_queries:
        start_time = time.time()
        query = execute_read_query(connection, query)
        new_time = ("Query%s %s seconds ---\n" %(queryx, (time.time() - start_time)))
        f.writelines(new_time)

        # optional: uncomment is you want to print it
        # for row in query:
        #     print(row)

        queryx += 1

run_all_queries(all_queries)