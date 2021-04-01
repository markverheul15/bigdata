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
# Query 2: Which IMDB info is from the Netherlands, order by title?
imdb_NL = """
SELECT *
FROM IMDB_movie_2020.title_akas
WHERE region = "NL"
ORDER BY title;
"""

#Query 3: Which movies are in Dutch, order by title?
movies_NL = """
SELECT title
FROM IMDB_movie_2020.title_akas as akas
LEFT JOIN IMDB_movie_2020.title_basic as basic
ON akas.titleID = basic.tconst
WHERE akas.region = "NL" AND basic.titleType = "movie"
ORDER BY title;
"""

# Query 4: How many movies are rom-coms?
# output: (13422,)
rom_com = """
SELECT count(*)
FROM IMDB_movie_2020.title_basic
WHERE genres LIKE "%Romance%"
AND genres LIKE "%Comedy%"
AND titleType = "movie";
"""

# Query 5: Which actors are a minor?
# output:
# ('James Walsh',)
# ('Klim Berdinskiy',)
# ('Elias Richard Siegmann',) ...
minor_actors = """
SELECT primaryName
FROM IMDB_movie_2020.name_basics
WHERE IMDB_movie_2020.name_basics.birthYear >  EXTRACT(YEAR FROM CURRENT_DATE) - 18
AND (IMDB_movie_2020.name_basics.primaryProfession LIKE "actor"
    OR  IMDB_movie_2020.name_basics.primaryProfession LIKE "actress");
"""

# Query 6: I want top 250 of mainstream imdb movies, defined by num_rating set at 100.000 ratings
# output:
# ('The Shawshank Redemption', 9.3)
# ('The Godfather', 9.2)
# ('The Godfather: Part II', 9.0)
# ('The Dark Knight', 9.0)
# ('12 Angry Men', 8.9) ...
movies_top250 = """
SELECT originalTitle, averageRating
FROM IMDB_movie_2020.title_rating
LEFT JOIN IMDB_movie_2020.title_basic
ON IMDB_movie_2020.title_rating.tconst = IMDB_movie_2020.title_basic.tconst
WHERE IMDB_movie_2020.title_basic.titleType = "movie" and IMDB_movie_2020.title_rating.numVotes > 100000
ORDER BY IMDB_movie_2020.title_rating.averageRating DESC
LIMIT 250;
"""

# Query 7: Which actors play in Inception?
# output: ('Leonardo DiCaprio',)
#         ('Tom Berenger',) ...
inception = """
SELECT primaryName
FROM IMDB_movie_2020.name_basics
WHERE (primaryProfession LIKE "%actor%"
OR primaryProfession LIKE "%actress%")
AND knownForTitles LIKE CONCAT("%", (
SELECT  tconst
FROM IMDB_movie_2020.title_basic
WHERE titleType = "movie" AND originalTitle = "Inception"
AND tconst is not null),"%");
"""

# Query 8: Change all the words 'Lake' into 'Sea' in the title
# output: ('The Lady of the Lake', 'The Lady of the Sea')
#         ('Le clown et ses chiens',) ...
lake_to_sea = """
SELECT originalTitle, REPLACE( originalTitle, 'Lake', 'Sea' ) as new_originalTitle
FROM IMDB_movie_2020.title_basic
WHERE originalTitle LIKE "%Lake%";
"""


# Query 9: Add four new rows in name_basics (personal information)
add_students = """
INSERT INTO IMDB_movie_2020.name_basics (nconst, primaryName, birthYear, primaryProfession)
VALUES	("uva1", "Julia Holst", 1997, "student data science"),
				("uva2", "Graeme Bruijn", 1995, "student data science"),
                ("uva3", "Lex Poon", 1996, "student data science"),
                ("uva4", "Mark Verheul", 1994, "student data science");
"""

# Query 10: Add recommended column based on rating 8+
recommended = """
SELECT averageRating,
IF (averageRating > 8,'Watch this','Skip this') AS tip 
FROM IMDB_movie_2020.title_rating;
"""

# Query 11: Remove all non original from akas table
remove_non_originals = """
DELETE FROM IMDB_movie_2020.title_akas WHERE isOriginalTitle = 0;
"""




# Query 1: Remove \N values from name_basics table and change column types to correct datatype
# Query 2 = imdb_NL
# Query 3 = movies_NL
# Query 4 = rom_com
# Query 5 = minor_actors
# Query 6 = movies_top250
# Query 7 = inception
# Query 8 = lake_to_sea
# Query 9 = add_students
# Query 10 = recommended
# Query 11 = remove_non_originals

# Query 11?: DELETE FROM IMDB_movie_2020.title_akas WHERE isOriginalTitle = 1;

query_x = recommended
start_time = time.time()
query = execute_read_query(connection, query_x)
for row in query:
    print(row)
print("--- %s seconds ---" % (time.time() - start_time))
