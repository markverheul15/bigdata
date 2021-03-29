import mysql.connector
from mysql.connector import Error


# connect to MySQL server
def create_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password
        )
        print("Connection to MySQL DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")

    return connection


# password = "ilemlbseicmzt2501527"
password = "DFGZrzc9bm6Qg}P-"
connection = create_connection("localhost", "root", password)


# Create database RedditComments
def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully")
    except Error as e:
        print(f"The error '{e}' occurred")


create_database_query = "CREATE DATABASE IMDB_movie_2020"
create_database(connection, create_database_query)


# Connect to MySQL database server after creation of database
def create_connection(host_name, user_name, user_password, db_name):
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


connection = create_connection("localhost", "root", password, "IMDB_movie_2020")


def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query executed successfully")
    except Error as e:
        print(f"The error '{e}' occurred")

create_name_basics_table = """
CREATE TABLE IF NOT EXISTS name_basics (
  nconst VARCHAR(255) NOT NULL PRIMARY KEY,
  primaryName TEXT,
  birthYear INT,
  deathYear INT,
  primaryProfession TEXT,
  knownForTitles TEXT
) ENGINE = InnoDB
"""

create_title_akas_table = """
CREATE TABLE IF NOT EXISTS title_akas (
  titleId VARCHAR(255) NOT NULL PRIMARY KEY,
  ordering INT,
  title TEXT,
  region VARCHAR(255),
  language TEXT,
  types TEXT,
  attributes TEXT,
  isOriginalTitle TINYINT(1)
) ENGINE = InnoDB
"""

create_title_basic_table = """
CREATE TABLE IF NOT EXISTS title_basic (
  tconst VARCHAR(255) NOT NULL PRIMARY KEY,
  titleType VARCHAR(255),
  primaryTitle TEXT,
  originalTitle TEXT,
  isAdult TINYINT(1),
  startYear INT,
  endYear INT,
  runtimeMinutes INT,
  genres TEXT
) ENGINE = InnoDB
"""

create_title_crew_table = """
CREATE TABLE IF NOT EXISTS title_crew (
  tconst VARCHAR(255) NOT NULL PRIMARY KEY,
  directors VARCHAR(255),
  writers VARCHAR(255)
) ENGINE = InnoDB
"""

create_title_rating_table = """
CREATE TABLE IF NOT EXISTS title_rating (
  tconst VARCHAR(255) NOT NULL PRIMARY KEY,
  averageRating DOUBLE(10, 2),
  numVotes INT
) ENGINE = InnoDB
"""

execute_query(connection, create_name_basics_table)
execute_query(connection, create_title_akas_table)
execute_query(connection, create_title_basic_table)
execute_query(connection, create_title_crew_table)
execute_query(connection, create_title_rating_table)

# LOAD DATA LOCAL INFILE 'D:/actors.list.tsv' INTO TABLE actors FIELDS TERMINATED BY '\t';