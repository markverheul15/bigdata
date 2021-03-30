import mysql.connector
import os
import pandas as pd
from mysql.connector import Error
from sqlalchemy import create_engine


# Connect to MySQL server
def create_connection_server(host_name, user_name, user_password):
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


# Create databse in MySQL server
def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully")
    except Error as e:
        print(f"The error '{e}' occurred")


# Connect to MySQL database in server
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


# Define dataset tables
create_name_basics_table = """
CREATE TABLE IF NOT EXISTS name_basics (
  nconst VARCHAR(255) NOT NULL PRIMARY KEY,
  primaryName TEXT,
  birthYear VARCHAR(255),
  deathYear VARCHAR(255),
  primaryProfession TEXT,
  knownForTitles VARCHAR(255)
) ENGINE = InnoDB
"""

create_title_akas_table = """
CREATE TABLE IF NOT EXISTS title_akas (
  titleId VARCHAR(255) NOT NULL PRIMARY KEY,
  ordering INT,
  title MEDIUMTEXT,
  region VARCHAR(255),
  language TEXT,
  types TEXT,
  attributes TEXT,
  isOriginalTitle VARCHAR(255)
) ENGINE = InnoDB
"""

create_title_basic_table = """
CREATE TABLE IF NOT EXISTS title_basic (
  tconst VARCHAR(255) NOT NULL PRIMARY KEY,
  titleType VARCHAR(255),
  primaryTitle TEXT,
  originalTitle TEXT,
  isAdult INT,
  startYear VARCHAR(255),
  endYear VARCHAR(255),
  runtimeMinutes VARCHAR(255),
  genres TEXT
) ENGINE = InnoDB
"""

create_title_crew_table = """
CREATE TABLE IF NOT EXISTS title_crew (
  tconst VARCHAR(255) NOT NULL PRIMARY KEY,
  directors TEXT,
  writers TEXT
) ENGINE = InnoDB
"""

create_title_rating_table = """
CREATE TABLE IF NOT EXISTS title_rating (
  tconst VARCHAR(255) NOT NULL PRIMARY KEY,
  averageRating DOUBLE(10, 2),
  numVotes INT
) ENGINE = InnoDB
"""

# Initialize MySQL server and connect to local database
password = "mysql123"
# password = "ilemlbseicmzt2501527"
connection = create_connection_server("localhost", "root", password)

# Execute database creation
create_database_query = "CREATE DATABASE IMDB_movie_2020"
create_database(connection, create_database_query)

# Connect to IMDB database
connection = create_connection_db("localhost", "root", password, "IMDB_movie_2020")

# Create tables in MySQl database
execute_query(connection, create_name_basics_table)
execute_query(connection, create_title_akas_table)
execute_query(connection, create_title_basic_table)
execute_query(connection, create_title_crew_table)
execute_query(connection, create_title_rating_table)

# Create and connect to SQL engine
engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                       .format(user="root", pw=password,
                               db='IMDB_movie_2020'))
dbConnection = engine.connect()
directory = "IMDB_movie_2020/"

# Insert datasets into database table via SQLalchemy engine
for file in os.listdir(directory):
    print("{} will now be inserted into the MySQL dataset".format(file))
    table = file[:-4]
    try:
        chunk = pd.read_csv('IMDB_movie_2020/{}'.format(file), sep='\t', chunksize=1000000, low_memory=False)
        df = pd.concat(chunk)
        if file == "title_akas.tsv" or file == "name_basics.tsv":
            df.drop_duplicates(subset=[df.columns[0]], keep='first', inplace=True)
        df.to_sql(table, dbConnection, if_exists='append', chunksize=1000000, index=False)
    except ValueError as vx:
        print(vx)
    except Exception as ex:
        print(ex)
    else:
        print("{} dataset has been inserted".format(table))
