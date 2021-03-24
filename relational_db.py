import mysql.connector
from mysql.connector import Error

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


password = "ilemlbseicmzt2501527"
connection = create_connection("localhost", "root", password)


def create_database(connection, query):
  cursor = connection.cursor()
  try:
    cursor.execute(query)
    print("Database created successfully")
  except Error as e:
    print(f"The error '{e}' occurred")


create_database_query = "CREATE DATABASE big_data"
create_database(connection, create_database_query)


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
  except Error as e:\
    print(f"The error '{e}' occurred")

  return connection

connection = create_connection("localhost", "root", password, "big_data")
