import mysql.connector
import random
import time
import datetime


# Global methods to push interact with the Database

# This method establishes the connection with the MySQL
def create_server_connection(host_name, user_name, user_password):
    # Implement the logic to create the server connection
    connection = None
    try :
        connection = mysql.connector.connect(
            host = host_name,
            user = user_name,
            password = user_password
        )
        print("Database Connection Successful")


    except Exception as err : 
        print(f"Error : '{err}'")

    return connection



# This method will create the database and make it an active database
def create_and_switch_database(connection, db_name, switch_db):
    # For database creatio nuse this method
    # If you have created your databse using UI, no need to implement anything
    cursor = connection.cursor()
    try : 
        drop_query = "DROP DATABASE IF EXISTS "+ db_name
        db_query = "CREATE DATABASE "+ db_name 
        switch_query = "USE " + switch_db
        cursor.execute(drop_query)
        cursor.execute(db_query)
        cursor.execute(switch_query)
        print("Database created succesfully")

    except Exception as err : 
        print(f"Error in creating database : '{err}'")


# This method will establish the connection with the newly created DB
def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        if connection.is_connected():
            print("MySQL Database connection successful")
        else:
            print("Failed to connect to MySQL database")

    except mysql.connector.Error as err:
        print(f"Error connecting to MySQL database: {err}")

    return connection


# Use this function to create the tables in a database
def create_table(connection, table_creation_statement):
    cursor = connection.cursor()

    try :
        cursor.execute(table_creation_statement)
        connection.commit()
        print("Table creation successful")
    except Exception as err :
        print(f"Error creating table : '{err}'")


# Perform all single insert statments in the specific table through a single function call
def create_insert_query(connection, query):
    # This method will perform creation of the table
    # this can also be used to perform single data point insertion in the desired table
    cursor = connection.cursor()
    try : 
        cursor.execute(query)
        connection.commit()
        print("Insertion successful")
    except Exception as err :
        print(f"Error in insertion : '{err}'")


# retrieving the data from the table based on the given query
def select_query(connection, query):
    # fetching the data points from the table 
    cursor = connection.cursor()
    result = None
    try :
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Exception as err : 
        print(f"Error in fetching data : '{err}'")


# Execute multiple insert statements in a table
def insert_many_records(connection, sql, val):
    cursor = connection.cursor()
    try :
        cursor.executemany(sql, val)
        connection.commit()
        print("Multiple Insertion successful")
    except Exception as err :
        print(f"Error in insertion : '{err}'")
