import csv
import database as db

PW = "adimak12"  # IMPORTANT! Put your MySQL Terminal password here.
ROOT = "root"
DB = "ecommerce_record"  # This is the name of the database we will create in the next step - call it whatever you like.
LOCALHOST = "localhost"  # considering you have installed MySQL server on your computer

RELATIVE_CONFIG_PATH = '../config/'

USER = 'users'
PRODUCTS = 'products'
ORDER = 'orders'

create_user_table = """
    CREATE TABLE IF NOT EXISTS users (
        user_id varchar(10) PRIMARY KEY,
        user_name varchar(45) NOT NULL,
        user_email varchar(45) NOT NULL,
        user_password varchar(45) NOT NULL,
        user_address varchar(45) NOT NULL,
        is_vendor tinyint(1) DEFAULT 0
    )
    """


create_product_table = """
    CREATE TABLE IF NOT EXISTS products (
        product_id varchar(40) NOT NULL PRIMARY KEY,
        product_name varchar(45) NOT NULL,
        product_price float(45) NOT NULL,
        product_description varchar(100) NOT NULL,
        vendor_id varchar(10) NOT NULL,
        emi_available varchar(10) NOT NULL,
        CONSTRAINT fk_vendor_id FOREIGN KEY (vendor_id) REFERENCES users (user_id)
    )
"""

create_order_table = """
    CREATE TABLE IF NOT EXISTS orders (
        order_id int NOT NULL PRIMARY KEY,
        customer_id varchar(10) NOT NULL,
        vendor_id varchar(10) NOT NULL,
        total_value float(45) NOT NULL,
        order_quantity int NOT NULL,
        reward_point int NOT NULL,
        CONSTRAINT fk_vendor_id_orders FOREIGN KEY (vendor_id) REFERENCES users (user_id),
        CONSTRAINT fk_customer_id FOREIGN KEY (customer_id) REFERENCES users (user_id)
    )
"""

create_customer_leaderboard = """
    CREATE TABLE IF NOT EXISTS customer_leaderboard (
        customer_id varchar(10) NOT NULL PRIMARY KEY,
        total_value float(45) NOT NULL,
        customer_name varchar(45) NOT NULL,
        customer_email varchar(45) NOT NULL,
        CONSTRAINT fk_customer_id_leaderboard FOREIGN KEY (customer_id) REFERENCES users (user_id)
    )
"""

connection = db.create_server_connection(LOCALHOST, ROOT, PW)

# creating the schema in the DB
#1.a
db.create_and_switch_database(connection, DB, DB)
#1.b
print("Initiating table creation ")
db.create_table(connection, create_user_table)
print("User table created successfully\n")

db.create_table(connection, create_product_table)
print("Product table created successfully\n")

db.create_table(connection, create_order_table)
print("Order table created successfully\n")

db.create_table(connection, create_customer_leaderboard)
print("Customer leaderboard table created successfully\n")

# Create the tables through python code here
# if you have created the table in UI, then no need to define the table structure
# If you are using python to create the tables, call the relevant query to complete the creation

#2.a
print("Initiating data insertion in user table. \n")
with open(RELATIVE_CONFIG_PATH + USER + '.csv', 'r') as f:
    val = []
    data = csv.reader(f)
    for row in data:
        val.append(tuple(row))

    sql = """
    INSERT INTO users (user_id, user_name, user_email, user_password, user_address, is_vendor)
    VALUES (%s, %s, %s, %s, %s, %s);
    """
    
    val.pop(0)
    db.insert_many_records(connection, sql, val)

print("Data insertion in user table is complete. \n")

print("Initiating data insertion in product table. \n")
with open(RELATIVE_CONFIG_PATH + PRODUCTS + '.csv', 'r') as f:
    val = []
    data = csv.reader(f)
    for row in data:
        val.append(tuple(row))

    sql = """
    INSERT INTO products (product_id, product_name, product_price, product_description, vendor_id, emi_available)
    VALUES (%s, %s, %s, %s, %s, %s);
    """
    val.pop(0)
    db.insert_many_records(connection, sql, val)

print("Data insertion in product table is complete. \n")

print("Initiating data insertion in order table. \n")
with open(RELATIVE_CONFIG_PATH + ORDER + '.csv', 'r') as f:
    val = []
    data = csv.reader(f)
    for row in data:
        val.append(tuple(row))

    sql = """
    INSERT INTO orders (order_id, customer_id, vendor_id, total_value, order_quantity, reward_point)
    VALUES (%s, %s, %s, %s, %s, %s);
    """
    val.pop(0)
    db.insert_many_records(connection, sql, val)

print("Data insertion in order table is complete. \n")

