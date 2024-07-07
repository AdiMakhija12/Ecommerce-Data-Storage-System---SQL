import database as db

# Driver code
if __name__ == "__main__":
    """
    Please enter the necessary information related to the DB at this place. 
    Please change PW and ROOT based on the configuration of your own system. 
    """
    PW = "adimak12"  # IMPORTANT! Put your MySQL Terminal password here.
    ROOT = "root"
    DB = "ecommerce_record"  # This is the name of the database we will create in the next step - call it whatever
    # you like.
    LOCALHOST = "localhost"
    connection = db.create_db_connection(LOCALHOST, ROOT, PW, DB)

#2.b    
    print("Inserting 5 new orders : ")

    new_orders = """
    INSERT INTO orders VALUES 
    (101,12,2,35600,50,100),
    (102,23,4,12000,16,200),
    (103,9,3,20000,10,200),
    (104,13,6,40000,5,100),
    (105,16,5,30000,10,300)
    """

    db.create_insert_query(connection,new_orders)
    print("5 new orders inserted successfully")

#2.c
    print("Listing all orders : ")
    q1 = """
    SELECT * FROM orders;
    """
    orders = db.select_query(connection,q1)
    for order in orders :
        print(order)

#3.a
    q2 = """
    SELECT * FROM orders WHERE total_value = (SELECT MIN(total_value) FROM orders);
    """
    min_order_detail = db.select_query(connection,q2)
    print("Minimum order detail : " )
    print(min_order_detail) 

    q3 = """
    SELECT * FROM orders WHERE total_value = (SELECT MAX(total_value) FROM orders);
    """
    max_order_details = db.select_query(connection,q3)
    print("Maximum order detail : ")
    print(max_order_details)

#3.b
    print("Listing orders with value greater than average orders value : ")
    q4 = """
    SELECT * FROM orders WHERE total_value > (SELECT AVG(total_value) FROM orders);
    """
    orders = db.select_query(connection,q4)

    for order in orders :
        print(order)

#3.c
    print("Fetching customer detail with maximum order value : ")
    q5 = """
    SELECT o.customer_id, MAX(o.total_value) as MAX_Value, c.user_name, c.user_email
    FROM ecommerce_record.orders o
    LEFT JOIN ecommerce_record.users c ON o.customer_id = c.user_id
    GROUP BY o.customer_id;
    """

    highest_purchase = db.select_query(connection,q5)

    sql = """
    INSERT INTO customer_leaderboard (customer_id,total_value,customer_name,customer_email)
    VALUES(%s,%s,%s,%s);
    """
    print("Initiating data insertion in customer_leaderboard table. ")
    db.insert_many_records(connection,sql,highest_purchase)
    print("Data insertion in customer leaderboard table is complete. ")

    
