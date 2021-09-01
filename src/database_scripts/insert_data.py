import psycopg2
from psycopg2.extras import RealDictCursor
import csv
from io import BytesIO


def create_db_connection(): #initiates connection
    connection = psycopg2.connect(
                    host="localhost", 
                    user="root", 
                    password="password",
                    database="team_1_group_project"
                    ) 
    connection.autocommit = True
    connection.cursor_factory = RealDictCursor
    cur = connection.cursor()
    return connection, cur


def single_inserts(connection, df, cur): #loads all data from df 
    data = BytesIO()
    df.index += 1
    df.to_csv(data, sep='\t', header = True, index = True)
    data.seek(0)
    load_data = "Copy raw_table from STDOUT csv DELIMITER '\t' NULL '' ESCAPE '\\' HEADER"
    cur.copy_expert(load_data, data)
    connection.commit()
    
    
def insert_products(connection, df, cur): #loads products table from df 
    data = BytesIO()
    df.index += 1
    df.to_csv(data, sep='\t', header = True, index = True)
    data.seek(0)
    load_data = "Copy products from STDOUT csv DELIMITER '\t' NULL '' ESCAPE '\\' HEADER"
    cur.copy_expert(load_data, data)
    connection.commit()
    
        
def insert_customer(connection): #insert data to customer table
    try:
        with connection.cursor() as cursor:
            sql = """INSERT INTO customers (customer_hash) SELECT DISTINCT customer_hash from raw_table"""
            cursor.execute(sql)
            connection.commit()
            cursor.close()
    except Exception as e:
        print(e)
        
def insert_payment(connection): #insert data to payment table
    try:
        with connection.cursor() as cursor:
            sql = """INSERT INTO payment (payment_id, payment_type) SELECT transaction_id, payment_type from raw_table"""
            cursor.execute(sql)
            connection.commit()
            cursor.close()
    except Exception as e:
        print(e) 
        
def insert_customer_order(connection, df, cur): #loads first_customer_table data from df
    data = BytesIO()
    df.to_csv(data, sep='\t', header = True, index = True)
    data.seek(0)
    load_data = "Copy first_customer_table from STDOUT csv DELIMITER '\t' NULL '' ESCAPE '\\' HEADER"
    cur.copy_expert(load_data, data)
    connection.commit()
    
def order_table_all(connection): #Not implemented yet
    try:
        with connection.cursor() as cursor:
            sql = """CREATE VIEW new_table
                AS
                SELECT  t1.raw_id, t1.customer_hash, t1.orders, t1.price, t2.product_id
                FROM customer_table t1
                    JOIN products t2
                        ON t1.orders = t2.product_name"""
            cursor.execute(sql)
            connection.commit()
            cursor.close()
    except Exception as e:
        print(e)
    
def insert_full_customer(connection): #adds product_id to first_customer_table using implicit join
    try:
        with connection.cursor() as cursor:
            sql = """INSERT INTO customer_table (customer_hash, orders, price, product_id)
                SELECT t1.customer_hash, t1.orders, t1.price, t2.product_id
                FROM first_customer_table t1, products t2
                WHERE t1.orders = t2.product_name"""
                    
                    #JOIN products t2
                        #ON t1.orders = t2.product_name
            cursor.execute(sql)
            connection.commit()
            cursor.close()
    except Exception as e:
        print(e)

def insert_orders(connection): #adds transaction_id to customer_table
    try:
        with connection.cursor() as cursor:
            sql = """INSERT INTO orders (order_id, customer_hash, customer_id, order_time, total) 
                SELECT t2.transaction_id, t1.customer_hash, t1.customer_id, t2.date, t2.total_price
                FROM customers t1
                JOIN raw_table t2
                ON t1.customer_hash = t2.customer_hash"""
            cursor.execute(sql)
            connection.commit()
            cursor.close()
    except Exception as e:
        print(e)
        
def insert_order_product(connection): #adds transaction_id to customer_table
    try:
        with connection.cursor() as cursor:
            sql = """INSERT INTO order_product (order_id, product_id, quantity) 
                SELECT t1.order_id, t2.product_id, t3.quantity
                FROM orders t1
                JOIN customer_table t2
                USING (customer_hash)
                JOIN (SELECT customer_hash, COUNT (*) AS quantity FROM customer_table GROUP BY customer_hash) t3
                USING (customer_hash)"""
            cursor.execute(sql)
            connection.commit()
            cursor.close()
    except Exception as e:
        print(e)

def insert_updated_customer(connection): #adds transaction_id to customer_table
    try:
        with connection.cursor() as cursor:
            sql = """INSERT INTO new_customer_table (transaction_id, customer_hash, orders, price, product_id) 
                SELECT t2.transaction_id, t1.customer_hash, t1.orders, t1.price, t1.product_id
                FROM customer_table t1
                JOIN raw_table t2
                ON t1.customer_hash = t2.customer_hash"""
            cursor.execute(sql)
            connection.commit()
            cursor.close()
    except Exception as e:
        print(e)