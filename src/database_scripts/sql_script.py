import psycopg2

def create_database():
    connection = psycopg2.connect(
                    host="localhost", 
                    user="root", 
                    password="password",
                    database="postgres"
                    )
    connection.autocommit = True
    try:
        with connection.cursor() as cursor:
            sql = 'CREATE database team_1_group_project'
            
            cursor.execute(sql)
            connection.close()
    except Exception as e:
            print(e)

def create_customer_table(connection):
    try:
        with connection.cursor() as cursor:
            sql = ''' CREATE TABLE IF NOT EXISTS customers
            (
                customer_id SERIAL, 
                customer_hash VARCHAR(255) NOT NULL,
                PRIMARY KEY(customer_id, customer_hash)
            )'''
            cursor.execute(sql)
            connection.commit()
            cursor.close()
    except Exception as e:
        print(e)
        
def create_product_table(connection):
    try:
        with connection.cursor() as cursor:
            sql = '''CREATE TABLE IF NOT EXISTS products
            (
                product_id  SERIAL PRIMARY KEY,
                product_name VARCHAR(255) NOT NULL,
                product_price FLOAT NOT NULL
            )'''
            cursor.execute(sql)
            connection.commit()
            cursor.close()
    except Exception as e:
        print(e)
        
def alter_product_table(connection):
    try:
        with connection.cursor() as cursor:
            sql = "ALTER TABLE products ADD COLUMN customer_hash VARCHAR(255) NOT NULL"
            cursor.execute(sql)
            connection.commit()
            cursor.close()
    except Exception as e:
        print(e)        
        
def create_payment_method_table(connection):
    try:
        with connection.cursor() as cursor:
            sql = ''' CREATE TABLE IF NOT EXISTS payment
            (
                payment_id SERIAL PRIMARY KEY,
                payment_type VARCHAR(255) NOT NULL
            )'''
            cursor.execute(sql)
            connection.commit()
            cursor.close()
    except Exception as e:
        print(e)


def create_orders_table(connection):
    try:
        with connection.cursor() as cursor:
            sql = ''' CREATE TABLE IF NOT EXISTS orders
            (
                order_key SERIAL PRIMARY KEY,
                order_id INT NOT NULL,
                customer_hash VARCHAR(255) NOT NULL,
                customer_id INT NOT NULL,
                    CONSTRAINT fk_customers
                        FOREIGN KEY(customer_hash,customer_id) 
                            REFERENCES customers(customer_hash,customer_id)
                                ON DELETE CASCADE
                                ON UPDATE CASCADE,
                order_time VARCHAR(255) NOT NULL,
                total FLOAT NOT NULL
            )'''
            cursor.execute(sql)
            connection.commit()
            cursor.close()
    except Exception as e:
        print(e) 

def create_order_product_table(connection):
    try:
        with connection.cursor() as cursor:
            sql = ''' CREATE TABLE IF NOT EXISTS order_product
            (
                order_id INT,
                    CONSTRAINT fk_orders
                        FOREIGN KEY(order_id) 
                            REFERENCES orders(order_key)
                                ON DELETE CASCADE
                                ON UPDATE CASCADE,
                product_id INT, 
                    CONSTRAINT fk_products
                        FOREIGN KEY(product_id) 
                            REFERENCES  products(product_id)
                                ON DELETE CASCADE
                                ON UPDATE CASCADE,     
                quantity INT NOT NULL
            )'''
            cursor.execute(sql)
            connection.commit()
            cursor.close()
    except Exception as e:
        print(e) 

def create_raw_table(connection): #loads full table data from df
    try:
        with connection.cursor() as cursor:
            sql = '''CREATE TABLE IF NOT EXISTS raw_table
            (
                order_id SERIAL PRIMARY KEY,
                date VARCHAR(255) NOT NULL,
                location VARCHAR(255) NOT NULL,
                customer_hash VARCHAR(255) NOT NULL,
                orders VARCHAR(255) NOT NULL,
                payment_type VARCHAR(255) NOT NULL,
                total_price FLOAT NOT NULL
            )'''
            cursor.execute(sql)
            connection.commit()
            cursor.close()
    except Exception as e:
        print(e)

def create_first_customer(connection): #loads first_customer_table from df
    try:
        with connection.cursor() as cursor:
            sql = '''CREATE TABLE IF NOT EXISTS first_customer_table
            (
                raw_id SERIAL PRIMARY KEY,
                customer_hash VARCHAR(255) NOT NULL,
                Orders VARCHAR(255) NOT NULL,
                Price FLOAT NOT NULL
            )'''
            cursor.execute(sql)
            connection.commit()
            cursor.close()
    except Exception as e:
        print(e)
        
def create_full_customer(connection): #adds product_id to first_customer_table
    try:
        with connection.cursor() as cursor:
            sql = '''CREATE TABLE IF NOT EXISTS customer_table
            (
                raw_id SERIAL PRIMARY KEY,
                customer_hash VARCHAR(255) NOT NULL,
                Orders VARCHAR(255) NOT NULL,
                Price FLOAT NOT NULL,
                product_id INT NOT NULL
            )'''
            cursor.execute(sql)
            connection.commit()
            cursor.close()
    except Exception as e:
        print(e)
        
def create_updated_customer(connection): #adds transaction_id to customer_table
    try:
        with connection.cursor() as cursor:
            sql = '''CREATE TABLE IF NOT EXISTS new_customer_table
            (
                raw_id SERIAL PRIMARY KEY,
                transaction_id INT,
                    CONSTRAINT fk_rawtable
                        FOREIGN KEY(transaction_id) 
                            REFERENCES raw_table(transaction_id)
                                ON DELETE CASCADE
                                ON UPDATE CASCADE,
                customer_hash VARCHAR(255) NOT NULL,
                Orders VARCHAR(255) NOT NULL,
                Price FLOAT NOT NULL,
                product_id INT NOT NULL,
                    CONSTRAINT fk_product
                        FOREIGN KEY(product_id) 
                            REFERENCES products(product_id)
                                ON DELETE CASCADE
                                ON UPDATE CASCADE
            )'''
            cursor.execute(sql)
            connection.commit()
            cursor.close()
    except Exception as e:
        print(e)
        
def alter_raw_table(connection): #handy to alter or drop tables. Only implemented when needed
    try:
        with connection.cursor() as cursor:
            sql = """DROP TABLE customers, orders, order_product"""
            cursor.execute(sql)
            connection.commit()
            cursor.close()
    except Exception as e:
        print(e)