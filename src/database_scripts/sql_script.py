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
                customer_id SERIAL PRIMARY KEY, 
                customer_hash VARCHAR(255) NOT NULL
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
                order_id SERIAL PRIMARY KEY, 
                customer_id INT,
                    CONSTRAINT fk_customers
                        FOREIGN KEY(customer_id) 
                            REFERENCES customers(customer_id),
                order_time TIME NOT NULL,
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
                            REFERENCES orders(order_id),
                product_id INT, 
                    CONSTRAINT fk_products
                        FOREIGN KEY(product_id) 
                            REFERENCES  products(product_id),
                quantity INT NOT NULL
            )'''
            cursor.execute(sql)
            connection.commit()
            cursor.close()
    except Exception as e:
        print(e) 


