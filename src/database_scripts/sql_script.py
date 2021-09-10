def create_customer_table(connection):
    try:
        with connection.cursor() as cursor:
            sql = ''' CREATE TABLE IF NOT EXISTS customers
            (
                customer_id INT IDENTITY(1,1) NOT NULL, 
                customer_hash VARCHAR(150) NOT NULL,
                PRIMARY KEY(customer_id)
            )'''
            cursor.execute(sql)
            connection.commit()
            cursor.close()
    except Exception as e:
        print(e)
        
def create_location_table(connection):
    try:
        with connection.cursor() as cursor:
            sql = ''' CREATE TABLE IF NOT EXISTS locations
            (
                location_id INT IDENTITY(1,1) NOT NULL,
                location VARCHAR(50) NOT NULL,
                PRIMARY KEY(location_id)
            )'''
            cursor.execute(sql)
            connection.commit()
            cursor.close()
    except Exception as e:
        print(e)
        
def create_payment_table(connection):
    try:
        with connection.cursor() as cursor:
            sql = ''' CREATE TABLE IF NOT EXISTS payments
            (
                payment_id INT IDENTITY(1,1) NOT NULL,
                payment_type VARCHAR(20) NOT NULL,
                PRIMARY KEY(payment_id)
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
                product_id INT IDENTITY(1,1) NOT NULL,
                product_name VARCHAR(100) NOT NULL,
                product_price FLOAT4 NOT NULL,
                PRIMARY KEY(product_id)
            )'''
            cursor.execute(sql)  #FLOAT4 is LIKE SMALL FLOAT
            connection.commit()
            cursor.close()
    except Exception as e:
        print(e)

def create_orders_table(connection):
    try:
        with connection.cursor() as cursor:
            sql = ''' CREATE TABLE IF NOT EXISTS orders
            (
                order_id INT IDENTITY(1,1) NOT NULL,
                customer_id INT NOT NULL,
                    CONSTRAINT fk_customers
                        FOREIGN KEY(customer_id) 
                            REFERENCES customers(customer_id),
                date VARCHAR(150) NOT NULL,
                payment_id INT NOT NULL,
                location_id INT NOT NULL,
                amount_paid FLOAT4 NOT NULL,
                PRIMARY KEY(order_id)
            )'''
            cursor.execute(sql)
            connection.commit()
            cursor.close()
    except Exception as e:
        print(e) 

def create_order_product_table(connection):
    try:
        with connection.cursor() as cursor:
            sql = ''' CREATE TABLE IF NOT EXISTS order_products
            (
                order_id INT,
                    CONSTRAINT fk_orders
                        FOREIGN KEY(order_id) 
                            REFERENCES orders(order_id),
                product_id INT, 
                    CONSTRAINT fk_products
                        FOREIGN KEY(product_id) 
                            REFERENCES products(product_id),     
                quantity INT NOT NULL
            )'''
            cursor.execute(sql)
            connection.commit()
            cursor.close()
    except Exception as e:
        print(e) 
        
def alter_raw_table(connection): #handy to alter or drop tables. Only implemented when needed
    try:
        with connection.cursor() as cursor:
            sql = """DROP TABLE customers, locations, payments, products, orders, order_products"""
            cursor.execute(sql)
            connection.commit()
            cursor.close()
    except Exception as e:
        print(e)