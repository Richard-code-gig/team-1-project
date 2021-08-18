import sql_script as sql
from create_connection import create_db_connection



sql.create_database()
connection = create_db_connection()      
sql.create_customer_table(connection)
sql.create_product_table(connection)
sql.create_payment_method_table(connection)
sql.create_orders_table(connection)
sql.create_order_product_table(connection)

connection.close()