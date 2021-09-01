from src.database_scripts import sql_script as sql
from src.database_scripts.create_connection import create_db_connection
from src.database_scripts.insert_data import create_db_connection as create
from src.database_scripts.insert_data import single_inserts, insert_customer, insert_payment, insert_products, insert_customer_order, order_table_all, insert_orders, insert_order_product, insert_full_customer, insert_updated_customer
import src.hash_customer_name as name
from src.read_csv_file import read_csv_file as reader
from src.database_scripts.remove_card_data import remove_card_data as remover
from src.transformation import transform, convert_order_to_dict, convert_to_DF, group_product

sql.create_database()
connection = create_db_connection()      
sql.create_customer_table(connection)
sql.create_product_table(connection)
sql.alter_product_table(connection)
sql.create_payment_method_table(connection)
sql.create_orders_table(connection)
sql.create_order_product_table(connection)
sql.create_raw_table(connection)
sql.create_first_customer(connection)
sql.create_full_customer(connection)
sql.create_updated_customer(connection)
# sql.alter_raw_table(connection) #Not implemented until needed

data = reader('isle-of-wight.csv')
data2 = name.hash_customer_name(data)
data3 = remover(data2)
data4 = transform(data3)
data5 = convert_order_to_dict(data4)
data6 = convert_to_DF(data5)
data7 = group_product(data6)

connection, cur = create()
single_inserts(connection, data3, cur)
insert_products(connection, data7, cur)
insert_customer(connection)
insert_payment(connection)
insert_customer_order(connection, data6, cur)
insert_orders(connection)
insert_order_product(connection)
order_table_all(connection)
insert_full_customer(connection)
insert_updated_customer(connection)
connection.close()
