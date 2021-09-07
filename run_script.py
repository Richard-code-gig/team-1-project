from src.database_scripts import sql_script as sql
from src.database_scripts.create_connection import create_db_connection
import src.database_scripts.insert_data as insert
import src.transformation as transform
import src.hash_customer_name as name
from src.read_csv_file import read_csv_file as reader
from src.database_scripts.remove_card_data import remove_card_data as remover
import merger
import pandas as pd
import numpy as np 
from psycopg2.extensions import register_adapter, AsIs

"""We will use pd.values in the programme so we need to put them in a format psycopg2 understands"""

def allow_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)

def allow_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)

register_adapter(np.float64, allow_numpy_float64)
register_adapter(np.int64, allow_numpy_int64)


sql.create_database()
connection = create_db_connection()      

sql.create_customer_table(connection)
sql.create_payment_table(connection)
sql.create_location_table(connection)
sql.create_product_table(connection)
sql.create_orders_table(connection)
sql.create_order_product_table(connection)

data = reader('new_file.csv')
data2 = name.hash_customer_name(data) 
data3 = remover(data2)
data4 = transform.transform(data3)
data5 = transform.convert_order_to_dict(data4) 
data_for_db_pay = transform.convert_items_for_db(data4, 'payment_type')
data_for_db_loc = transform.convert_items_for_db(data4, 'location')
data6 = transform.convert_to_DF(data5)
data7 = transform.group_product(data6)
data_prod_dict = transform.convert_df_to_dict(data7)
product_db = transform.get_product_from_db(connection)
product_db.rename(columns={'product_name':'Orders'})
quantity = merger.ord_prod_table(data6) # gets customer_id, hash
tab_order_id = transform.get_order_id_from_db(connection)

customer_db = transform.get_customer_from_db(connection) 
payment_db = transform.get_payment_from_db(connection) 
location_db = transform.get_location_from_db(connection) 

df_order_db_loc = merger.order_for_db_loc(data2, location_db, customer_db) 
df_order_db_pay = merger.order_for_db_pay(payment_db, df_order_db_loc)

data_ord_dict = merger.ready_order_db(df_order_db_pay, transform.convert_df_to_dict) 
data_ord_prod_dict = merger.ready_ord_prod_db(product_db, data6, tab_order_id, quantity, transform.convert_df_to_dict)
insert.insert_customers(connection, data5)
insert.insert_payments(connection, data_for_db_pay)
insert.insert_locations(connection, data_for_db_loc)
insert.insert_products(connection, data_prod_dict)
insert.insert_orders(connection, data_ord_dict)
insert.insert_order_product(connection, data_ord_prod_dict)

connection.close()