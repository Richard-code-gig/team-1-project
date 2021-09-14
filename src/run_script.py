from database_scripts import sql_script as sql
from database_scripts import three_nf
from read_csv_file import read_csv_file as reader
from database_scripts.remove_card_data import remove_card_data as remove
from database_scripts.create_connection import create_db_connection
import hash_customer_name as name
import transformation as transform
import database_scripts.insert_data as insert
import merger as merger
import pandas as pd
import numpy as np 
from psycopg2.extensions import register_adapter, AsIs
from time import sleep

"""We will use pd.values in the programme so we need to put them in a format psycopg2 understands"""

sql.create_database()

def allow_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)

def allow_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)

register_adapter(np.float64, allow_numpy_float64)
register_adapter(np.int64, allow_numpy_int64)

def etl(filename):      
    connection = create_db_connection()

    sql.create_customer_table(connection)
    sql.create_payment_table(connection)
    sql.create_location_table(connection)
    sql.create_product_table(connection)
    sql.create_orders_table(connection)
    sql.create_order_product_table(connection)

    raw_data = reader(filename)
    hash_name = name.hash_customer_name(raw_data) 
    remove_card_data = remove(hash_name)
    
    customer_3NF = three_nf.df_customers(remove_card_data)
    payment_3NF = three_nf.df_payments(remove_card_data)
    location_3NF = three_nf.df_locations(remove_card_data)
    
    split_order_col = transform.transform(remove_card_data)
    convert_split_to_dict = transform.convert_order_to_dict(split_order_col)
    data_for_db_cus = transform.convert_3NF_items_for_db(customer_3NF, 'customer_hash')
    data_for_db_pay = transform.convert_3NF_items_for_db(payment_3NF, 'payment_type')
    data_for_db_loc = transform.convert_3NF_items_for_db(location_3NF, 'location')
    return_dict_to_df = transform.convert_to_DF(convert_split_to_dict)
    merge_product = transform.group_product(return_dict_to_df)
    data_prod_dict = transform.convert_df_to_dict(merge_product)
    
    insert.insert_customers(connection, data_for_db_cus)
    insert.insert_payments(connection, data_for_db_pay)
    insert.insert_locations(connection, data_for_db_loc)
    insert.insert_products(connection, data_prod_dict)

    sleep(0.005)
    customer_db = transform.get_customer_from_db(connection) 
    payment_db = transform.get_payment_from_db(connection) 
    location_db = transform.get_location_from_db(connection) 
    product_db = transform.get_product_from_db(connection)
    
    sleep(0.005)
    product_db.rename(columns={'product_name':'Orders'})
    df_order_db_loc = merger.order_for_db_loc(remove_card_data, location_db, customer_db)
    df_order_db_pay = merger.order_for_db_pay(payment_db, df_order_db_loc)
    data_ord_dict = merger.ready_order_db(df_order_db_pay, transform.convert_df_to_dict) 
    
    insert.insert_orders(connection, data_ord_dict)

    sleep(0.005)
    tab_order_id = transform.get_order_id_from_db(connection)
    data_ord_prod_dict = merger.ready_ord_prod_db(product_db, return_dict_to_df, tab_order_id, transform.convert_df_to_dict)
    
    insert.insert_order_product(connection, data_ord_prod_dict)
    connection.close()
   
etl('new_file.csv')