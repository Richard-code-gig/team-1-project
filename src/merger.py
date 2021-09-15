import pandas as pd

"""The functions here make merges between dataframes in same manner in which SQL join statements work"""

def order_for_db_loc(data2, location_db, customer_db):
    df2 = pd.merge(data2, customer_db, on='customer_hash', how='left') 
    df_order_db_loc = pd.merge(df2, location_db, on='location', how='left')
    return df_order_db_loc

def order_for_db_pay(payment_db, df_order_db_loc):
    df_order_db_pay = pd.merge(df_order_db_loc, payment_db, on='payment_type', how='left') 
    return df_order_db_pay

def ready_order_db(df_order_db_payment, convert_df_to_dic):
    df_order_db_payment['date'] = df_order_db_payment['date'].astype(str)
    df = df_order_db_payment[['customer_id', 'date', 'payment_id', 'location_id', 'total_price']]
    dic = convert_df_to_dic(df)
    return dic 

def ready_ord_prod_db(product_db, data6, tab_order_id, convert_df_to_dic):
    product_db.rename(columns={'product_name':'Orders'}, inplace = True)
    df7 = pd.merge(product_db, data6, on='Orders', how='right')
    df8 = pd.merge(tab_order_id, df7, on='customer_hash', how='right')
    df = df8.groupby(['order_id', 'product_id'])['product_id'].size().reset_index(name='quantity')
    dic = convert_df_to_dic(df)
    return dic