from collections import defaultdict
import pandas as pd
from src.database_scripts.create_connection import create_db_connection

"""Functions here do moderate to intense transformation of data.
They use raw data from dataframe or send SELECT queries to database for data needed.
Data transitions between dataframe, dictionaries, and a list of tuples,
the final format needed by the execute_values of psycopg2 in insert_data.py file"""

connection = create_db_connection()

def chunk(lst, n): #chunks a list in multiples of n integer
    return [lst[i:i + n] for i in range(0, len(lst), n)]

def transform(data):
    df = data #This returned hashed name
    col = 'order'
    df[col] = df[col].apply(lambda x: x.split(',')) #same as str.split(',')
    df[col] = df[col].apply(lambda x: chunk(x, 3))  #split in 3's
    return df

def convert_order_to_dict(data): #converts order DF to python dict
    df = data
    dic = defaultdict()
    col = 'order'
    for i, _ in enumerate(df[col]):
        dic[df['customer_hash'].iloc[i]] = df[col].iloc[i]
    return dic

def convert_to_DF(data): #converts back to DF
    dic = data
    dic_list = [(key, *i) for key,value in dic.items() for i in value] #converts dict to list of tuples
    df = pd.DataFrame(dic_list, columns=['customer_hash','Size','Type','Price'])
    orders = df[['Size', 'Type']].apply(lambda x: ' '.join(x), axis=1) #joins size with order type
    df.insert(1, 'Orders', orders) #make price column last
    df.drop(columns=['Size', 'Type'], inplace=True)
    return df
    
def convert_items_for_db(data, col): #converts from DF to python dict
    df = data
    lst = []
    for i, _ in enumerate(df[col]):
        lst.append(df[col].iloc[i])
    return lst

def convert_df_to_dict(data): #Converts any DF to list of tuples needed by psycopg2 execute_values
    arr = data.values
    df = [tuple(i) for i in arr]
    return df
    
def group_order_product(data):
    data['Price'] = data['Price'].apply(float)
    dic = defaultdict(float)
    col = data['customer_hash']
    for i, v in enumerate(col):
        dic[v] += data['Price'].iloc[i]
    df = pd.DataFrame(dic.items(), columns=['customer_hash','total'])
    return df

def group_product(data):
    pass
    df = data.groupby(['Orders', 'Price']).size().reset_index(name='total_quantity')
    df.columns = ['Orders', 'Price', 'total_quantity']
    df.drop(columns ='total_quantity', inplace=True)
    return df

def get_customer_from_db(connection):
    try:
        query_customer = pd.read_sql_query(
        """select *
        from customers""", connection)

        df_customer = pd.DataFrame(query_customer, columns=['customer_id', 'customer_hash'])
        return df_customer
    except Exception as e:
        print(e)

def get_location_from_db(connection):
    try:
        query_location = pd.read_sql_query(
        """select *
        from locations""", connection)

        df_location = pd.DataFrame(query_location, columns=['location_id', 'location'])
        return df_location
    except Exception as e:
        print(e)
        
def get_payment_from_db(connection):
    try:
        query_payment = pd.read_sql_query(
        """select *
        from payments""", connection)

        df_payment = pd.DataFrame(query_payment, columns=['payment_id', 'payment_type'])
        return df_payment
    except Exception as e:
        print(e)
        
def get_product_from_db(connection):
    try:
        query_product = pd.read_sql_query(
        """select *
        from products""", connection)

        df_product = pd.DataFrame(query_product, columns=['product_id', 'product_name', 'product_price'])
        df_product.rename(columns={'product_name':'Orders'}, inplace = True)
        return df_product
    except Exception as e:
        print(e)

def get_order_id_from_db(connection):
    try:
        query_order = pd.read_sql_query(
        """select o.order_id, c.customer_id, c.customer_hash
        from orders o
        join customers c
        on o.customer_id = c.customer_id""", connection)

        df_order = pd.DataFrame(query_order, columns=['order_id', 'customer_id', 'customer_hash'])
        return df_order
    except Exception as e:
        print(e)