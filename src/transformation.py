from collections import defaultdict
import pandas as pd
from database_scripts.three_nf import item_from_db

"""Functions here do moderate to intense transformation of data.
They use raw data from dataframe or send SELECT queries to database for data needed.
Data transitions between dataframe, dictionaries, and a list of tuples,
the final format needed by the execute_values of psycopg2 in insert_data.py file"""

def chunk(lst, n): #chunks a list in multiples of n integer
    return [lst[i:i + n] for i in range(0, len(lst), n)]

def transform(df): #This returned hashed name
    col = 'order'
    df[col] = df[col].str.split(',')
    df[col] = df[col].apply(lambda x: chunk(x, 1))  #split in 3's
    return df

def convert_order_to_dict(df): #converts order DF to python dict
    dic = defaultdict()
    col = 'order'
    for i, _ in enumerate(df[col]):
        try:
            dic[df['customer_hash'].iloc[i]] += df[col].iloc[i]
        except KeyError:
            dic[df['customer_hash'].iloc[i]] = df[col].iloc[i]
            
    new_dic = defaultdict()
    for k, v in dic.items():
        new_dic[k] = [in_lst.rsplit('-', 1) for out_lst in v for in_lst in out_lst]
    return new_dic

def convert_to_DF(dic): #converts back to DF
    dic_list = [(key, *i) for key,value in dic.items() for i in value] #converts dict to list of tuples
    df = pd.DataFrame(dic_list, columns=['customer_hash','Orders','Price'])
    df['Orders'] = df['Orders'].str.strip()
    df['Price'] = df['Price'].astype(float)
    return df
    
def convert_3NF_items_for_db(df, col): #converts from DF to python list
    lst = []
    for i, _ in enumerate(df[col]):
        lst.append(df[col].iloc[i])
    return lst

def convert_df_to_dict(data): #Converts any DF to list of tuples needed by psycopg2 execute_values
    arr = data.values
    df = [tuple(i) for i in arr]
    return df

def group_product(data6, connection, table, item):
    df = data6.groupby(['Orders', 'Price']).size().reset_index(name='total_quantity')
    df_item = item_from_db(connection, table, item)
    df_item.columns = ['Orders', 'Price']
    df.columns = ['Orders', 'Price', 'total_quantity']
    df.drop(columns ='total_quantity', inplace=True)
    df['Orders'] = df['Orders'].str.strip()
    df = df.drop_duplicates()
    df_item['Orders'] = df_item['Orders'].str.strip()
    if df_item.dropna().empty:
        set_df = df
    else:
        df_both = df_item.merge(df.drop_duplicates(), on=['Orders', 'Price'], how='right', indicator=True)
        set_df = df_both[df_both['_merge'] == 'right_only']
        set_df = set_df.drop(columns=['_merge'])
    return set_df
        
    return set_df
        
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