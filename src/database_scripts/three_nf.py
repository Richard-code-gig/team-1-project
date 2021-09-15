import pandas as pd

"""Functions here are those that are easily presented in 3NF formats with little to no transformation requirements"""

def item_from_db(connection, table, item):
    x = ""
    for i, v in enumerate(item):
        if i < len(item)-1:
            x += v + ', '
        else:
            x += v
    try:
        query = pd.read_sql_query(
        f"""select {x} from {table}""", connection)
        df = pd.DataFrame(query, columns=item)
        return df
    except Exception as e:
        print(e)
        df = pd.DataFrame([])
        return df

# def item_from_db(connection, table, item):
#     try:
#         query = pd.read_sql_query(
#         f"""select {item} from {table}""", connection)
#         df = pd.DataFrame(query, columns=[item])
#         return df
#     except Exception as e:
#         print(e)
#         df = pd.DataFrame([])
#         return df

def df_payments(data, connection, table, item): #loads payments table from df 
    df = data['payment_type'].unique()
    df = pd.DataFrame(df, columns=item)
    df_item = item_from_db(connection, table, item)
    if df_item.empty:
        set_df = df
    else:
        set_df = pd.concat([df,df_item]).drop_duplicates(keep=False)
    return set_df

def df_locations(data, connection, table, item): #loads branches table from df 
    df = data['location'].unique()
    df = pd.DataFrame(df, columns=item)
    df_item = item_from_db(connection, table, item)
    if df_item.empty:
        set_df = df
    else:
        set_df = pd.concat([df,df_item]).drop_duplicates(keep=False)
    return set_df

def df_customers(data, connection, table, item): #loads customers table from df
    df = data['customer_hash'].unique()
    df = pd.DataFrame(df, columns=item)
    df_item = item_from_db(connection, table, item)
    if df_item.empty:
        set_df = df
    else:
        set_df = pd.concat([df,df_item]).drop_duplicates(keep=False)
    return set_df