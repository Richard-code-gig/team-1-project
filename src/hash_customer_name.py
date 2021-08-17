import pandas as pd

def hash_customer_name(df):
    col = 'customer_hash'
    for i in range(len(df[col])):
        customer_name = df[col].iloc[i]
        df[col].iloc[i] = hash(customer_name)
    return df
