import pandas as pd

"""Functions here are those that are easily presented in 3NF formats with little to no transformation requirements"""

def df_payments(data): #loads payments table from df 
    df = data['payment_type'].unique()
    df = pd.DataFrame(df, columns=['payment_type'])
    return df

def df_locations(data): #loads branches table from df 
    df = data['location'].unique()
    df = pd.DataFrame(df, columns=['location'])
    return df

def df_customers(data): #loads customers table from df
    df = data['customer_hash'].unique()
    df = pd.DataFrame(df, columns=['customer_hash'])
    return df