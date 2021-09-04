import pandas as pd

"""Functions here are those that are easily presented in 3NF formats with little to no transformation requirements"""

def df_payments(data): #loads payments table from df 
    df = data['payment_type'].unique()
    df = pd.DataFrame(df)
    return df

def df_branches(data): #loads branches table from df 
    df = data['location'].unique()
    df = pd.DataFrame(df)
    return df

def df_customers(data): #loads customers table from df
    d = data['customer_hash'].unique()
    d = pd.DataFrame(d)
    return d