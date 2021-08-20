import pandas as pd

def remove_card_data(df):
    drop = ['card_no']
    df.drop(columns = drop, inplace=True)
    return df

