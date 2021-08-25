import pandas as pd

def remove_card_data(df):
    '''Drops "card_no" column from the df'''
    drop = ['card_no']
    df.drop(columns = drop, inplace=True)
    return df

