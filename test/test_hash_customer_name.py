import pandas as pd
from src.data_sensitivity.hash_customer_name import hash_customer_name, hash_64

mock_data = {'ID': [1,2,3], 'customer_hash': ['Dave', 'Jane','Dave']}

mock_df = pd.DataFrame(data=mock_data)

def test_hash_64():
    assert hash_64('Dave') != 'Dave'

def test_hash_customer_name():
    df = hash_customer_name(mock_df)
    assert df['customer_hash'].iloc[0] == hash_64('Dave')
    assert df['customer_hash'].iloc[1] == hash_64('Jane')

def test_consistant_hash():
    df = hash_customer_name(mock_df)
    assert df['customer_hash'].iloc[0] == df['customer_hash'].iloc[2]

def test_diffrent_hash():
    df = hash_customer_name(mock_df)
    assert df['customer_hash'].iloc[0] != df['customer_hash'].iloc[1]