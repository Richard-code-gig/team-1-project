from collections import defaultdict
import pandas as pd

def chunk(lst, n): #chunks a list in multiples of n integer
    return [lst[i:i + n] for i in range(0, len(lst), n)]

def transform(data):
    df = data #This returned hashed name
    col = 'order'
    df[col] = df[col].apply(lambda x: x.split(',')) #same as str.split(',')
    df[col] = df[col].apply(lambda x: chunk(x, 3))  #split in 3's
    return df

def convert_order_to_dict(data): #converts from DF to python dict
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
    df.index += 1
    return df

def group_product(data):
    df = data.groupby(['Orders', 'Price']).size().reset_index(name='total_quantity')
    df.columns = ['product_name', 'product_price', 'total_quantity']
    return df
    