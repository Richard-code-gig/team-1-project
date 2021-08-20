#depends on convert_order_to_dict 

def convert_to_DF(data): #converts back to DF
    dic = data
    dic_list = [(key, *i) for key,value in dic.items() for i in value] #converts dict to list of tuples
    df = pd.DataFrame(dic_list, columns=['customer_hash','Size','Type','Price'])
    orders = df[['Size', 'Type']].apply(lambda x: ' '.join(x), axis=1) #joins size with order type
    df.insert(1, 'Orders', orders) #make price column last
    df.drop(columns=['Size', 'Type'], inplace=True)
    return df

