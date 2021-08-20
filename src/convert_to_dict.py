#depends on transform

def convert_order_to_dict(data): #converts from DF to python dict
    df = data
    dic = defaultdict()
    col = 'order'
    for i, _ in enumerate(df[col]):
        dic[df['customer_hash'].iloc[i]] = df[col].iloc[i]
    return dic

def separate_order_dict(data): #Not implemented
    dic = convert_order_to_dict(data)
    lst = []
    mini_dic = {}
    for key, val in dic.items():
        for i in range(len(val)):
            mini_dic[key] = val[i]
            lst.append((mini_dic))
            mini_dic = {}
    
    return lst

