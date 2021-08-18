import base64

def hash_64(name):
    name_bytes = name.encode('ascii')
    base64_bytes = base64.b64encode(name_bytes)
    base64_name = base64_bytes.decode('ascii')
    return base64_name

def hash_customer_name(df):
    col = 'customer_hash'
    for i in range(len(df[col])):
        customer_name = df.loc[i,col]
        df.loc[i,col] = hash_64(customer_name)
    return df
