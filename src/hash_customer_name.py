import hashlib

def hash_64(name):
    encoded = name.encode()
    result = hashlib.sha256(encoded)
    return result.hexdigest()

def hash_customer_name(df):
    col = 'customer_hash'
    for i, _ in enumerate(df[col]):
        df.loc[i, col] = hash_64(df.loc[i,col])
    return df