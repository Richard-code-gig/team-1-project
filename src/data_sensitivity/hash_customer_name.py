import hashlib

def hash_64(name):
    '''Hash value "name" using sha256'''
    encoded = name.encode()
    result = hashlib.sha256(encoded)
    return result.hexdigest()

def hash_customer_name(df):
    '''Replaces values of "customer_hash" in the df with their hashed version'''
    col = 'customer_hash'
    df[col] = df[col].apply(hash_64)
    return df
