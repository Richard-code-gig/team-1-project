import hashlib

def hash_64(name):
    encoded = name.encode()
    result = hashlib.sha256(encoded)
    return result.hexdigest()

def hash_customer_name(df):
    col = 'customer_hash'
    df[col] = df[col].apply(hash_64)
    return df