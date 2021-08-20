#depends on hash_customer_name

def chunk(lst, n): #chunks a list in multiples of n integer
    return [lst[i:i + n] for i in range(0, len(lst), n)]

def transform(data):
    df = data #This returned hashed name
    col = 'order'
    df[col] = df[col].apply(lambda x: x.split(',')) #same as str.split(',')
    df[col] = df[col].apply(lambda x: chunk(x, 3))  #split in 3's
    return df

