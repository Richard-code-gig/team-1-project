#depends on convert_to_DF 

def order_quantity(data): #Returns the count of orders made by customers
    df = data
    df = df.groupby(['customer_hash']).size().reset_index(name='count')
    return df