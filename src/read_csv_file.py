import pandas as pd

def read_csv_file(filename):
    df = pd.read_csv(filename, header=None, names=[
        'date', 'location', 'customer_hash', 
        'order', 'payment_type', 'total_price', 'card_no'
        ], skip_blank_lines=True, na_filter=True,
        parse_dates=['date'], index_col='date', infer_datetime_format=True).dropna(how='any') 
    
    return df