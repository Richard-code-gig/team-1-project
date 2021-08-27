from src.database_scripts.create_connection import create_db_connection
from src.read_csv_file import read_csv_file
from src.data_sensitivity.hash_customer_name import hash_customer_name
from src.data_sensitivity.remove_card_data import remove_card_data
import src.database_scripts.insert_data as raw

def etl(file):
    
    raw_data = read_csv_file(file)

    card_removed_data = remove_card_data(raw_data)

    data = hash_customer_name(card_removed_data)
    print(data)
    connection, cur = raw.create_db_connection()

    raw.single_inserts(connection, data, cur)
