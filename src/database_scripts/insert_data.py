import psycopg2
from psycopg2.extras import RealDictCursor
import csv
from io import StringIO, BytesIO


def create_db_connection():
    connection = psycopg2.connect(
                    host="localhost", 
                    user="root", 
                    password="password",
                    database="team_1_group_project"
                    ) 
    connection.autocommit = True
    connection.cursor_factory = RealDictCursor
    cur = connection.cursor()
    return connection, cur


def single_inserts(connection, df, cur):
    data = BytesIO()
    df.to_csv(data, sep='\t', header = True, index = True)
    data.seek(0)
    load_data = "Copy raw_table from STDOUT csv DELIMITER '\t' NULL '' ESCAPE '\\' HEADER"
    cur.copy_expert(load_data, data)
    connection.commit()