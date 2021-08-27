import psycopg2

def create_db_connection():
    connection = psycopg2.connect(
                    host="localhost", 
                    user="root", 
                    password="password",
                    database="team_1_group_project"
                    ) 

    return connection

