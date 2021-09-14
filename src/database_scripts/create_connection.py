import psycopg2, boto3

# client = boto3.client('redshift', region_name='eu-west-1')
    
# REDSHIFT_USER = "awsuser"
# REDSHIFT_CLUSTER = "redshiftcluster-fbtitpjkbelw"
# REDSHIFT_HOST = "redshiftcluster-fbtitpjkbelw.cnvqpqjunvdy.eu-west-1.redshift.amazonaws.com"
# REDSHIFT_DATABASE = "team1db"
    
def create_db_connection():
    # creds = client.get_cluster_credentials(
    # DbUser=REDSHIFT_USER,
    # DbName=REDSHIFT_DATABASE,
    # ClusterIdentifier=REDSHIFT_CLUSTER,
    # DurationSeconds=3600)
    
    # conn = psycopg2.connect(
    # user=creds['DbUser'], 
    # password=creds['DbPassword'],
    # host=REDSHIFT_HOST,
    # database=REDSHIFT_DATABASE,   
    # port=5439
    # )
    # return conn
    connection = psycopg2.connect(
                    host="localhost", 
                    user="root", 
                    password="password",
                    database="team_1_group_project"
                    ) 

    return connection