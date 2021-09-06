import boto3
from io import StringIO
from src.read_csv_file import read_csv_file

def handel(event, context):
    # gets key and bucket information from event
    key = event['Records'][0]['s3']['object']['key']
    bucket = event['Records'][0]['s3']['bucket']['name']
    
    #Use boto3 to access file in s3 bucket
    client = boto3.client('s3')
    s3_object = client.get_object(Bucket = bucket, Key = key)
    data = s3_object['Body'].read().decode('utf-8')
    
    df = read_csv_file(StringIO(data))
    print(df)
    