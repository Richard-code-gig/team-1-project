import boto3
import src.run_script as run_script
from io import StringIO

def lambda_handler(event, context):
    
    key = event['Records'][0]['s3']['object']['key'] #file name
    bucket = event['Records'][0]['s3']['bucket']['name'] #bucket name
    s3_client = boto3.client('s3')
    response_object = s3_client.get_object(Bucket=bucket, Key=key)
    raw_csv_object = response_object['Body'].read().decode('utf-8')
    if raw_csv_object:
        run_script.etl(StringIO(raw_csv_object))
        return 'Successful'
    else:
        return 'Failed'
