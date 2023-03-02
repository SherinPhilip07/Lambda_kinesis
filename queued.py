import boto3
import urllib
import json
import csv
import pandas as pd
from io import StringIO

def lambda_handler(event, context):
    s3_cient = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    buck="outputbuckk"
    message = event["Records"][0]["body"]
    message = json.loads(message)
    message = message['Message']
    message = json.loads(message)
    s3_file_name = message['Records'][0]["s3"]["object"]["key"]
    bucket_name = message['Records'][0]["s3"]["bucket"]["name"]
    print(s3_file_name,bucket_name)
    resp = s3_cient.get_object(Bucket=bucket_name, Key=s3_file_name)
    books_df = pd.read_csv(resp.get("Body"))
    # books_df=books_df.drop(['TRD_DATE'],axis=1)
    books_df=books_df['CLIENT_PAN'].str.lower()
    
    csv_buffer = StringIO()
    books_df.to_csv(csv_buffer)
    s3_resource.Object(buck, 'books_df.csv').put(Body=csv_buffer.getvalue())
    print("Successfully inserted")