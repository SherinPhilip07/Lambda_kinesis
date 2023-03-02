import boto3
import urllib
import csv
import pandas as pd

def lambda_handler(event, context):

    s3_cient = boto3.client('s3')
    dynamo_db = boto3.resource('dynamodb')
    table = dynamo_db.Table('test')
    
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    s3_file_name = event["Records"][0]["s3"]["object"]["key"]
    resp = s3_cient.get_object(Bucket=bucket_name, Key=s3_file_name)
    books_df = pd.read_csv(resp.get("Body"))
    books_df=books_df.drop(['TRD_DATE'],axis=1)
    print(books_df)