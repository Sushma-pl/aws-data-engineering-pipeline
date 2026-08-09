import boto3
from botocore.client import Config

s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='admin',
    aws_secret_access_key='minioadmin',
    config=Config(signature_version='s3v4')
)

response = s3.list_buckets()

for buckets in response['Buckets']:
    print(f'Bucket Name: {buckets["Name"]}')