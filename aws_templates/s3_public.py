import boto3
import json
import pandas as pd

region_name = ['ap-south-1', 'us-east-1']
df = pd.read_csv('Cloud_awscloud_202209211754.csv')
temp_list = []
for index, row in df.iterrows():
    role_arn = row['arn']
    session_name = str(row['account_id'])
    client = boto3.client('sts')
    cred = client.assume_role(RoleArn=role_arn,RoleSessionName=session_name)
    cred = cred['Credentials']
    cred = dict(aws_access_key_id=cred["AccessKeyId"],aws_secret_access_key=cred["SecretAccessKey"],aws_session_token=cred["SessionToken"],region_name=region_name[0])
    new_client = boto3.client('s3', **cred)
    s3_bucket_dict = new_client.list_buckets()
    owner_id = s3_bucket_dict['Owner']['ID']
    bucket_list = s3_bucket_dict['Buckets']
    for i in bucket_list:
        bucket_name = i['Name']
        response = new_client.get_bucket_policy_status(Bucket=bucket_name)
        response['bucket_name'] = bucket_name
        temp_list.append(response)
with open('s3_public_bucket_response.json', 'w') as f:
    json.dump(temp_list, f, default=str)
