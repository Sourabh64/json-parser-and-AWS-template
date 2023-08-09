import boto3
import json
import pandas as pd

region_names = ['ap-south-1', 'us-east-1']
df = pd.read_csv('Cloud_awscloud_202209211754.csv')
temp_list = []
for index, row in df.iterrows():
    role_arn = row['arn']
    session_name = str(row['account_id'])
    for region in region_names:
        client = boto3.client('sts', region_name=region)
        cred = client.assume_role(RoleArn=role_arn, RoleSessionName=session_name)
        cred = cred['Credentials']
        try:
            cred = dict(aws_access_key_id=cred["AccessKeyId"], aws_secret_access_key=cred["SecretAccessKey"], aws_session_token=cred["SessionToken"], region_name=region)
            subnet_client = boto3.client('ec2', **cred)
            subnet_dict = subnet_client.describe_subnets()
            subnet_dict['account_id'] = session_name
            temp_list.append(subnet_dict)
        except Exception as e:
            print(e)
with open('json_responses/describe_subnets.json', 'w') as f:
    json.dump(temp_list, f, default=str)
