import time
import json
import boto3
import pandas as pd
from json_parser import Parser
from push_to_db import DB


class AWS:
    def __init__(self):
        pass

    def get_data(self, df):
        region_names = ['ap-south-1', 'us-east-1']
        # df = pd.read_csv('Cloud_awscloud_202209211754.csv')
        temp_list = []
        for index, row in df.iterrows():
            role_arn = row['arn']
            session_name = str(row['account_id'])
            try:
                client = boto3.client('sts')
                cred = client.assume_role(RoleArn=role_arn, RoleSessionName=session_name)
                cred = cred['Credentials']
                cred = dict(aws_access_key_id=cred["AccessKeyId"], aws_secret_access_key=cred["SecretAccessKey"],
                            aws_session_token=cred["SessionToken"], region_name=region_names[0])
                try:
                    new_client = boto3.client('s3', **cred)
                    s3_bucket_dict = new_client.list_buckets()
                    owner_id = s3_bucket_dict['Owner']['ID']
                    bucket_list = s3_bucket_dict['Buckets']
                    for i in bucket_list:
                        bucket_name = i['Name']
                        response = new_client.list_objects(Bucket=bucket_name)
                        response['bucket_name'] = bucket_name
                        temp_list.append(response)
                except Exception as e:
                    print(e)
            except Exception as e:
                print(e)
        return temp_list


if __name__ == '__main__':
    start = time.time()
    aws = AWS()
    db = DB()
    conn, cursor = db.connect()
    query = """select arn, account_id from aws.cloud_details where cloud = 'AWS'"""
    resp = db.execute_query(cursor, query)
    df = pd.DataFrame(resp, columns=["arn", "account_id"])
    response = aws.get_data(df)
    parser = Parser()
    df_dict = parser.process(response, 's3_list')
    db_response = db.db_process(df_dict)
