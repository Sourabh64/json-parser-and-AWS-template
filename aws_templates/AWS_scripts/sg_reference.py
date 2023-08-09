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
            for region in region_names:
                try:
                    client = boto3.client('sts', region_name=region)
                    cred = client.assume_role(RoleArn=role_arn, RoleSessionName=session_name)
                    cred = cred['Credentials']
                    try:
                        cred = dict(aws_access_key_id=cred["AccessKeyId"], aws_secret_access_key=cred["SecretAccessKey"], aws_session_token=cred["SessionToken"], region_name=region)
                        new_client = boto3.client('ec2', **cred)
                        ec2_regions = [region['RegionName'] for region in new_client.describe_regions()['Regions']]
                        for region in ec2_regions:
                            ec2 = boto3.client('ec2', region_name=region)
                            ec2_security_group = ec2.describe_security_groups()
                            for sg in ec2_security_group['SecurityGroups']:
                                group_id = sg['GroupId']
                                sg_reference = ec2.describe_security_group_references(GroupId=[group_id])
                                sg_reference['account_id'] = session_name
                                temp_list.append(sg_reference)
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
    df_dict = parser.process(response, 'sg_reference')
    db_response = db.db_process(df_dict)
