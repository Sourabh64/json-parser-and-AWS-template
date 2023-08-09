import time
import boto3
import json
import pandas as pd
from json_parser import Parser
from push_to_db import DB


class AWS:
    def __init__(self):
        pass

    def get_data(self, df):
        region_names = ['ap-south-1', 'us-east-1']
        # df = pd.read_csv('Cloud_awscloud_202209211754.csv')
        inspector_list = []
        for index, row in df.iterrows():
            role_arn = row['arn']
            session_name = str(row['account_id'])
            for region in region_names:
                client = boto3.client('sts', region_name=region)
                cred = client.assume_role(RoleArn=role_arn, RoleSessionName=session_name)
                cred = cred['Credentials']
                try:
                    cred = dict(aws_access_key_id=cred["AccessKeyId"], aws_secret_access_key=cred["SecretAccessKey"], aws_session_token=cred["SessionToken"], region_name=region)
                    inspector_client = boto3.client('inspector', **cred)
                    inspector_dict = inspector_client.list_findings()
                    inspector_list = inspector_dict['findingArns']
                    while 'NextToken' in inspector_dict:
                        inspector_dict = inspector_client.list_findings(NextToken=inspector_dict['NextToken'])
                        inspector_list.extend(inspector_dict['findingArns'])
                    # temp_list.append(inspector_list)
                except Exception as e:
                    print(e)
        with open('inspector_findings_list.json', 'w') as f:
            json.dump(inspector_list, f, default=str)
        return inspector_list


if __name__ == '__main__':
    start = time.time()
    aws = AWS()
    db = DB()
    conn, cursor = db.connect()
    query = """select arn, account_id from aws.cloud_details where cloud = 'AWS' and account_id = '531341628109'"""
    resp = db.execute_query(cursor, query)
    df = pd.DataFrame(resp, columns=["arn", "account_id"])
    response = aws.get_data(df)
    parser = Parser()
    df_dict = parser.process(response, 'inspector_findings')
    db_response = db.db_process(df_dict)
