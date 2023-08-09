import time
import boto3
import pandas as pd
from json_parser import Parser
from push_to_db import DB


class AWS:
    def __init__(self):
        pass

    def get_data(self):
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
                    cred = dict(aws_access_key_id=cred["AccessKeyId"],aws_secret_access_key=cred["SecretAccessKey"],aws_session_token=cred["SessionToken"],region_name=region)
                    api_gw_client = boto3.client('apigateway', **cred)
                    api_gw_instance_dict = api_gw_client.get_rest_apis()['items']
                    for i in api_gw_instance_dict:
                        item_id = i['id']
                        api_gw_instance_dict = api_gw_client.get_stages(restApiId=item_id)
                        api_gw_instance_dict['item_id'] = item_id
                        api_gw_instance_dict['account_id'] = session_name
                    temp_list.append(api_gw_instance_dict)
                except Exception as e:
                    print(e)
        return temp_list


if __name__ == '__main__':
    start = time.time()
    aws = AWS()
    response = aws.get_data()
    parser = Parser()
    df_dict = parser.process(response, 'api_gw_api')
    db = DB()
    db_response = db.db_process(df_dict)
