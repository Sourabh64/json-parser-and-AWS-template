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
                try:
                    client = boto3.client('sts', region_name=region)
                    cred = client.assume_role(RoleArn=role_arn, RoleSessionName=session_name)
                    cred = cred['Credentials']
                    try:
                        cred = dict(aws_access_key_id=cred["AccessKeyId"], aws_secret_access_key=cred["SecretAccessKey"], aws_session_token=cred["SessionToken"], region_name=region)
                        client = boto3.client('autoscaling', **cred)
                        resp_list = client.describe_auto_scaling_groups()['AutoScalingGroups']
                        for resp in resp_list:
                            lb_names = resp['LoadBalancerNames']
                            for lb in lb_names:
                                lb_client = boto3.client('elb', **cred)
                                resp_dict = lb_client.describe_load_balancers(LoadBalancerNames=[lb])
                                resp_dict['asg_name'] = resp['AutoScalingGroupName']
                                resp_dict['account_id'] = session_name
                                temp_list.append(resp_dict)
                    except Exception as e:
                        print(e)
                except Exception as e:
                    print(e)
        return temp_list


if __name__ == '__main__':
    start = time.time()
    aws = AWS()
    response = aws.get_data()
    parser = Parser()
    df_dict = parser.process(response, 'asg_elb')
    db = DB()
    db_response = db.db_process(df_dict)