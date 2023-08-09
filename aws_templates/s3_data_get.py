# import boto3
#
# region_names = ['ap-south-1', 'us-east-1']
# # df = pd.read_csv('Cloud_awscloud_202209211754.csv')
# temp_list = []
# # for index, row in df.iterrows():
# role_arn = row['arn']
# session_name = '123456789'
#
# try:
#     client = boto3.client('sts', region_name=region_names[0])
#     cred = client.assume_role(RoleArn=role_arn, RoleSessionName=session_name)
#     cred = cred['Credentials']
#     try:
#         cred = dict(aws_access_key_id=cred["AccessKeyId"], aws_secret_access_key=cred["SecretAccessKey"], aws_session_token=cred["SessionToken"], region_name=region)
#         client = boto3.client('s3', **cred)
#         resp_dict = client.describe_auto_scaling_groups()
#         resp_dict['account_id'] = session_name
#         temp_list.append(resp_dict)
#     except Exception as e:
#         print(e)
# except Exception as e:
#     print(e)
# access_key = os.environ['aws_access_key_id']
# secret_key = os.environ['aws_secret_access_key']
# print(access_key)
# cred = dict(aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name='ap-south-1')
# client = boto3.client('s3', **cred)
# resp = client.copy_object(Bucket='', copy_source='', key='/')
# print(resp)

new = [1, 0, 1, 1, 1, 0]
count = 0
one_count = 0
for i in new:
    if one_count != 2 or one_count == 1 or one_count == 0:
        count += 1
        if i == 1:
            one_count += 1
        else:
            one_count = 0
    else:
        print(count)
        break

