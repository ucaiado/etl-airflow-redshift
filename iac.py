#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
'Infrastructure as Code'


@author: udacity, ucaiado

Created on 07/07/2020
"""

# import libraries
import argparse
import textwrap
import boto3
import json
import os
import configparser
import subprocess
import pandas as pd


'''
Begin help functions and variables
'''

config = configparser.ConfigParser()
config.read_file(open('confs/dpipe.cfg'))


KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')

DPIPE_CLUSTER_TYPE = config.get("DPIPE", "DPIPE_CLUSTER_TYPE")
DPIPE_NUM_NODES = config.get("DPIPE", "DPIPE_NUM_NODES")
DPIPE_NODE_TYPE = config.get("DPIPE", "DPIPE_NODE_TYPE")

DPIPE_CLUSTER_IDENTIFIER = config.get("DPIPE", "DPIPE_CLUSTER_IDENTIFIER")
DPIPE_HOST = config.get("CLUSTER", "HOST")
DPIPE_DB = config.get("CLUSTER", "DB_NAME")
DPIPE_DB_USER = config.get("CLUSTER", "DB_USER")
DPIPE_DB_PASSWORD = config.get("CLUSTER", "DB_PASSWORD")
DPIPE_PORT = config.get("CLUSTER", "DB_PORT")

DPIPE_IAM_ROLE_NAME = config.get("DPIPE",  "DPIPE_IAM_ROLE_NAME")
IAM_ROLE_ARN = config.get("IAM_ROLE",  "ARN")

DPIPE_IAM_ROLE_NAME = config.get("DPIPE",  "DPIPE_IAM_ROLE_NAME")
IAM_ROLE_ARN = config.get("IAM_ROLE",  "ARN")

S3_LOG_DATA = config.get("S3", "LOG_DATA")
S3_LOG_JSONPATH = config.get("S3",  "LOG_JSONPATH")
S3_SONG_DATA = config.get("S3",  "SONG_DATA")


def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus",
                  "MasterUsername", "DBName", "Endpoint", "NumberOfNodes",
                  'VpcId']
    x = [(k, v) for k, v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


'''
End help functions and variables
'''


if __name__ == '__main__':
    s_txt = '''\
            Infrastructure as code
            --------------------------------
            Create Amazon Readshift cluster
            '''
    # include and parse variables
    obj_formatter = argparse.RawDescriptionHelpFormatter
    parser = argparse.ArgumentParser(
        formatter_class=obj_formatter, description=textwrap.dedent(s_txt))

    s_help = 'Create IAM role'
    parser.add_argument('-i', '--iam', action='store_true', help=s_help)

    s_help = 'Create a Readshift cluster'
    parser.add_argument('-r', '--redshift', action='store_true', help=s_help)

    s_help = 'Check Readshift cluster status'
    parser.add_argument('-s', '--status', action='store_true', help=s_help)

    s_help = 'Open incoming TCP port'
    parser.add_argument('-t', '--tcp', action='store_true', help=s_help)

    s_help = 'Create Airflow Variable file'
    parser.add_argument('-a', '--airflow', action='store_true', help=s_help)

    s_help = 'Clean up your resources'
    parser.add_argument('-d', '--delete', action='store_true', help=s_help)

    # check what should do
    args = parser.parse_args()
    b_create_iam = args.iam
    b_create_redshift = args.redshift
    b_check_status = args.status
    b_open_tcp = args.tcp
    b_delete = args.delete
    b_airflow = args.airflow

    s_err = 'Please select only one option from -h menu'
    b_test_all = (b_create_iam or b_create_redshift or b_check_status or
                  b_open_tcp or b_delete or b_airflow)
    assert b_test_all, s_err
    if b_create_redshift or b_open_tcp:
        assert len(IAM_ROLE_ARN) > 2, 'Please run --iam flag before this step'
    if b_open_tcp:
        s_err = ('Please run --iam and --redshift flags and wait for the '
                 'cluster be ready before proceed to this step')
        assert len(DPIPE_HOST) > 2, s_err

    # create clients
    print('...create clients for EC2, S3, IAM, and Redshift')
    ec2 = boto3.resource(
        'ec2',
        region_name='us-west-2',
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET)

    s3 = boto3.resource(
        's3',
        region_name='us-west-2',
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET)

    iam = boto3.client(
        'iam',
        region_name='us-west-2',
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET)

    redshift = boto3.client(
        'redshift',
        region_name='us-west-2',
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET)

    if b_create_iam:
        # Create the IAM role
        try:
            DPIPERole = iam.create_role(
                Path='/',
                RoleName=DPIPE_IAM_ROLE_NAME,
                Description=('Allows Redishift clusters to call AWS '
                             'services on your behalf'),
                AssumeRolePolicyDocument=json.dumps(
                    {'Statement': [{
                        'Action': 'sts:AssumeRole',
                        'Effect': 'Allow',
                        'Principal': {'Service': 'redshift.amazonaws.com'}}],
                     'Version': '2012-10-17'}
                ))
            print('...create a new IAM Role')
        except Exception as e:
            print(e)

        # Attaching Policy
        print('...attach policy')
        iam.attach_role_policy(
            RoleName=DPIPE_IAM_ROLE_NAME,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
        )['ResponseMetadata']['HTTPStatusCode']

        # Get and print the IAM role ARN
        print('...get the IAM role ARN')
        role_arn = iam.get_role(RoleName=DPIPE_IAM_ROLE_NAME)['Role']['Arn']
        print('   !! fill in the IAM_ROLE ARN field in DPIPE.cfg file with the '
              'following string:')
        print(role_arn)
    elif b_create_redshift:
        try:
            response = redshift.create_cluster(
                # add parameters for hardware
                ClusterType=DPIPE_CLUSTER_TYPE,
                NodeType=DPIPE_NODE_TYPE,
                NumberOfNodes=int(DPIPE_NUM_NODES),

                # add parameters for identifiers & credentials
                DBName=DPIPE_DB,
                ClusterIdentifier=DPIPE_CLUSTER_IDENTIFIER,
                MasterUsername=DPIPE_DB_USER,
                MasterUserPassword=DPIPE_DB_PASSWORD,

                # add parameter for role (to allow s3 access)
                IamRoles=[IAM_ROLE_ARN])
            print('...create redshift cluster')
        except Exception as e:
            print(e)
    elif b_check_status:
        print('...check cluster status')
        try:
            my_cluster_prop = redshift.describe_clusters(
                ClusterIdentifier=DPIPE_CLUSTER_IDENTIFIER)['Clusters'][0]
            df = prettyRedshiftProps(my_cluster_prop).set_index('Key')
            print(df.to_string())
            print('\ntry to recover endpoint string')
            if my_cluster_prop['ClusterStatus'] == 'available':
                DPIPE_ENDPOINT = my_cluster_prop['Endpoint']['Address']
                DPIPE_ROLE_ARN = my_cluster_prop['IamRoles'][0]['IamRoleArn']
                endpoint = my_cluster_prop['Endpoint']['Address']
                print('   !! fill in the HOST CLUSTER field in DPIPE.cfg file '
                      'with the following string:')
                print("HOST :: ", endpoint)
            else:
                print('   !!cluster status is not "available" yet')
        except Exception as e:
            print(e)
    elif b_open_tcp:
        print('...open an incoming TCP port to access the cluster ednpoint')
        my_cluster_prop = redshift.describe_clusters(
            ClusterIdentifier=DPIPE_CLUSTER_IDENTIFIER)['Clusters'][0]
        try:
            vpc = ec2.Vpc(id=my_cluster_prop['VpcId'])
            defaultSg = list(vpc.security_groups.all())[0]

            defaultSg.authorize_ingress(
                GroupName='default',
                CidrIp='0.0.0.0/0',
                IpProtocol='TCP',
                FromPort=int(DPIPE_PORT),
                ToPort=int(DPIPE_PORT)
            )
        except Exception as e:
            print(e)

    elif b_delete:
        print('...clean up your resources')
        redshift.delete_cluster(
            ClusterIdentifier=DPIPE_CLUSTER_IDENTIFIER,
            SkipFinalClusterSnapshot=True)

        iam.detach_role_policy(
            RoleName=DPIPE_IAM_ROLE_NAME,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        iam.delete_role(RoleName=DPIPE_IAM_ROLE_NAME)

        print('...check cluster status')
        my_cluster_prop = redshift.describe_clusters(
            ClusterIdentifier=DPIPE_CLUSTER_IDENTIFIER)['Clusters'][0]
        df = prettyRedshiftProps(my_cluster_prop).set_index('Key')
        print(df.to_string())

    elif b_airflow:
        from airflow import settings
        from airflow.models.connection import Connection
        from airflow.models import Variable

        print('...delete old airflow connections')
        s_bash = f"airflow connections -d --conn_id aws_credentials "
        process = subprocess.Popen(s_bash.split(), stdout=subprocess.PIPE)
        output, error = process.communicate()

        s_bash = f"airflow connections -d --conn_id redshift "
        process = subprocess.Popen(s_bash.split(), stdout=subprocess.PIPE)
        output, error = process.communicate()

        print('...create airflow connection to AWS')

        c = Connection(
            conn_id='aws_credentials',
            conn_type='aws',
            login=f'{KEY}',
            password=f'{SECRET}')
        session = settings.Session()
        session.add(c)
        session.commit()

        print('...create airflow connection to Redshift')
        c = Connection(
            conn_id='redshift',
            conn_type='postgres',
            host=f'{DPIPE_HOST}',
            schema='dev',
            login=f'{DPIPE_DB_USER}',
            password=f'{DPIPE_DB_PASSWORD}',
            port=f'{DPIPE_PORT}')
        session = settings.Session()
        session.add(c)
        session.commit()

        print('...create variables to store S3 paths')
        Variable.set("S3_LOG_DATA", S3_LOG_DATA)
        Variable.set("S3_LOG_JSONPATH", S3_LOG_JSONPATH)
        Variable.set("S3_SONG_DATA", S3_SONG_DATA)

