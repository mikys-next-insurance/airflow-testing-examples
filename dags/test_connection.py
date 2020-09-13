import json

import pandas as pd
import psycopg2
import datetime as dt

import requests
from airflow import DAG
from airflow.operators import DummyOperator, PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.hooks.S3_hook import S3Hook
import boto3
import os
from airflow.hooks.base_hook import BaseHook

# #------------------------------------------------------------------------------------------#
# # vars
# #------------------------------------------------------------------------------------------#
s3 = boto3.client('s3')
connection_prod='redshift-prod'
connection_stg='redshift-stg'

# #------------------------------------------------------------------------------------------#
# #  Functions
# #------------------------------------------------------------------------------------------#

# note that the test slack webhook sends to the #bi_alerts_test channel
def send_slack_msg(**kwargs):
    msg = ''':barber: Send slack message test successful'''
    slackhost = BaseHook.get_connection("slack-webhook").host
    slack_webhook_token = BaseHook.get_connection("slack-webhook").password
    webhook_url = slackhost + "/" + slack_webhook_token
    slack_data = {'text': msg}
    response = requests.post(
        webhook_url, data=json.dumps(slack_data),
        headers={'Content-Type': 'application/json'}
    )
    print(response)
    print(response.content)

def test_connection_prod(**kwargs):
    conn_id = kwargs.get('conn_id')
    pg_hook = PostgresHook(conn_id)
    conn=pg_hook.get_conn()
    cur = conn.cursor()
    df_prod=pd.read_sql("""select tracking_id, business_id, email, cookied_user_id
                              from dwh.sources_attributed_table e limit 10""",conn)
    print('######################## Running query from redshift prod output below #############################')
    print(df_prod)
    conn.close()

def test_connection_stg(**kwargs):
    conn_id = kwargs.get('conn_id')
    pg_hook = PostgresHook(conn_id)
    conn=pg_hook.get_conn()
    cur = conn.cursor()
    df_STG=pd.read_sql("""select tracking_id, business_id, email, cookied_user_id
                              from dwh.sources_attributed_table e limit 10""",conn)
    print('######################## Running query from redshift STG output below #############################')
    print(df_STG)
    conn.close()

def test_s3_creds(**kwargs):
    print('######################## s3 airflow creds #############################')
    aws_creds = S3Hook(aws_conn_id='aws_default', verify=None).get_credentials()
    print(aws_creds.access_key)
    print(aws_creds.secret_key)

def upload_file_to_S3(**kwargs):
    print('######################## test s3 boto connection #############################')
    dir_path = os.path.dirname(os.path.realpath(__file__))
    os.chdir(dir_path)
    print(dir_path)
    session = boto3.Session()
    credentials = session.get_credentials()
    access_key = credentials.access_key
    secret_key = credentials.secret_key
    print( access_key+'| |'+secret_key)
    bucket_name = kwargs.get('bucket_name')
    filename = kwargs.get('filename')
    s3_target = kwargs.get('s3_target')
    s3.upload_file(filename, bucket_name, s3_target + filename)

# #------------------------------------------------------------------------------------------#
# #  DAG
# #------------------------------------------------------------------------------------------#

default_args = {
    'owner': 'bi_team',
    'start_date': dt.datetime(2020, 3, 12),
    'retries': 1,
    'depends_on_past': False,
    'retry_delay': dt.timedelta(seconds=5),
    'provide_context':True,
}

with DAG('test_connection',
         default_args=default_args,
         schedule_interval='@once',
         max_active_runs=1,
         ) as dag:
    start_task = DummyOperator(
        task_id='dummy_start'
    )
    test_connection_prod = PythonOperator(
        task_id='test_connection_prod',
        op_kwargs = {'conn_id':connection_prod},
        python_callable=test_connection_prod,
        provide_context=True
    )
    test_connection_stg = PythonOperator(
        task_id='test_connection_stg',
        op_kwargs = {'conn_id':connection_stg},
        python_callable=test_connection_stg,
        provide_context=True
    )
    test_s3_creds = PythonOperator(
        task_id='test_s3_creds',
        python_callable=test_s3_creds,
        provide_context=True
    )
    upload_to_S3_task = PythonOperator(
        task_id='upload_to_S3',
        python_callable=upload_file_to_S3,
        op_kwargs={
            's3_target': 'debug/',
            'filename': 'test_connection.py',
            'bucket_name': 'gb-sftp-files'},
        provide_context=True
        )
    slack_task = PythonOperator(
        task_id='send_slack_message',
        python_callable=send_slack_msg,
        provide_context=True
    )

start_task >> test_s3_creds>> upload_to_S3_task
start_task >> test_connection_prod
start_task >> slack_task
