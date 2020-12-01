from airflow.models.dag import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.decorators import task
from airflow.providers.amazon.aws.operators.s3_file_transform import S3FileTransformOperator


from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
import logging

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 9, 7),
    "email": ["error_airflow@rootstrap.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(minutes=1)
}

def load_files(bucket_name, s3_prefix):
    
    '''Returns a list of filenames in a certain s3 folder.'''
    
    s3 = S3Hook(aws_conn_id='s3_connection')
    s3.get_conn()
    files = s3.list_keys(bucket_name=bucket_name, prefix=s3_prefix, delimiter='/')
    logging.info('Files:', files)
    if (len(files)>1):
        files = files[1:]
    else:
        files = []
    files = list(map(lambda x:x.split('/')[1], files))
    return files

def create_s3_file_transform_group():

    '''For each file in s3 folder transform it.'''

    # 1. List files in s3 folder
    files = load_files('patients-records', 'raw-files/')
    
    # 2. for each file: transform to csv 
    process_files = [S3FileTransformOperator(
                task_id=f'transform_s3_data-{i}',
                source_s3_key='s3://patients-records/raw-files/' + file,
                dest_s3_key='s3://patients-records/cleaned/' + file.split('.')[0] + '.csv', 
                replace=True,
                transform_script='/opt/airflow/dags/scripts/transform.py',
                source_aws_conn_id='s3_connection',
                dest_aws_conn_id='s3_connection'
            ) for file,i in zip(files,range(len(files)))
    ]
    
    process_files

with DAG(dag_id="batchfiles", default_args=default_args, schedule_interval= '@once') as dag:

    # dummy operator to start DAG graph 
    start = DummyOperator(task_id='start')

    # Group tasks to load s3 files' data to redshift  
    with TaskGroup("S3FileTransformGroup", tooltip="transform files in s3") as s3_file_transform_group:
        create_s3_file_transform_group()

    start  >> s3_file_transform_group 






