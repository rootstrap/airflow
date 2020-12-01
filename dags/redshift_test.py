from airflow.models.dag import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.decorators import task
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
import logging

from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable


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

def create_s3_to_tedshift_group():

    '''For each file in s3 folder load file's data to redshift.'''
    
    #1. List files in s3 folder
    files = load_files('patients-records', 'cleaned/')

    #2. for each file: load content to redshift
    process_files = [S3ToRedshiftOperator(
            task_id = f's3_to_redshift_transformer-{i}',
            schema = 'PUBLIC',
            table = 'raw_records',
            s3_bucket = 'patients-records',
            s3_key = 'cleaned' + '/' + file,
            redshift_conn_id = 'redshift_connection',
            aws_conn_id = 's3_connection',
            copy_options = ["csv"],
            truncate_table  = False
        ) for file,i in zip(files,range(len(files)))
    ]

    process_files

with DAG(dag_id="redshift_transformer", default_args=default_args, schedule_interval= '@once') as dag:

    # dummy operator to start DAG graph 
    start = DummyOperator(task_id='start')

    # Group tasks to load s3 files' data to redshift  
    with TaskGroup("S3ToRedshiftGroup", tooltip="Load files to Redshift") as s3_to_tedshift_group:
        create_s3_to_tedshift_group()

    start  >> s3_to_tedshift_group 




