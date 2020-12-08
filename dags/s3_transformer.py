"""Moves raw files to another bucket, converting them to CSV"""

from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.s3_file_transform_operator import S3FileTransformOperator
from airflow.models import Variable

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 9, 7),
    "email": ["mikaela.pisani@rootstrap.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

def process_file(file):
    """Callback executed for each file"""
    print('Processing file ', file)


source_s3_path = Variable.get("raw_path")
dest_s3_path = Variable.get("cleaned_path")
s3_bucket = Variable.get("s3_bucket")
dags_folder = Variable.get("AIRFLOW__CORE__DAGS_FOLDER")


def load_files():
    """ Call S3Hook to list files in bucket """
    cloud_hook = S3Hook(aws_conn_id='s3_connection')
    cloud_hook.get_conn()
    files = cloud_hook.list_keys(bucket_name=s3_bucket, prefix=source_s3_path + '/', delimiter='/')
    if len(files)>1:
        files = files[1:]
    else:
        files = []
    files = list(map(lambda x:x.split('/')[1], files))
    return files

def create_section():
    """ Call Batch of tasks, containing multiple files """
    files = load_files()
    list_files = PythonOperator(task_id='list_files',
                    python_callable=load_files
        )

    source_folder = 's3://' + s3_bucket +'/' + source_s3_path + '/'
    dest_folder = 's3://' + s3_bucket +'/' + dest_s3_path + '/'

    process_files = [S3FileTransformOperator(
                task_id=f'transform_s3_data-{i}',
                source_s3_key=  source_folder + file,
                dest_s3_key= dest_folder + file.split('.')[0] + "_{{ run_id }}.csv",
                replace=True,
                transform_script=dags_folder + 'scripts/transform.py',
                script_args=["{{run_id}}"],
                source_aws_conn_id='s3_connection',
                dest_aws_conn_id='s3_connection'
            ) for file,i in zip(files,range(len(files)))
    ]

    list_files >> process_files

with DAG(dag_id="S3_initial", default_args=default_args, schedule_interval= '@once') as dag:

    start = DummyOperator(task_id='start')

    with TaskGroup("section", tooltip="Tasks for Section") as section:
        create_section()

    start  >> section
