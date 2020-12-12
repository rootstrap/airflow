"""Moves raw files to another bucket, converting them to CSV"""
from airflow.models.dag import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.s3_file_transform_operator import S3FileTransformOperator

import sys, os
sys.path.insert(0, '/opt/airflow/dags/airflow.git/dags')

#from utils.general import groups, defaults
#from utils.s3 import helper as s3_helper

dag_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 9, 7),
    "email": ["mikaela.pisani@rootstrap.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

def test():
	print(os.environ.get('AIRFLOW__CORE__DAGS_FOLDER'))
	print(sys.path)

	    


with DAG(dag_id="dummy_test", default_args=dag_args, schedule_interval= '@once') as dag:

    start = PythonOperator(task_id='task-start', python_callable=test, dag=dag)

    start
