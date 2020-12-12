"""Moves raw files to another bucket, converting them to CSV"""
from airflow.models.dag import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.s3_file_transform_operator import S3FileTransformOperator

from datetime import datetime, timedelta
from airflow.models import Variable

import sys, os
AIRFLOW__CORE__DAGS_FOLDER = Variable.get("AIRFLOW__CORE__DAGS_FOLDER")
sys.path.insert(0,AIRFLOW__CORE__DAGS_FOLDER)

from importlib import import_module
defaults = import_module('utils.general.defaults')
s3_helper = import_module('utils.s3.helper')
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
	print(sys.path)
	print(defaults)
	print(s3_helper.csv_name('test.xml', '1'))

	    


with DAG(dag_id="dummy_test", default_args=dag_args, schedule_interval= '@once') as dag:

    start = PythonOperator(task_id='task-start', python_callable=test, dag=dag)

    start
