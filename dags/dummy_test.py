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


def test():
	print(os.environ.get('AIRFLOW__CORE__DAGS_FOLDER'))
	print(sys.path)

	    


with DAG(dag_id="s3_normalize_csv", default_args=defaults.dag_args, schedule_interval= '@once') as dag:

    start = PythonOperator(task_id='task-start', python_callable=test, dag=dag)

    start
