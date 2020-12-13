"""Moves raw files to another bucket, converting them to CSV"""
from airflow.models.dag import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.s3_file_transform_operator import S3FileTransformOperator

import sys, os
from airflow.models import Variable
AIRFLOW__CORE__DAGS_FOLDER = Variable.get("AIRFLOW__CORE__DAGS_FOLDER")
sys.path.insert(0, AIRFLOW__CORE__DAGS_FOLDER)
sys.path.insert(0, AIRFLOW__CORE__DAGS_FOLDER + 'dags_utils')

from dags_utils.general import groups, defaults
from dags_utils.s3 import helper as s3_helper


def s3_files_section():
    """ Call Batch of tasks, containing multiple files """
    DummyOperator(task_id=f'task-start') >> s3_helper.s3_transform_operator("{{run_id}}")


with DAG(dag_id="s3_normalize_csv", default_args=defaults.dag_args, schedule_interval= '@once') as dag:

    start = DummyOperator(task_id='start')

    with TaskGroup("normalize-to-csv", tooltip="Normalize files and move to S3 CSV folder") as section:
        s3_files_section()

    end = DummyOperator(task_id='end')

    start >> section >> end
