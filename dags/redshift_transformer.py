"""Moves CSV files from S3 Bucket to Redshift"""

from airflow.models.dag import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

AIRFLOW__CORE__DAGS_FOLDER = Variable.get("AIRFLOW__CORE__DAGS_FOLDER")
sys.path.append(AIRFLOW__CORE__DAGS_FOLDER + 'utils/')

from utils.general import groups, defaults
from utils.s3 import helper as s3_helper

def redshift_move_section():
    """ Call Batch of tasks, containing multiple files """
    DummyOperator(task_id='redshift-transfer-start') >> s3_helper.s3_to_redshift("{{run_id}}")


with DAG(dag_id="redshift_transformer", default_args=defaults.dag_args, schedule_interval= '@once') as dag:

    start = DummyOperator(task_id='start')

    with TaskGroup("section", tooltip="Tasks for Section") as section:
        redshift_move_section()

    start  >> section
