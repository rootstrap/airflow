#!/usr/local/bin/python

"""General useful variables"""

from datetime import datetime, timedelta
from airflow.models import Variable
import os

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

redshift_table = Variable.get("redshift_table")
s3_bucket = Variable.get("s3_bucket")
dest_s3_path = Variable.get("cleaned_path")
source_s3_path = Variable.get("raw_path")
s3_conn_name = 's3_connection'
redshift_conn_name = 'redshift_connection'
source_folder = "s3://{}/{}/".format(s3_bucket, source_s3_path)
dest_folder = "s3://{}/{}/".format(s3_bucket, dest_s3_path)
dags_folder = os.environ.get('AIRFLOW__CORE__DAGS_FOLDER')
s3_transform_script = dags_folder + '/scripts/transform.py' 
