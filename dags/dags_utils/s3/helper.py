""" Helper functions for Amazon S3 """
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from dags_utils.general import defaults
from airflow.operators.s3_file_transform_operator import S3FileTransformOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

def csv_name(filename, run_id):
    """ Formats output of CSV file """
    return filename.split('.')[0] + "_{}.csv".format(run_id)

def files_in_folder(dest_path):
    """ Call S3Hook to list files in bucket """
    cloud_hook = S3Hook(aws_conn_id=defaults.s3_conn_name)
    cloud_hook.get_conn()
    files = cloud_hook.list_keys(bucket_name=defaults.s3_bucket, prefix=dest_path + '/', delimiter='/')
    if len(files)>1:
        files = files[1:]
    else:
        files = []
    return list(map(lambda x:x.split('/')[1], files))


def s3_transform_operator(run_id):
    """ Applies transformation script on file """

    files = files_in_folder(defaults.source_s3_path)

    return [S3FileTransformOperator(
                task_id=f'transform_s3_data-{i}',
                source_s3_key=  defaults.source_folder + file,
                dest_s3_key= defaults.dest_folder + csv_name(file, run_id),
                replace=True,
                transform_script=defaults.s3_transform_script,
                script_args=[run_id],
                source_aws_conn_id=defaults.s3_conn_name,
                dest_aws_conn_id=defaults.s3_conn_name
            ) for file,i in zip(files,range(len(files)))
    ]

def s3_to_redshift(run_id):
    """ Moves file from S3 bucket to redshift """
    # TODO: Filter by run_id in files_in_folder (regex or use folder in S3)
    files = files_in_folder(defaults.dest_s3_path)

    return [S3ToRedshiftOperator(
            task_id = f's3_to_redshift_transformer-{i}',
            schema = 'PUBLIC',
            table = defaults.redshift_table,
            s3_bucket = defaults.s3_bucket,
            s3_key = defaults.dest_s3_path + '/' + file,
            redshift_conn_id = defaults.redshift_conn_name,
            aws_conn_id = defaults.s3_conn_name,
            copy_options = ["csv"],
            truncate_table  = False
        ) for file,i in zip(files,range(len(files)))
    ]
