import json
from os import environ
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.lambda_function import (
    AwsLambdaInvokeFunctionOperator,
)
import dotenv
import etl
import utils

from include import utils, etl

config = dotenv.dotenv_values()

locations = ["US", "UK", "Canada"][0]
jobs = ["Data Analyst", "Data Engineer"][0]

default_args = {
    "owner": "oluwasayo",
    "start_date": datetime(2023, 4, 30),
}

with DAG(
    "jobs_pipeline", default_args=default_args, schedule="00 14 * * *", catchup=True
) as dag:
    fetch_raw_jobs_data = PythonOperator(
        task_id="fetch_jobs_data",
        python_callable=etl.extract_job_data,
        op_kwargs={
            "locations": locations,
            "jobtitles": jobs,
        },
    )

    push_raw_jobs_data_to_s3 = PythonOperator(
        task_id="push_raw_jobs_data_to_s3",
        python_callable=utils.upload_file_to_s3,
        op_kwargs={
            "filepath": "raw_jobs_data.json",
            "bucket": environ.get(
                "RAW_DATA_BUCKET",
                config.get("RAW_DATA_BUCKET", environ.get("RAW_DATA_BUCKET")),
            ),
        },
        do_xcom_push=True,
    )

    transform_data_to_csv = AwsLambdaInvokeFunctionOperator(
        function_name="jobs-data-pipeline-dev-transform_json_to_csv",
        task_id="transform_json_to_csv",
        aws_conn_id="aws_sayoreacts",
        invocation_type="RequestResponse",
        payload=json.dumps(
            "{{ task_instance.xcom_pull(task_ids='push_raw_jobs_data_to_s3', key='return_value') }}"
        ),
        do_xcom_push=True,
    )

    create_jobs_database_table = PythonOperator(
        task_id="create_jobs_table_if_not_exist",
        python_callable=utils.create_database_table,
        op_kwargs={
            "con": utils.get_database_conn(),
        },
    )

    load_csv_to_redshift = AwsLambdaInvokeFunctionOperator(
        function_name="jobs-data-pipeline-dev-load_csv_to_redshift",
        task_id="load_csv_to_redshift",
        aws_conn_id="aws_sayoreacts",
        invocation_type="RequestResponse",
        payload="{{ task_instance.xcom_pull(task_ids='transform_json_to_csv', key='return_value') }}",
        do_xcom_push=True,
    )

    (
        fetch_raw_jobs_data
        >> push_raw_jobs_data_to_s3
        >> transform_data_to_csv
        >> create_jobs_database_table
        >> load_csv_to_redshift
    )
