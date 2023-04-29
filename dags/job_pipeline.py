import json
from os import environ
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.lambda_function import (
    AwsLambdaInvokeFunctionOperator,
)
from include import helpers


default_args = {
    "owner": "oluwasayo",
    "start_date": datetime(2023, 4, 20),
}

with DAG(
    "job_pipeline", default_args=default_args, schedule="15 17 * * *", catchup=False
) as dag:
    fetch_raw_jobs_data = PythonOperator(
        task_id="fetch_jobs_data",
        python_callable=helpers.pull_job_data,
        op_kwargs={"locations": ["UK"], "jobtitles": ["Data Analyst"]},
    )

    push_raw_jobs_data_to_s3 = PythonOperator(
        task_id="push_raw_jobs_data_to_s3",
        python_callable=helpers.upload_file_to_s3,
        op_kwargs={
            "filepath": "raw_jobs_data.json",
            "bucket": environ.get("RAW_DATA_BUCKET"),
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
        python_callable=helpers.create_database_table,
        op_kwargs={
            "con": helpers.get_database_conn(),
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
