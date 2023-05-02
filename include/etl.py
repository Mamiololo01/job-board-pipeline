import json
import boto3
import utils
from os import environ
from pathlib import Path
from dotenv import dotenv_values

config = dotenv_values()


def extract_job_data(jobtitles, locations):
    try:
        queries = [
            f"{title} in {location}" for location in locations for title in jobtitles
        ]

        results = []
        for query in queries:
            data = utils.pull_job_data(query)
            results.extend(data)
        path = str(Path("raw_jobs_data.json").resolve())
        with open(path, "w") as f:
            json.dump(results, f, indent=4)
        print(f"Jobs data downloaded to {path}")
        return path

    except KeyError as e:
        raise KeyError(
            "Could not load 'data' from response. API quote may have reached it's limit"
        )


def transform_data(event, context=None, use_lambda=False):
    if isinstance(event, str):
        event = event.replace("'", '"')
        event = json.loads(event)

    if use_lambda:
        print("Invoking transform_data lambda locally...")
        aws_lambda = boto3.client("lambda")
        response = aws_lambda.invoke(
            FunctionName="jobs-data-pipeline-dev-transform_json_to_csv",
            InvocationType="RequestResponse",
            Payload=json.dumps(event),
        )

        print("Transformation complete")
        if response.get("FunctionError"):
            raise Exception(
                "An error occured durint lambda invocation. Check lambda logs"
            )
        payload = response["Payload"].read()
        return json.loads(payload)

    bucket = event.get("Bucket")
    key = event.get("Key")

    filepath = utils.get_file_from_s3(bucket=bucket, key=key)
    transformed_result = utils.process_data(filepath=filepath)
    result = utils.upload_file_to_s3(
        filepath=transformed_result["output_path"],
        bucket=config.get(
            "TRANSFORMED_DATA_BUCKET", environ.get("TRANSFORMED_DATA_BUCKET")
        ),
    )
    return {**result, "create_table_stmnt": transformed_result["create_table_stmnt"]}


def load_data(event, context=None, use_lambda=False):
    try:
        if use_lambda:
            print("Invoking load_data lambda locally...")
            aws_lambda = boto3.client("lambda")
            response = aws_lambda.invoke(
                FunctionName="jobs-data-pipeline-dev-load_csv_to_redshift",
                InvocationType="RequestResponse",
                Payload=json.dumps(event),
            )
            print("Data successfuly loaded to redshift")
            if response.get("FunctionError"):
                raise Exception(
                    "An error occured durint lambda invocation. Check lambda logs"
                )
            payload = response["Payload"].read()
            return json.loads(payload)

        if isinstance(event, str):
            event = event.replace("'", '"')
            event = json.loads(event)

        params = dict(
            bucket=event.get("Bucket"),
            key=event.get("Key"),
            region=config.get("REGION", environ.get("REGION")),
            iam_role=config.get("REDSHIFT_IAM_ROLE", environ.get("REDSHIFT_IAM_ROLE")),
        )

        con = utils.get_database_conn()

        res = utils.load_csv_from_s3_to_redshift(**params, con=con)
        print(res)
        return {
            "result": f"s3://{config.get('TRANSFORMED_DATA_BUCKET', environ.get('TRANSFORMED_DATA_BUCKET'))}/{event.get('Key')}",
            "status": "Successful",
        }

    except Exception as e:
        raise e("An error occured while loading to redshift")
