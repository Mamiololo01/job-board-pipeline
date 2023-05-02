import json
import boto3
import dotenv
from datetime import datetime, timedelta
from pathlib import Path, WindowsPath, PosixPath
import pandas as pd
from typing import Union
import requests
import psycopg2
from psycopg2._psycopg import connection
from os import environ

config = dotenv.dotenv_values()


def pull_job_data(query):
    try:
        url = "https://jsearch.p.rapidapi.com/search"

        params = {"page": "1", "num_pages": "1", "date_posted": "today"}
        headers = {
            "X-RapidAPI-Key": config.get(
                "X_RAPID_API_KEY", environ.get("X_RAPID_API_KEY")
            ),
            "X-RapidAPI-Host": config.get(
                "X_RAPID_API_HOST", environ.get("X_RAPID_API_HOST")
            ),
        }

        params["query"] = query
        response = requests.get(url, headers=headers, params=params)
        return response.json()["data"]

    except KeyError:
        raise KeyError(
            "Could not load 'data' from response. API quote may have reached it's limit"
        )


def upload_file_to_s3(filepath: Union[PosixPath, WindowsPath, str], bucket: str):
    s3 = boto3.client("s3")
    with open(filepath, "rb") as f:
        date = datetime.now().date().strftime("%Y/%m/%d")
        if type(filepath) == str:
            filename = filepath.split("/")[-1]
        else:
            filename = filepath.name
        key = f"{date}/{filename}"
        s3.put_object(Body=f, Bucket=bucket, Key=key)

    return {"Bucket": bucket, "Key": key}


def get_file_from_s3(bucket, key, path_to_save="/tmp"):
    s3 = boto3.client("s3")
    path = Path(path_to_save)
    if not path.exists():
        path.mkdir()
    res = s3.get_object(Bucket=bucket, Key=key)
    filename = key.split("/")[-1]
    filepath = path / filename
    with open(filepath, "wb") as file:
        file.write(res["Body"].read())

    print(str(filepath))

    return str(filepath)


def process_data(
    filepath: Union[PosixPath, WindowsPath, str], path_to_save="/tmp", use_lambda=False
):
    pandas_to_sql_type_map = {
        "object": "VARCHAR",
        "datetime64[ns, UTC]": "TIMESTAMPTZ",
        "datetime64[ns]": "TIMESTAMP",
        "int64": "INT",
        "float64": "DECIMAL",
    }

    columns_to_extract = [
        "employer_website",
        "job_id",
        "job_employment_type",
        "job_title",
        "job_apply_link",
        "job_description",
        "job_city",
        "job_country",
        "job_posted_at_timestamp",
        "employer_company_type",
    ]

    with open(filepath, "r") as f:
        jd = json.loads(f.read())
    path = Path(path_to_save)
    if not path.exists():
        path.mkdir()

    if type(filepath) == str:
        filename = filepath.split("/")[-1].split("\\")[-1].split(".")[0]
    else:
        filename = filepath.name.split(".")[0]

    output_path = f"{str(path/filename)}.csv"
    df = pd.json_normalize(jd, max_level=0)
    df = df.loc[:, columns_to_extract]
    df["job_posted_at_timestamp"] = pd.to_datetime(
        df["job_posted_at_timestamp"], unit="s"
    )
    df["job_posted_at_timestamp"] = df["job_posted_at_timestamp"].add(
        timedelta(hours=1)
    )
    df["job_description"] = df["job_description"].apply(lambda x: "" + str(x) + "")
    schema = {
        k: pandas_to_sql_type_map.get(str(v)) for k, v in df.dtypes.to_dict().items()
    }
    schema["job_description"] = "VARCHAR(max)"
    df.to_csv(output_path, index=False)
    create_table_stmnt = generate_database_table_from_pandas_dtypes(
        "jobs", schema=schema
    )

    print("=" * 50)
    print(df.head())

    return {
        "output_path": str(Path(output_path).resolve()),
        "create_table_stmnt": create_table_stmnt,
    }


def get_database_conn():
    creds = {
        "dbname": config.get("DB_NAME", environ.get("DB_NAME")),
        "user": config.get("DB_USER", environ.get("DB_USER")),
        "password": config.get("DB_PASSWORD", environ.get("DB_PASSWORD")),
        "host": config.get("DB_HOST", environ.get("DB_HOST")),
        "port": config.get("DB_PORT", environ.get("DB_PORT")),
    }
    con = psycopg2.connect(**creds)
    return con


def generate_database_table_from_pandas_dtypes(name, schema: dict):
    arr = [f"\t{column} {dtype}\n" for column, dtype in schema.items()]
    return rf"""CREATE TABLE IF NOT EXISTS {name} (
    {",".join(arr)}
    )
    """


def create_database_table(create_statement=None, con: connection = None, **kwargs):
    ti = kwargs["ti"]
    if ti:
        results = ti.xcom_pull(task_ids="transform_json_to_csv", key="return_value")
        results = json.loads(results)
        create_statement = results.get("create_table_stmnt")
    con.set_session(autocommit=True)
    with con.cursor() as cursor:
        response = cursor.execute(create_statement)
    print(response)


def load_csv_from_s3_to_redshift(bucket, key, region, iam_role, con: connection):
    query = f"""
    COPY jobs
    FROM 's3://{bucket}/{key}'
    DELIMITER ','
    IGNOREHEADER 1
    csv quote as '"'
    REGION '{region}'
    IAM_ROLE '{iam_role}'
    timeformat AS 'auto';
    """
    try:
        con.set_session(autocommit=True)
        with con.cursor() as cursor:
            cursor.execute(query)
    except Exception as e:
        print("An error occured: ", e)
