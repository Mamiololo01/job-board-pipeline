import requests
import json
import boto3
import dotenv
from datetime import datetime
from pathlib import Path
import pandas as pd
import psycopg2

config = dotenv.dotenv_values()


titles = ['Data Engineer', 'Data Analyst']
locations = ['US', 'UK', 'Canada']

def pull_job_data(jobtitles, locations):
    url = "https://jsearch.p.rapidapi.com/search"

    params = {
        "page":"1",
        "num_pages":"1",
        "date_posted": "today"
    }

    headers = {
        "X-RapidAPI-Key": config.get('X_RAPID_API_KEY'),
        "X-RapidAPI-Host": config.get('X_RAPID_API_HOST')
    }

    queries = [f'{title} in {location}' for location in locations for title in jobtitles]


    results = []
    for query in queries:
        params['query'] = query
        response = requests.get(url, headers=headers, params=params)
        results.extend(response.json()['data'])

    with open("raw_jobs_data.json", 'w') as f:
        json.dump(results, f, indent=4)


def upload_file_to_s3(filepath, bucket):
    s3 = boto3.client('s3')

    with open(filepath, 'rb') as f:
        date = datetime.now().date().strftime('%Y/%m/%d')
        filename = filepath.split('/')[-1]
        key = f'{date}/{filename}'
        s3.put_object(Body=f, Bucket=bucket, Key=key)
    
    return {
        'Bucket': bucket,
        'Key': key
    }

def get_file_from_s3(bucket, key, path_to_save='.', format='json'):
    s3 = boto3.client('s3')
    path = Path(path_to_save)
    if not path.exists():
        path.mkdir()
    res = s3.get_object(Bucket=bucket, Key=key)
    filename = key.split('/')[-1]
    filepath = path/filename
    with open(filepath, 'wb') as file:
        file.write(res['Body'].read())
    print(str(filepath))
    return str(filepath)
pandas_to_sql = {
    'object': 'VARCHAR',
    'datetime64[ns, UTC]': 'TIMESTAMP',
}
columns_to_extract = ['employer_website', 'job_id', 'job_employment_type', 'job_title',
'job_apply_link', 'job_description', 'job_city', 'job_country',
'job_posted_at_timestamp', 'employer_company_type']

def transform_data(filepath, path_to_save='.'):
    with open(filepath, 'r') as f:
        jd = json.loads(f.read())
    path = Path(path_to_save)
    if not path.exists():
        path.mkdir()
    filename = filepath.split('/')[-1].split('\\')[-1].split('.')[0]
    output_path = f'{str(path/filename)}.csv'
    df = pd.json_normalize(jd, max_level=0)
    df = df.loc[:, columns_to_extract]
    df['job_posted_at_timestamp'] = pd.to_datetime(df['job_posted_at_timestamp'], utc=True, unit='s')
    schema = {k: pandas_to_sql.get(str(v)) for k, v in df.dtypes.to_dict().items()}
    df.to_csv(output_path, index=False)

    return {
        'output_path': Path(output_path).absolute(),
        'schema': schema
    }



def get_env_database_credentials(config):
    creds = {
        'dbname': config.get('DB_NAME'),
        'user': config.get('DB_USER'),
        'password': config.get('DB_PASSWORD'),
        'host': config.get('DB_HOST'),
        'port': config.get('DB_PORT'),
    }
    return creds

# pull_job_data(titles, locations)
upload = upload_file_to_s3('raw_jobs_data.json', 'raw-jobs-data-sayo')
print("JSON Upload: ", upload)
p = get_file_from_s3(upload['Bucket'], upload['Key'], path_to_save='extract')

transformed_path = transform_data(p, path_to_save='transformed')
print(transformed_path)
upload = upload_file_to_s3('transformed/raw_jobs_data.csv', 'transformed-jobs-data-sayo')
print("JSON Upload: ", upload)

con = psycopg2.connect(**get_env_database_credentials(config))
cursor = con.cursor()


def generate_database_table_from_pandas_dtypes(name, schema: dict):
    arr = [f"\t{column} {dtype}\n" for column, dtype in schema.items()]
    # s 
    return f"""CREATE TABLE IF NOT EXISTS {name} (
    {f','.join(arr)}
    )
    """
query = generate_database_table_from_pandas_dtypes('jobs', transformed_path['schema'])

cursor.execute()


cursor.execute("""
COPY jobs
FROM 'transformed\\raw_jobs_data.csv'
DELIMITER ','
""")