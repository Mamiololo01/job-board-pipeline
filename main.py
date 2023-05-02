import etl
import utils
import dotenv
import argparse
from pathlib import Path

config = dotenv.dotenv_values()
parser = argparse.ArgumentParser()

parser.add_argument('--use-lambda', action='store_true')

RAW_DATA_BUCKET = config.get('RAW_DATA_BUCKET')
TRANSFORMED_DATA_BUCKET = config.get('TRANSFORMED_DATA_BUCKET')

titles = ['Data Engineer']
locations = ['US']
use_lambda = False

if __name__ == "__main__":
    args, _ = parser.parse_known_args()
    # raw_data_path = etl.extract_job_data(titles, locations)
    upload = utils.upload_file_to_s3(Path(r"C:\Users\hp\10alytics\job-board-service-pipeline\raw_jobs_data.json"), RAW_DATA_BUCKET)
    transformed_output = etl.transform_data(upload, use_lambda=args.use_lambda)
    print("Transformed Output: ", transformed_output)
    load_data = etl.load_data(
        event=transformed_output,
        use_lambda=args.use_lambda
    )