import json
import helpers
import dotenv

config = dotenv.dotenv_values()

def handler(event, context):
    if isinstance(event, str):
        event = json.loads(event)

    bucket = event.get('Bucket')
    key = event.get('Key')
    region = config.get('AWS_REGION')
    iam_role = config.get('REDSHIFT_IAM_ROLE')

    con = helpers.get_database_conn()
    res = helpers.load_csv_from_s3_to_redshift(
        bucket=bucket,
        key=key,
        region=region,
        iam_role=iam_role,
        con=con
    )

    return {
        'result': f"s3://{config.get('TRANSFORMED_DATA_BUCKET')}/{event.get('Key')} sucessfully loaded to",
        'res': res
    }
