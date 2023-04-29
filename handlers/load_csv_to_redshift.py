import json
import helpers
import dotenv

config = dotenv.dotenv_values()


def handler(event, context):
    try:
        if isinstance(event, str):
            print("Its a string: ", event)
            event = json.loads(event)

        bucket = event.get("Bucket")
        key = event.get("Key")
        region = config.get("REGION")
        iam_role = config.get("REDSHIFT_IAM_ROLE")

        con = helpers.get_database_conn()
        res = helpers.load_csv_from_s3_to_redshift(
            bucket=bucket, key=key, region=region, iam_role=iam_role, con=con
        )

        return {
            "result": f"s3://{config.get('TRANSFORMED_DATA_BUCKET')}/{event.get('Key')}",
            "status": "Successful",
        }
    except Exception as e:
        raise e("An error occured while loading to redshift")
