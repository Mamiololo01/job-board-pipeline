import dotenv
import json
from helpers import get_file_from_s3, upload_file_to_s3, transform_data

config = dotenv.dotenv_values()


def handler(event, context):
    if isinstance(event, str):
        event = event.replace("'", '"')
        event = json.loads(event)

    bucket = event.get("Bucket")
    key = event.get("Key")
    filepath = get_file_from_s3(bucket=bucket, key=key, path_to_save="/tmp")
    transformed_result = transform_data(filepath=filepath, path_to_save="/tmp")
    result = upload_file_to_s3(
        filepath=transformed_result["output_path"],
        bucket=config.get("TRANSFORMED_DATA_BUCKET"),
    )
    return {"create_table_stmnt": transformed_result["create_table_stmnt"], **result}
