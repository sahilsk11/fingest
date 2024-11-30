import io
from typing import Tuple
from flask import Flask, request, jsonify
import boto3
import pandas as pd

import passwords
import sf_import

s3 = boto3.resource("s3")

wrapper = sf_import.SnowflakeWrapper(
    user=passwords.get_snowflake_user(),
    password=passwords.get_snowflake_password(),
    account=passwords.get_snowflake_account(),
    schema=passwords.get_snowflake_schema(),
    database=passwords.get_snowflake_database(),
)
snowflake_import_engine = sf_import.SnowflakeImportEngine(
    user=passwords.get_snowflake_user(),
    password=passwords.get_snowflake_password(),
    account=passwords.get_snowflake_account(),
    schema=passwords.get_snowflake_schema(),
    database=passwords.get_snowflake_database(),
    sf_wrapper=wrapper,
)

app = Flask(__name__)


def get_file_from_s3(bucket_name: str, file_path: str) -> Tuple[str, str]:
    # Get the object from the S3 bucket
    obj = s3.Object(bucket_name, file_path)
    # Get the file type
    file_type = file_path.split(".")[-1]
    # Read the object content as a string
    content = obj.get()["Body"].read().decode("utf-8")
    return content, file_type


@app.route("/file-uploaded", methods=["POST"])
def file_uploaded():
    # Get the data from the request
    data = request.json  # Assuming the data is sent as JSON
    s3_bucket = data.get("s3Bucket")
    s3_file_path = data.get("s3FilePath")
    source_institution = data.get("sourceInstitution")

    file_content, file_type = get_file_from_s3(s3_bucket, s3_file_path)

    if file_type.lower() != "csv":
        return jsonify({"error": f"Unsupported file type: {file_type}"}), 400

    csv_to_df = pd.read_csv(io.StringIO(file_content))
    import_run_id = snowflake_import_engine.import_csv(csv_to_df, source_institution)


    return jsonify({"importRunId": import_run_id}), 200


if __name__ == "__main__":
    app.run(debug=True, port=5010)
