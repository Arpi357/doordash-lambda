import os
import json
import boto3
import pandas as pd
import io
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
sns = boto3.client("sns")

TARGET_BUCKET = os.environ.get("TARGET_BUCKET")
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")

def process_file(bucket, key):
    logger.info(f"Processing file: s3://{bucket}/{key}")
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj['Body'].read()

    try:
        df = pd.read_json(io.BytesIO(body), lines=True)
    except ValueError:
        df = pd.read_json(io.BytesIO(body))

    if "status" not in df.columns:
        raise ValueError("Missing 'status' column in input data")

    df["status"] = df["status"].astype(str).str.lower()
    delivered = df[df["status"] == "delivered"]

    out_key = key.replace("-raw_input", "-delivered")
    if out_key == key:
        out_key = "processed/" + key

    output = delivered.to_json(orient="records", lines=True)
    s3.put_object(Bucket=TARGET_BUCKET, Key=out_key, Body=output.encode("utf-8"))

    return len(delivered), out_key


def lambda_handler(event, context):
    try:
        results = []
        for record in event.get("Records", []):
            bucket = record["s3"]["bucket"]["name"]
            key = record["s3"]["object"]["key"]
            count, out_key = process_file(bucket, key)
            results.append({
                "input": f"s3://{bucket}/{key}",
                "output": f"s3://{TARGET_BUCKET}/{out_key}",
                "rows": count
            })

        message = {"status": "success", "results": results}
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=json.dumps(message),
            Subject="DoorDash Processing: SUCCESS"
        )
        return {"statusCode": 200, "body": message}

    except Exception as e:
        logger.exception("Lambda failed")
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=str(e),
            Subject="DoorDash Processing: FAILURE"
        )
        raise
