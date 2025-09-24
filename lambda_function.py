import json
import boto3
import os
import pandas as pd
from io import StringIO

# Initialize AWS clients
s3_client = boto3.client('s3')
sns_client = boto3.client('sns')

# Environment variables (set these in Lambda console)
LANDING_BUCKET = os.environ.get('LANDING_BUCKET', 'doordash-landing-zn')
TARGET_BUCKET = os.environ.get('TARGET_BUCKET', 'doordash-target-zn')
SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN')  # SNS topic ARN

def lambda_handler(event, context):
    try:
        # Get S3 object info from the event
        record = event['Records'][0]['s3']
        bucket_name = record['bucket']['name']
        file_key = record['object']['key']

        print(f"Processing file: s3://{bucket_name}/{file_key}")

        # Download JSON file from S3
        obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        file_content = obj['Body'].read().decode('utf-8')

        # Read JSON into pandas DataFrame
        data = pd.read_json(StringIO(file_content))

        # Filter records where status is "delivered"
        filtered_data = data[data['status'] == 'delivered']

        if filtered_data.empty:
            print("No delivered records found.")
        else:
            # Convert filtered DataFrame to JSON string
            filtered_json = filtered_data.to_json(orient='records', lines=False)

            # Create target file key (replace 'raw_input' with 'processed')
            target_key = file_key.replace('raw_input', 'processed')

            # Upload filtered JSON to target S3 bucket
            s3_client.put_object(
                Bucket=TARGET_BUCKET,
                Key=target_key,
                Body=filtered_json
            )
            print(f"Filtered data uploaded to s3://{TARGET_BUCKET}/{target_key}")

        # Publish success notification to SNS
        message = f"Successfully processed {file_key}. Filtered records: {len(filtered_data)}"
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=message,
            Subject='DoorDash Data Processing Success'
        )

        return {
            'statusCode': 200,
            'body': message
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        # Publish failure notification to SNS
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=f"Failed to process {file_key}. Error: {str(e)}",
            Subject='DoorDash Data Processing Failure'
        )
        raise e
