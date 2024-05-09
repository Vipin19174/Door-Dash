import boto3
import pandas as pd
from io import StringIO, BytesIO
import json
from datetime import datetime




def lambda_handler(event, context):
    # Get the current date
    current_date = datetime.now().strftime('%Y-%m-%d')

    s3_client = boto3.client('s3')
    sns_client = boto3.client('sns')
    sns_arn = 'arn:aws:sns:ap-south-1:590183940123:DoorDash_notifications'
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    source_key = event['Records'][0]['s3']['object']['key']
    t_one = 0
    t_two = 0
    t_three = 0


    try:
        # Get the S3 bucket and object key from the Lambda event trigger
        # source_bucket = event['Records'][0]['s3']['bucket']['name']
        # source_key = event['Records'][0]['s3']['object']['key']

        # Use boto3 to get the JSON file from the source S3 bucket
        response     = s3_client.get_object(Bucket=source_bucket, Key=source_key)
        file_content = response["Body"].read().decode('utf-8')

        # Load JSON content into a Python dictionary
        json_data = json.loads(file_content)

        # Convert JSON data to a pandas DataFrame
        data = pd.DataFrame.from_dict(json_data)

        message = "Input S3 File {} has arrived and data has been read succesfuly !!".format("s3://" + source_bucket + "/" + source_key)
        res = sns_client.publish(Subject="SUCCESS - Daily Data Processing", TargetArn=sns_arn, Message=message,
                                 MessageStructure='text')
        t_one=1
    except Exception as err:

        message = "Input S3 File {} processing is Failed !!".format("s3://" + source_bucket + "/" + source_key)
        res = sns_client.publish(Subject="FAILED - Daily Data Processing", TargetArn=sns_arn, Message=message,
                                     MessageStructure='text')

    if t_one == 1:
        try:
            # Filter data by 'status' column
            filtered_data = data[data['status'] == 'delivered']

            # Convert filtered data to JSON string
            filtered_json = filtered_data.to_json(orient='records')

            # Write filtered JSON data to a temporary file
            temp_file = BytesIO(filtered_json.encode('utf-8'))

            # Define the destination key with the current date
            destination_key = f"{current_date}_doordash-target-zn.json"

            message = "Input S3 File {} has been transformed succesfuly !!".format(
                "s3://" + source_bucket + "/" + source_key)
            res = sns_client.publish(Subject="SUCCESS - Daily Data Processing", TargetArn=sns_arn, Message=message,
                                     MessageStructure='text')
            t_two=1

        except Exception as err:
            message = "Input S3 File {} transformation has Failed !!".format(
                "s3://" + source_bucket + "/" + source_key)
            res = sns_client.publish(Subject="FAILED - Daily Data Processing", TargetArn=sns_arn, Message=message,
                                         MessageStructure='text')

    if t_two==1:
        try:
            # Upload the temporary file to the destination S3 bucket
            destination_bucket = 'doordash-target-zn-19174'
            s3_client.upload_fileobj(temp_file, destination_bucket, destination_key)

            message = "Input S3 File {} has been proccessed and loaded successfully in the target bucket !!".format(
                "s3://" + source_bucket + "/" + source_key)
            res = sns_client.publish(Subject="SUCCESS - Daily Data Processing", TargetArn=sns_arn, Message=message,
                                     MessageStructure='text')

        except Exception as err:
            message = "Input S3 File {} upload has failed in the target bucket !!".format(
                "s3://" + source_bucket + "/" + source_key)
            res = sns_client.publish(Subject="FAILED - Daily Data Processing", TargetArn=sns_arn, Message=message,
                                     MessageStructure='text')




