import json
import boto3
import os
from datetime import datetime
import sqlalchemy
from sqlalchemy import create_engine, text
import pandas as pd 
from datetime import datetime

   
s3 = boto3.client('s3')
source_bucket = 'wikitablescrapexample'
destination_bucket = 'wikitablescrapexample'
destination_prefix = 'amazon_sprinklr_pull/latest_tag_mapping/'
paid_tag_file = "Paid_Tags_15_TagPull.json"
organic_tag_file = "Organic_Tags_12_TagPull.json"
tagg_file_location = 'amazon_sprinklr_pull/latest_tag_mapping/'
 
tag_files = [
    
    'Organic_Tags_12_TagPull.json',
    'Target_Geography_17_TagPull.json'
   
]
 
paid_tag_processing = ['Paid_Tags_15_TagPull.json', 'PAID_Tags_2024_13_TagPull.json']

def read_json_lines_from_s3(bucket, key):
    """Reads a JSON file line-by-line from S3 and returns a DataFrame."""
    response = s3.get_object(Bucket=bucket, Key=key)
    json_lines = response['Body'].read().decode('utf-8').splitlines()
    data = [json.loads(line) for line in json_lines]
    return pd.DataFrame(data)

 
def process_and_merge_paid_tags(subfolder):
    file_paths = [os.path.join(subfolder, filename) for filename in paid_tag_processing]

    # Read and preprocess the JSON files
    dataframes = [read_json_lines_from_s3(source_bucket, file_path) for file_path in file_paths]

    # Assuming you might need to preprocess dataframes[1] similar to your original question
    columns_to_drop = [
        'GCCI_SOCIAL_MEDIA__REQUESTING_PR_ORG__OUTBOUND_MESSAGE',
        'GCCI_SOCIAL_MEDIA_EMEA_APAC__TIER_1_EVENT___OUTBOUND_MESSAGE',
        'GCCI_SOCIAL_MEDIA_EMEA_APAC__TIER_1_EVENT___PAID_INITIATIVE'
    ]
    dataframes[0] = dataframes[0].drop(columns=columns_to_drop, errors='ignore')

    # Merge DataFrames on 'AD_VARIANT_NAME' column or another suitable column
    merged_df = pd.merge(dataframes[1], dataframes[0], on='AD_VARIANT_NAME', how='left')

    # Convert the merged DataFrame to JSON Lines format
    merged_json = merged_df.to_json(orient='records', lines=True)

    # Determine destination key based on subfolder/pull date
    destination_key = os.path.join(tagg_file_location, "Paid_Tags_15_TagPull.json")

    # Save the merged JSON to S3
    s3.put_object(Bucket=destination_bucket, Key=destination_key, Body=merged_json.encode('utf-8'))
 
 

def copy_tag_files(subfolder):
    # Extract the pull date from the subfolder path
    try:
        date_str = os.path.basename(subfolder).split('_')[0]
        pull_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        print(f"Pull date extracted: {pull_date}")
        
        # Store the pull date in the destination folder
        pull_date_filename = os.path.join(destination_prefix, 'pull_date.txt')
        pull_date_str = pull_date.strftime('%Y-%m-%d')
        s3.put_object(Bucket=destination_bucket, Key=pull_date_filename, Body=pull_date_str.encode())
        print(f"Successfully stored pull date in {pull_date_filename}")
    except Exception as e:
        print(f"Failed to extract or store pull date: {str(e)}")
        return
 
    # List objects in the subfolder
    response = s3.list_objects_v2(Bucket=source_bucket, Prefix=subfolder)
    
    if 'Contents' not in response:
        print(f"No files found in the subfolder: {subfolder}")
        return
 
    for content in response.get('Contents', []):
        key = content['Key']
        filename = os.path.basename(key)
        if filename in tag_files:
            copy_source = {'Bucket': source_bucket, 'Key': key}
            destination_key = os.path.join(destination_prefix, filename)
            s3.copy_object(Bucket=destination_bucket, CopySource=copy_source, Key=destination_key)
            print(f"Successfully copied {filename} to {destination_key}")
    
def lambda_handler(event, context):
    try:
        # Capture the file name from the event
        object_key = event['Records'][0]['s3']['object']['key']
        
        #object_key = 'amazon_sprinklr_pull/Tag-Pull/2024-02-21_00-27-39_622/'
        
        
        # Identify the folder/subdirectory from the object key
        subfolder = os.path.dirname(object_key)

        # Copy the specified tag files from this subfolder to the destination
        copy_tag_files(subfolder)
        
        process_and_merge_paid_tags(subfolder)
          
                # Only proceed to invoke another Lambda if today is Tuesday or Friday
        today = datetime.now()
        if today.weekday() in (1, 5):  # 1 is Tuesday, 4 is Friday
            lambda_client = boto3.client('lambda')
            invoke_response = lambda_client.invoke(
                FunctionName='prod_push_latest_tag_core',  # Replace with your target Lambda's name
                InvocationType='Event',  # Asynchronous invocation
                Payload=json.dumps({'key': 'value'})  # Optional payload
            )
            print(f"Successfully invoked another Lambda: {invoke_response}")
     
 
        
        return {
            'statusCode': 200,
            'body': json.dumps('Operation completed.')
        }
    except Exception as e:
        print(f"Error in lambda_handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps('An error occurred.')
        }
  
