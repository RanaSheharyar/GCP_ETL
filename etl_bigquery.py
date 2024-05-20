import functions_framework
import pandas as pd
from google.cloud import storage
from pandas_gbq import to_gbq
from datetime import datetime

# Instantiates a client
storage_client = storage.Client()
project_id = 'jobs-data-linkedin'
dataset_id = 'jobs_data'
table_id_new = 'jobs_data_latest'
table_id_existing = 'jobs_data_all'

def write_to_bq_new_table(df, table_name):
    df['data'] = df['data'].astype('str')
    response = to_gbq(df, f"{dataset_id}.{table_name}", project_id=project_id, if_exists='replace')  # Create new table
    return response

def write_to_bq_existing_table(df, table_id):
    df['data'] = df['data'].astype('str')
    response = to_gbq(df, f"{dataset_id}.{table_id}", project_id=project_id, if_exists='append')  # Append to existing table
    return response

def write_to_bq_dynamic_table(df, table_name):
    df['data'] = df['data'].astype('str')
    response = to_gbq(df, f"{dataset_id}.{table_name}", project_id=project_id, if_exists='replace')  # Create new table
    return response

def read_json_file(bucket_name, file_name, chunksize=10**5):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    
    # Stream the JSON file in chunks
    with blob.open("rb") as json_file:
        for chunk in pd.read_json(json_file, lines=True, chunksize=chunksize): # Adjust chunk size as needed
            yield chunk

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def hello_gcs(cloud_event):
    data = cloud_event.data

    event_id = cloud_event["id"]
    event_type = cloud_event["type"]

    bucket = data["bucket"]
    name = data["name"]
    metageneration = data["metageneration"]
    timeCreated = data["timeCreated"]
    updated = data["updated"]

    print(f"Event ID: {event_id}")
    print(f"Event type: {event_type}")
    print(f"Bucket: {bucket}")
    print(f"File: {name}")
    print(f"Metageneration: {metageneration}")
    print(f"Created: {timeCreated}")
    print(f"Updated: {updated}")
    
    # Dynamically generate table name based on the current date
    today_date = datetime.today().strftime('%m_%d')
    new_table_name = f'jobs_data_{today_date}'
    
    # Process JSON file in chunks
    for df_chunk in read_json_file(bucket, name):
        print(df_chunk.head(2))
        response_new = write_to_bq_new_table(df_chunk, table_id_new)  # Create new table with the chunk
        print(response_new)
        response_existing = write_to_bq_existing_table(df_chunk, table_id_existing)  # Append chunk to existing table
        print(response_existing)
        response_dynamic = write_to_bq_dynamic_table(df_chunk, new_table_name)  # Create new dynamic table
        print(response_dynamic)
    
    return "Processing completed"
