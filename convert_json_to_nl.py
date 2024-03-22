from google.cloud import storage
import json
from io import StringIO
import pandas as pd
import os
import glob
import numpy as np
from datetime import datetime, timedelta, date

# def crate_new_bucket(bucket_name, project_id):
#     # Instantiating storage class
#     storage_client = storage.Client(project_id)
#     # The name for the new bucket gathering from existing bucket
#     bucket_name_conv = f"{bucket_name}_jsonl_conv" # + "jsonl_conv"
#     # Creates the new bucket if bucket already exists using existing name with conversion
#     if storage_client.bucket(bucket_name_conv).exists():
#         bucket_new  = bucket_name_conv
#     # If bucket does not exists crates it and gets its name
#     else:
#         bucket_new = storage_client.create_bucket(bucket_name_conv)
#         bucket_new  = bucket_new.name
#     return bucket_new


# def convert_json_jsonl(bucket_name, project_id, blob_name = [], path = []):
#     ''' This function reads multiple json files from location passed as parameter 
#     and converts them to jsonl and saves into created bucket with an inner function
#     if bucket exists uses existing bucket'''
#     # Instantiating needed classes and methods
#     storage_client = storage.Client(project_id)
#     bucket = storage_client.bucket(bucket_name)
#     blobs = storage_client.list_blobs(bucket_name)
#     # New bucket creation
#     new_bucket = crate_new_bucket(bucket_name,project_id)

#     # Loop through blobs in bucket and converting nljson with saving into new bucket path
#     for blob in blobs:
#         df = pd.read_json("gs://{}/{}".format(bucket_name, blob.name)) 
#         df = df.to_json(f'gs://{new_bucket}/{blob.name}_nl.json',orient="records",lines=True)
#     return 'json_files_converted_nljson'


def crate_new_bucket(bucket_name, project_id):
    # Instantiating storage class
    storage_client = storage.Client(project_id)
    # The name for the new bucket gathering from existing bucket
    bucket_name_conv = f"{bucket_name}_jsonl_conv" # + "jsonl_conv"
    # Creates the new bucket if bucket already exists using existing name with conversion
    if storage_client.bucket(bucket_name_conv).exists():
        bucket_new  = bucket_name_conv
    # If bucket does not exists crates it and gets its name
    else:
        bucket_new = storage_client.create_bucket(bucket_name_conv, location='europe-west8')
        bucket_new.location = 'europe-west8'
        print("created bucket {} in {}".format(bucket_new.name, bucket_new.location))
        bucket_new  = bucket_new.name
    return bucket_new


def convert_json_jsonl(bucket_name, project_id, blob_name = [], path = []):
    ''' This function reads multiple json files from location passed as parameter 
    and converts them to jsonl and saves into created bucket with an inner function
    if bucket exists uses existing bucket'''
    # New bucket creation
    new_bucket = crate_new_bucket(bucket_name,project_id)

    # Instantiating needed classes and methods
    storage_client = storage.Client(project_id)
    bucket = storage_client.bucket(bucket_name)
    blobs = storage_client.list_blobs(bucket_name)
    
    # Loop through blobs in bucket and converting nljson with saving into new bucket path
    for blob in blobs:
        print(blob.name)
        df = pd.read_json("gs://{}/{}".format(bucket_name, blob.name)) 
        df.to_json(f'gs://{new_bucket}/{blob.name}',orient="records",lines=True)
    return 'json_files_converted_nljson'