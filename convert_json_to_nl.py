from google.cloud import storage
import json
from json import loads, dumps
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
#         bucket_new = storage_client.create_bucket(bucket_name_conv, location='europe-west8')
#         bucket_new.location = 'europe-west8'
#         print("created bucket {} in {}".format(bucket_new.name, bucket_new.location))
#         bucket_new  = bucket_new.name
#     # Returning created/existing bucket name as variable
#     return bucket_new

# def convert_json_jsonl(bucket_name,project_id):
#     # Instantiating needed classes and methods
#     storage_client = storage.Client(project_id)
#     bucket = storage_client.bucket(bucket_name)
#     blobs = storage_client.list_blobs(bucket_name)

#     # New bucket creation
#     new_bucket = crate_new_bucket(bucket_name,project_id)

#     # Loop through blobs in bucket and converting nljson with saving into new bucket path
#     for blob in blobs:
#         # Getting json file content as string
#         JSON_file = json.loads(blob.download_as_string(client=None))
#         # Converting to JSON to JSON New Line
#         # nl_JSON_file = '\n'.join([json.dumps(JSON_file)])
#         # That part is alternative if outer keys would like to be dispersed
#         nl_JSON_file = '\n'.join([json.dumps(JSON_file[outer_key], sort_keys=True) 
#                           for outer_key in sorted(JSON_file.keys(),
#                                                   key=lambda x: int(x))])
#         # Calling new bucket name meta
#         bucket_new = storage_client.bucket(new_bucket)
#         # Blob name creation for converted blobs
#         blob_name = f'{blob.name}_converted_jsonl'
#         # Getting blob variable to apply read/write operations
#         blob = bucket_new.blob(blob_name)
#         # Writing new blob to new bucket
#         with blob.open("w") as f:
#             f.write(nl_JSON_file)
#     return 'files_on_bucket_converted_toJSONnl'

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
    # Returning created/existing bucket name as variable
    return bucket_new

def convert_json_jsonl(bucket_name,project_id):
    # Instantiating needed classes and methods
    storage_client = storage.Client(project_id)
    bucket = storage_client.bucket(bucket_name)
    blobs = storage_client.list_blobs(bucket_name)

    # New bucket creation
    new_bucket = crate_new_bucket(bucket_name,project_id)
    # Combine JSONS
    d_json = {}
    global blob_name
    blob_name = 'temp_blob'
    # Loop through blobs in bucket and converting nljson with saving into new bucket path
    for blob in blobs:
        # Getting json file content as string and converting to dict
        # JSON_file = json.loads(blob.download_as_string(client=None))
        d_json.update(json.loads(blob.download_as_string(client=None)))
        # Blob name creation for converted blobs
        # blob_name = f'{blob.name}_converted_jsonl'
        blob_name = blob_name = 'epl_2022_2023_season_stats.json'
    # Converting to JSON to JSON New Line
    # nl_JSON_file = '\n'.join([json.dumps(d_json)])
    # That part is alternative if outer keys would like to be dispersed
    nl_JSON_file = '\n'.join([json.dumps(d_json[outer_key], sort_keys=True) 
                        for outer_key in sorted(d_json.keys(),
                                                key=lambda x: int(x))])
    # Calling new bucket name meta
    bucket_new = storage_client.bucket(new_bucket)
    # Getting blob variable to apply read/write operations
    blob = bucket_new.blob(blob_name)
    # Writing new blob to new bucket
    with blob.open("w") as f:
        f.write(nl_JSON_file)
    return 'files_on_bucket_converted_toJSONnl'