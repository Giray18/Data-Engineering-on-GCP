from google.cloud import storage
import json
from io import StringIO
import pandas as pd
import os
import glob
import numpy as np
# import dat
from datetime import datetime, timedelta, date
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import re


# def list_blob_elements(bucket_name):
#     """Lists blob elements in mentioned bucket"""

#     storage_client = storage.Client()
#     bucket = storage_client.bucket(bucket_name)
#     blobs = storage_client.list_blobs(bucket_name)
#     # blob = bucket.blob(blob_name)
#     list_blobs = []
   
#     for blob in blobs:
#         list_blobs.append(blob.name)
        
#     return list_blobs

def hello_gcs(event, context):
    bq_client = bigquery.Client()
    bucket = storage.Client().bucket("bucket-name")
    for blob in bucket.list_blobs(prefix="folder-name/"):
        if ".csv" in blob.name: #Checking for csv blobs as list_blobs also returns folder_name
           job_config = bigquery.LoadJobConfig(
               autodetect=True,
               skip_leading_rows=1,
               source_format=bigquery.SourceFormat.CSV,
           )
           csv_filename = re.findall(r".*/(.*).csv",blob.name) #Extracting file name for BQ's table id
           bq_table_id = "project-name.dataset-name."+csv_filename[0] # Determining table name
       
           try: #Check if the table already exists and skip uploading it.
               bq_client.get_table(bq_table_id)
               print("Table {} already exists. Not uploaded.".format(bq_table_id))
           except NotFound: #If table is not found, upload it.    
               uri = "gs://bucket-name/"+blob.name
               print(uri)
               load_job = bq_client.load_table_from_uri(
                   uri, bq_table_id, job_config=job_config
               )  # Make an API request.
               load_job.result()  # Waits for the job to complete.
               destination_table = bq_client.get_table(bq_table_id)  # Make an API request.
               print("Table {} uploaded.".format(bq_table_id))