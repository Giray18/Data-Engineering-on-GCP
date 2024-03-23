import pandas as pd
import json
from google.cloud import storage
import os
import glob
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import re
from io import StringIO
from json import loads, dumps
import gcsfs

def write_to_bq(project_id, bucket_name):
    # Instantiating big query client
    bq_client = bigquery.Client(project_id)
    # Getting bucket variable as source
    bucket = storage.Client().bucket(bucket_name)
    # If there are folders in bucket prefix part could be used
    for blob in bucket.list_blobs(prefix=""):
        # Checking for json blobs as list_blobs also returns folder_name
        if ".json" in blob.name: 
            # Parsing JSON file name to create table on BQ with same file name
            json_filename = blob.name.split('.json')[0]
        #    json_filename = re.findall(r".*/(.*).json",blob.name) #Extracting file name for BQ's table id
            # Determining table name
            table_id = f"capable-memory-417812.premiership.{json_filename}" 
            try: # Check if table exists and operate TRUNCATE/WRITE ROWS
               bq_client.get_table(table_id)
               uri = f"gs://{bucket_name}/{blob.name}"
               # Creating configs for load job operations (append)
               job_config = bigquery.LoadJobConfig(
               autodetect=True,
               write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
               source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON)
               print("Table {} already exists. Rows append operation be done.".format(table_id))
               load_job = bq_client.load_table_from_uri(
                     uri, table_id, location = 'europe-west8',  
                     job_config=job_config,
               )  
               # Make an API request.
               load_job.result()  
               # Waits for the job to complete.
               destination_table = bq_client.get_table(table_id)  
               # Make an API request.
               print("Total {} rows exits on the table.".format(destination_table.num_rows))
           # If table is not found, upload it WRITE If table empty.  
            except NotFound:   
               uri = f"gs://{bucket_name}/{blob.name}"
               print(uri)
               job_config = bigquery.LoadJobConfig(
               autodetect=True,
               write_disposition=bigquery.WriteDisposition.WRITE_EMPTY,
               source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON)
               load_job = bq_client.load_table_from_uri(
                     uri, table_id, location = 'europe-west8',  
                     job_config=job_config,
               )  
               # Make an API request.
               load_job.result()  
               # Waits for the job to complete.
               destination_table = bq_client.get_table(table_id)  # Make an API request.
               print("Loaded {} rows.".format(destination_table.num_rows))