from google.cloud import storage
import json
from io import StringIO
import pandas as pd
import os
import glob
import numpy as np
# import dat
from datetime import datetime, timedelta, date


# def convert_json_jsonl(bucket_name, blob_name = []):
#     """Write and read a blob from GCS using file-like IO"""

#     storage_client = storage.Client()
#     bucket = storage_client.bucket(bucket_name)
#     blobs = storage_client.list_blobs(bucket_name)
#     # blob = bucket.blob(blob_name)
   
#     for blob in blobs:
#         d = json.loads(blob.download_as_string(client=None))
#         result = [json.dumps(values) for key,values in d.items()]
#         ndjson_str = '\n'.join(result)

#         with blob.open("w") as f:
#             f.write(ndjson_str)
        
#     return 'conversion_completed'



def convert_json_jsonl(path = []):
    ''' This function reads multiple json files from location passed as parameter 
    and transposes them by a condition '''
    df = pd.read_json(path)
    if len(df.columns) > len(df.index):
        df = df.transpose()
    else:
        df
    game_ids = df.index.to_list()
    col_with_json_val = [df[i].name for i in df.columns if "{" in str(df[i].iloc[0]) and ":" in str(df[i].iloc[0])]
    for col in df.columns:
        if df[col].dtype == "object" and df[col].name not in col_with_json_val:
            try:
                df[col] = [i.lower() for i in df[col]]
                df[col] = [i.replace('&', "and") for i in df[col]]
            except AttributeError:
                # pass
                for c in game_ids:
                    df[col][c] =  [v.lower() for v in df[col][c]]
                    df[col][c] =  [v.replace('&', "and") for v in df[col][c]]
    # df = df.to_json("/home/vforvalbuena/GCP_AIRFLOW/newline.json",orient="records",lines=True)
    df = df.to_json(orient="records",lines=True)
    return df