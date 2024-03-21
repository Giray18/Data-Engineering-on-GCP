# Package Imports
import pandas as pd
# import dat
from blob_file_finder import convert_json_jsonl
from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

# Creating Variables and Arguments
yesterday = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())

# Creating source objects list to read from GCS
# source_objects_def = convert_json_jsonl("game_data_giray")

# Default Arguments
default_args = {
    'start_date': yesterday,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# DAG definitions
with DAG(dag_id= 'storage_to_bq',
        catchup=False,
        schedule_interval=timedelta(days=1),
        default_args=default_args
        ) as dag:




# Transfer .json file on storage to BQ
        gcs_to_bq_load_file = GoogleCloudStorageToBigQueryOperator(
        task_id='gcs_to_bq_load_file',
        bucket='game_data_giray',
        # source_objects=f"{source_objects_def}",
        source_objects= ['*.json'],
        source_format='NEWLINE_DELIMITED_JSON',
        encoding = 'UTF=32',
        # destination_project_dataset_table='birincidingil.capable-memory-417812.premiership',
        destination_project_dataset_table='capable-memory-417812.premiership.premiership',
        autodetect = True,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE'
        ,dag=dag)