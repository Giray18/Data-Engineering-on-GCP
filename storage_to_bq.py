# Package Imports
import pandas as pd
from bulk_load_to_bq import *
from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Creating Variables and Arguments
yesterday = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())

# Creating source objects list to read from GCS
# source_objects_def = list_blob_elements("game_data_giray")

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

        # Runs big query loads
        python_task = PythonOperator(
        task_id='load_to_bq',
        python_callable=write_to_bq,
        op_kwargs={'bucket_name': 'tryoutdavar_jsonl_conv', 'project_id' : 'capable-memory-417812'},
        dag=dag)


# Transfer .jsonl file on storage to BQ
        # gcs_to_bq_load_file_1 = GoogleCloudStorageToBigQueryOperator(
        # task_id='gcs_to_bq_load_file_1',
        # bucket='game_data_giray_jsonl_conv',
        # # source_objects=f"{source_objects_def}",
        # source_objects= ['epl_2022_2023_07_02_2023.json'],
        # source_format='NEWLINE_DELIMITED_JSON',
        # encoding = 'UTF=32',
        # # destination_project_dataset_table='birincidingil.capable-memory-417812.premiership',
        # destination_project_dataset_table='capable-memory-417812.premiership.davar_1',
        # autodetect = True,
        # create_disposition='CREATE_IF_NEEDED',
        # write_disposition='WRITE_TRUNCATE')


        # gcs_to_bq_load_file_2 = GoogleCloudStorageToBigQueryOperator(
        # task_id='gcs_to_bq_load_file_2',
        # bucket='game_data_giray_jsonl_conv',
        # # source_objects=f"{source_objects_def}",
        # source_objects= ['epl_2022_2023_07_02_2024.json'],
        # source_format='NEWLINE_DELIMITED_JSON',
        # encoding = 'UTF=32',
        # # destination_project_dataset_table='birincidingil.capable-memory-417812.premiership',
        # destination_project_dataset_table='capable-memory-417812.premiership.davar_2',
        # autodetect = True,
        # create_disposition='CREATE_IF_NEEDED',
        # write_disposition='WRITE_TRUNCATE')

        # #Dependencies
        # gcs_to_bq_load_file_1 >> gcs_to_bq_load_file_2

         # Triggering next dag
        trigger = TriggerDagRunOperator(
        task_id="trigger_dependent_dag",
        trigger_dag_id="analytic_queries_as_table",
        # conf={'wait_for_completion':''}
        wait_for_completion=False,
        deferrable=False,  
        dag=dag)

        #Dependencies
        python_task >> trigger