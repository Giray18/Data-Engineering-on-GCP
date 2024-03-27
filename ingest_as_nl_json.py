# Package Imports
import pandas as pd
from convert_json_to_nl import *
from datetime import datetime, timedelta
from airflow.contrib.sensors.gcs_sensor import GCSObjectUpdateSensor,GCSObjectsWithPrefixExistenceSensor,GoogleCloudStorageUploadSessionCompleteSensor
from airflow import DAG
from airflow.models import DAG,xcom,TaskInstance
from airflow.models.dagrun import *
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator


# Creating Variables and Arguments
yesterday = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())
bucket_name = 'tryoutdavar'
project_id = 'capable-memory-417812'

# Default Arguments
default_args = {
    'start_date': yesterday,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}




# DAG definitions
with DAG(dag_id= 'ingest_as_json_nl',
        catchup=False,
        schedule_interval=timedelta(days=1),
        # schedule_interval='*/5 * * * *',
        default_args=default_args
        ) as dag:


        file_sensor_detect = GCSObjectsWithPrefixExistenceSensor(
            task_id='gcs_polling',  
            bucket= bucket_name,
            prefix='epl_2022_2023_07_02_202',
            do_xcom_push=True,
            dag=dag)

        # file_sensor_update = GCSObjectUpdateSensor(
        #         bucket= bucket_name,
        #         object= f'{i}', 
        #         task_id="gcs_object_update_sensor_task",
        #         timeout = 360,
        #         do_xcom_push=True,
        #         dag=dag)


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

        def convert_json_jsonl(bucket_name,project_id,file_name=[],**kwargs):
            ti = kwargs['ti']
            value = ti.xcom_pull(task_ids='gcs_polling',key='return_value')
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
                if blob.name in value:
                    # Getting json file content as string and converting to dict
                    # JSON_file = json.loads(blob.download_as_string(client=None))
                    d_json.update(json.loads(blob.download_as_string(client=None)))
                    # Blob name creation for converted blobs
                    # blob_name = f'{blob.name}_converted_jsonl'
                    blob_name = 'epl_2022_2023_season_stats.json'
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


        
        python_task = PythonOperator(
        task_id='conversions',
        python_callable=convert_json_jsonl,
        op_kwargs={'bucket_name': 'tryoutdavar', 'project_id' : 'capable-memory-417812'},
        dag=dag)
        
        # conversions = convert_json_jsonl(bucket_name,project_id)

        # Triggering next dag
        # trigger = TriggerDagRunOperator(
        # task_id="trigger_dependent_dag",
        # trigger_dag_id="storage_to_bq",
        # # conf={'wait_for_completion':''}
        # wait_for_completion=False,
        # deferrable=False,  
        # dag=dag)

        # #Dependencies
        file_sensor_detect >> python_task
