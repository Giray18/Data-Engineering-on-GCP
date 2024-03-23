# Package Imports
import pandas as pd
from convert_json_to_nl import *
from datetime import datetime, timedelta
from airflow import DAG
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
        default_args=default_args
        ) as dag:

        # Runs json conversions
        python_task = PythonOperator(
        task_id='conversions',
        python_callable=convert_json_jsonl,
        op_kwargs={'bucket_name': 'tryoutdavar', 'project_id' : 'capable-memory-417812'},
        dag=dag)

        
        # conversions = convert_json_jsonl(bucket_name,project_id)

        # Triggering next dag
        trigger = TriggerDagRunOperator(
        task_id="trigger_dependent_dag",
        trigger_dag_id="storage_to_bq",
        # conf={'wait_for_completion':''}
        wait_for_completion=False,
        deferrable=False,  
        dag=dag)

        #Dependencies
        python_task >> trigger
