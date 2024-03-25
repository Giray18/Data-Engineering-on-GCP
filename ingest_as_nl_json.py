# Package Imports
import pandas as pd
from convert_json_to_nl import *
from datetime import datetime, timedelta
from airflow.contrib.sensors.gcs_sensor import GCSObjectUpdateSensor,GCSObjectsWithPrefixExistenceSensor
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


def eben(**kwargs):
    file_sensor_detect = GCSObjectsWithPrefixExistenceSensor(
            task_id='gcs_polling',  
            bucket= bucket_name,
            prefix='epl_2022_2023_07_02_202',
            # do_xcom_push=True,
            dag=dag)
    ti = kwargs['ti']
    ti.xcom_push(key='dadan', value = file_sensor_detect )
    return "ok"

# DAG definitions
with DAG(dag_id= 'ingest_as_json_nl',
        catchup=False,
        schedule_interval=timedelta(days=1),
        # schedule_interval='*/5 * * * *',
        default_args=default_args
        ) as dag:

        # Runs json conversions
        python_task = PythonOperator(
        task_id='conversions',
        python_callable=convert_json_jsonl,
        op_kwargs={'bucket_name': 'tryoutdavar', 'project_id' : 'capable-memory-417812'},
        dag=dag)

#         file_sensor_detect = GCSObjectsWithPrefixExistenceSensor(
#                     task_id='gcs_polling',  
#                     bucket= bucket_name,
#                     prefix='epl_2022_2023_07_02_202',
#                     # do_xcom_push=True,
#                     dag=dag
# )


        # def eben(**kwargs):
        #     file_sensor_detect = GCSObjectsWithPrefixExistenceSensor(
        #             task_id='gcs_polling',  
        #             bucket= bucket_name,
        #             prefix='epl_2022_2023_07_02_202',
        #             # do_xcom_push=True,
        #             dag=dag)
        #     ti = kwargs['ti']
        #     ti.xcom_push(key='dadan', value = file_sensor_detect )
        #     return "ok"

        eben_2 = PythonOperator(
            task_id='eben_info',
            python_callable= eben,
            provide_context=True,
            dag=dag
        )


    #     # task_instance = TaskInstance(file_sensor_detect)
        # dg = DagRun(dag_id= 'ingest_as_json_nl')
        # ti = dg.get_task_instance(task_id='gcs_polling')
        # # dg.xcom_pull
        # # ti = dagrun.get_task_instance('gcs_polling')
        # # ti= get_task_instance(task_ids='gcs_polling')
        # value = dg.xcom_pull(key = 'return_value',task_ids='gcs_polling')
        # value = ti.xcom_pull(key = 'return_value',task_ids='gcs_polling')

        value = ti.xcom_pull(task_ids='eben_2',key='dadan')

        for i in ti.xcom_pull(task_ids='eben_2',key='dadan'):
            file_sensor = GCSObjectUpdateSensor(
            bucket= bucket_name,
            object= f'{i}', 
            task_id="gcs_object_update_sensor_task_{i}",
            timeout = 360,
            dag=dag
    )

        
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
        # file_sensor_detect >> eben 
