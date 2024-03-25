# from airflow.contrib.sensors.gcs_sensor import GoogleCloudStoragePrefixSensor,GCSObjectExistenceSensor,GCSObjectsWithPrefixExistenceSensor
# from airflow.contrib.operators.gcs_list_operator import GoogleCloudStorageListOperator
# import datetime as dt
# from airflow.decorators import dag, task
# from datetime import datetime, timedelta
# from airflow.models import DAG,taskinstance,xcom
# from airflow.operators.dagrun_operator import TriggerDagRunOperator

# lasthour = dt.datetime.now() - dt.timedelta(hours=1)
# bucket_name = 'tryoutdavar'
# project_id = 'capable-memory-417812'




# # @task.branch(task_id="branch_task")
# # def branch_func(ti=None):
# #     # xcom_value = taskinstance.xcom_pull(key="return_value", task_ids="gcs_polling")
# #     xcom_value = int(ti.xcom_pull(task_ids="gcs_polling"))
# #     if len(xcom_value) >  len(ls):
# #         return "trigger" # run just this one task, skip all else
# #     else:
# #         return None # skip everything


# # Default Arguments
# default_args = {
#     'start_date': lasthour,
#     'depends_on_past': False
# }

# with DAG (
#      dag_id='GCS_sensor_dag',
#      catchup=False,
#      schedule_interval='*/5 * * * *',
#      default_args = default_args
#     ) as dag:


    # file_sensor = GoogleCloudStoragePrefixSensor(
    #                 task_id='gcs_polling',  
    #                 bucket= bucket_name,
    #                 prefix='epl_2022_2023_07_02_202',
    #                 dag=dag
    #             )

    # file_sensor = GCSObjectsWithPrefixExistenceSensor(
    #                 task_id='gcs_polling',  
    #                 bucket= bucket_name,
    #                 prefix='epl_2022_2023_07_02_202',
    #                 dag=dag
    #             )

#     file_sensor = GCSObjectUpdateSensor(
#     bucket=bucket_name,
#     object=FILE_NAME,
#     task_id="gcs_object_update_sensor_task",dag=dag
# )


    # file_sensor = GCSObjectExistenceSensor(
    #                 task_id='gcs_polling',  
    #                 bucket= bucket_name,
    #                 google_cloud_conn_id = 'google_cloud_default',
    #                 object='epl_2022_2023_07_02_202',
    #                 dag=dag
    #             )





    # # Triggering next dag
    # trigger = TriggerDagRunOperator(
    # task_id="trigger_dependent_dag",
    # trigger_dag_id="ingest_as_json_nl",
    # # conf={'wait_for_completion':''}
    # wait_for_completion=False,
    # deferrable=False,  
    # dag=dag)


    # #Dependencies
    # file_sensor >> trigger