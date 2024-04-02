from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import json
from json import loads, dumps
from io import StringIO
import pandas as pd
import os
import variables
import airflow
import glob
import numpy as np
from datetime import datetime, timedelta, date
import datetime
from airflow.contrib.sensors.gcs_sensor import GCSObjectUpdateSensor,GCSObjectsWithPrefixExistenceSensor,GoogleCloudStorageUploadSessionCompleteSensor
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.decorators import dag, task, task_group
from airflow.models import DAG,xcom,TaskInstance
from airflow.models.dagrun import *
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

# Creating Variables and Arguments
yesterday = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())
bucket_name = variables.variables_dict()['bucket_name']
project_id = variables.variables_dict()['project_id']
converted_bucket_name = variables.variables_dict()['converted_bucket_name']

# Default Arguments
# default_args = {
#     'start_date': yesterday,
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=1)
# }

# DAG definitions
# with DAG(dag_id= 'etl_premier_league_dag',
#         # catchup=False,
#         start_date=datetime.datetime(2021, 1, 1),
#         schedule_interval=None,
#         # default_args=default_args
#         ) as dag:

with airflow.DAG(
    "etl_premier_league_dag",
    start_date=yesterday,
    # Not scheduled, trigger only
    schedule_interval=None,
    ) as dag:

        # On Task Group - 1 Merging all stream files into one file and convert JSON to JSONL for big query upload also creates a new bucket
        # if there is no one created for jsonl files
        with TaskGroup('convert_json_to_jsonl') as tg1:
            file_sensor_detect = GCSObjectsWithPrefixExistenceSensor(
                task_id='gcs_polling',  
                bucket= bucket_name,
                prefix='epl_2022_2023_07_02_202',
                do_xcom_push=True,
                timeout = 120,
                dag=dag)

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
                value = ti.xcom_pull(task_ids='convert_json_to_jsonl.gcs_polling',key='return_value')
                print(value)
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
            op_kwargs={'bucket_name': bucket_name, 'project_id' : project_id},
            dag=dag)

            file_sensor_detect >> python_task


        # On Task Group - 2 Loading combined JSONL file into a BQ table. If table does not exists creates it 
        # Code is capable fot adjusting notfound exception
        with TaskGroup('load_to_bq') as tg2:
            def write_to_bq(project_id, converted_bucket_name):
                # Instantiating big query client
                bq_client = bigquery.Client(project_id)
                # Getting bucket variable as source
                bucket = storage.Client().bucket(converted_bucket_name)
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
                            uri = f"gs://{converted_bucket_name}/{blob.name}"
                            # Creating configs for load job operations (append)
                            job_config = bigquery.LoadJobConfig(
                            autodetect=True,
                            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
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
                            uri = f"gs://{converted_bucket_name}/{blob.name}"
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

            python_task = PythonOperator(
                task_id='loading_bq',
                python_callable=write_to_bq,
                op_kwargs={'converted_bucket_name': converted_bucket_name, 'project_id' : project_id},
                dag=dag)

        # On Task Group - 3 Creating tables/views on BQ for requested 
        with TaskGroup('analytical_queries') as tg3:
            top10_starters = BigQueryExecuteQueryOperator(
            task_id="find_top_10_starter",
            sql="""
            WITH starters_union AS (
            SELECT starters FROM `capable-memory-417812.premiership.epl_2022_2023_season_stats`,UNNEST(team1_startings) AS starters
            UNION ALL
            SELECT starters FROM `capable-memory-417812.premiership.epl_2022_2023_season_stats`,UNNEST(team2_startings) AS starters
            ) SELECT starters,COUNT(starters) AS starting_lineup_count FROM starters_union GROUP BY starters ORDER BY starting_lineup_count DESC LIMIT 10
            """,
            destination_dataset_table=f"capable-memory-417812.premiership.top10_starters",
            write_disposition="WRITE_TRUNCATE",
            use_legacy_sql=False,
        )

            top10_scoring_teams = BigQueryExecuteQueryOperator(
            task_id="find_top_10_scoring_teams",
            sql="""
            WITH RECURSIVE 
            game_scores AS(
            SELECT SPLIT(REGEXP_EXTRACT_ALL(event[OFFSET(0)],r'\,(.*?)\.')[0],",") AS game_scores_goals FROM `capable-memory-417812.premiership.epl_2022_2023_season_stats`)
            ,game_scores_by_teams AS(
            SELECT REGEXP_REPLACE(game_scores_teams,r'\s[0-9]+','') AS team_name,RIGHT(game_scores_teams,1) AS goals FROM game_scores, UNNEST(game_scores_goals) AS game_scores_teams)
            SELECT team_name, SUM(CAST(goals AS INT64)) AS total_goal FROM game_scores_by_teams GROUP BY team_name ORDER BY total_goal DESC LIMIT 10
            """,
            destination_dataset_table=f"capable-memory-417812.premiership.top10_scoring_teams",
            write_disposition="WRITE_TRUNCATE",
            use_legacy_sql=False,
        )


            total_offsides_chart_all_teams = BigQueryExecuteQueryOperator(
            task_id="offside_chart",
            sql="""
            WITH total_offsides_chart AS (
            SELECT REPLACE(team2_name,'&','and') AS team_names,SUM(team2_stat.offsides) AS total_offsides FROM `capable-memory-417812.premiership.epl_2022_2023_season_stats` GROUP BY team_names 
            UNION ALL
            SELECT REPLACE(team1_name,'&','and') AS team_names,SUM(team1_stat.offsides) AS total_offsides FROM `capable-memory-417812.premiership.epl_2022_2023_season_stats` GROUP BY team_names)
            SELECT team_names,SUM(total_offsides) AS total_offsides_caught FROM total_offsides_chart GROUP BY team_names ORDER BY total_offsides_caught DESC
            """,
            destination_dataset_table=f"capable-memory-417812.premiership.total_offsides_chart_all_teams",
            write_disposition="WRITE_TRUNCATE",
            use_legacy_sql=False,
        )

            total_goal_chart_teams  = BigQueryExecuteQueryOperator(
            task_id="goal_chart_weekly",
            sql="""
            WITH
            RECURSIVE game_scores AS(
            SELECT
            SPLIT(REGEXP_EXTRACT_ALL(event[
            OFFSET
                (0)],r'\,(.*?)\.')[0],",") AS game_scores_goals,
            CAST(RIGHT(matchweek,2)AS INT64) AS game_week
            FROM
            `capable-memory-417812.premiership.epl_2022_2023_season_stats`),
            game_scores_by_teams AS(
            SELECT
            REGEXP_REPLACE(game_scores_teams,r'\s[0-9]+','') AS team_name,
            game_week,
            CAST(RIGHT(game_scores_teams,1) AS INT64) AS goals
            FROM
            game_scores,
            UNNEST(game_scores_goals) AS game_scores_teams)
            SELECT
            team_name,
            game_week,
            goals as goals_this_week,
            LAG(goals) OVER (PARTITION BY team_name ORDER BY game_week) AS goals_last_week,
            goals - LAG(goals) OVER (PARTITION BY team_name ORDER BY game_week) AS diff_last_week,
            SUM(goals) OVER(PARTITION BY team_name ORDER BY game_week) AS running_total_goal,
            SUM(goals) OVER(PARTITION BY team_name) AS total_goals,
            ROUND(SAFE_DIVIDE(SUM(goals) OVER(PARTITION BY team_name ORDER BY game_week),SUM(goals) OVER(PARTITION BY team_name)),2) AS perc_to_total_goals,
            AVG(goals) OVER(PARTITION BY team_name ORDER BY game_week  ROWS BETWEEN 3 PRECEDING AND CURRENT ROW ) AS running_avg_3_weeks
            FROM
            game_scores_by_teams
            ORDER BY
            team_name ,game_week 
            """,
            destination_dataset_table=f"capable-memory-417812.premiership.total_goal_chart_weekly",
            write_disposition="WRITE_TRUNCATE",
            use_legacy_sql=False,
        )

        # On Task Group - 4 Deleting temp files uploded onto staging bucket
        with TaskGroup('delete_from_staging') as tg4:
            def delete_blob(bucket_name, project_id):
                storage_client = storage.Client(project_id)
                bucket = storage_client.get_bucket(bucket_name)
                # list all objects in the directory
                blobs = bucket.list_blobs(prefix='')
                for blob in blobs:
                    blob.delete()
                return 'blobs deleted'

            python_task = PythonOperator(
                task_id='delete_staging_files',
                python_callable=delete_blob,
                op_kwargs={'bucket_name': bucket_name, 'project_id' : project_id},
                dag=dag)


        # Task Group Dependencies
        tg1 >> tg2 >> tg3 >> tg4

