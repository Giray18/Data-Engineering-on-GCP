# Package Imports
import pandas as pd
from convert_json_to_nl import *
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

# Creating Variables and Arguments
yesterday = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())

# Default Arguments
default_args = {
    'start_date': yesterday,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# DAG definitions
with DAG(dag_id= 'analytic_queries_as_table',
        catchup=False,
        schedule_interval=timedelta(days=1),
        default_args=default_args
        ) as dag:

    top10_starters = BigQueryExecuteQueryOperator(
    task_id="find_top_10_starter",
    sql="""
    WITH starters_union AS (
    SELECT starters FROM `capable-memory-417812.premiership.epl_2022_2023_07_02_2024`,UNNEST(team1_startings) AS starters
    UNION ALL
    SELECT starters FROM `capable-memory-417812.premiership.epl_2022_2023_07_02_2024`,UNNEST(team2_startings) AS starters
    ) SELECT starters,COUNT(starters) AS starting_lineup_count FROM starters_union GROUP BY starters ORDER BY starting_lineup_count DESC LIMIT 10
    """,
    destination_dataset_table=f"capable-memory-417812.premiership.top10_starters",
    write_disposition="WRITE_TRUNCATE",
    use_legacy_sql=False,
)
