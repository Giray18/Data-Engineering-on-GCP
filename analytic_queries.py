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



