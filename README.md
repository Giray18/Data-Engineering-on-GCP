# Data Engineering on GCP Platform
## Motivation
Purpose of this repository is to create an ETL activity on GCP by harnessing Cloud Funtions (Triggering DAGS), Airflow (Cloud Composer) and BigQuery.
Source data is football Application data in JSON format.

Below is the flow diagram of established structure

## Flow Diagram
 ![picture alt](flow_diagram.PNG)  

 ## Repo elements
 **etl_premier_league_dag_triggered.py** : One dag consisting of 4 task groups being triggered by Google cloud functions when new file added to ingestion bucket. Below are explanations of task group duties.

  * convert_json_to_jsonl : Detects required files to be used on DAG by GCSObjectsWithPrefixExistenceSensor and pushes name of related files to XCOM storage. Following to that 2 functions named as crate_new_bucket and convert_json_jsonl  gets file names from XCOM and combines files into one file and saves into second storage bucket.

  * load_to_bq : Loads combined file into big query that was created on previous task group

  * analytical_queries : Creates views that holding requested analytical queries from ingested table on Bigquery. Applying flattenning functions and creating tabular datasets can be exported by downstream systems.

  * delete_from_staging : After all steps completed files located on first ingestion bucket being deleted by dag at this taskgroup.

**cloud function** : Holding python files used to create Cloud function that triggers main dag file

 



