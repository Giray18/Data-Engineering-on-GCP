# Data Engineering on GCP Platform
## Motivation
Purpose of this repository is to create an ETL activity on GCP by harnessing Cloud Funtions (Triggering DAGS), Airflow (Cloud Composer) and BigQuery.
Source data is football Application data in JSON format.

Below is the flow diagram of established structure

## Flow Diagram
 ![picture alt](flow_diagram.PNG)  

 ## Repo elements
 **etl_premier_league_dag_triggered.py** : One dag consisting of 4 task groups being triggered by Google cloud functions when new file added to ingestion bucket. 

   **convert_json_to_jsonl** : Detects required files to be used on DAG by GCSObjectsWithPrefixExistenceSensor and pushes name of related files to XCOM storage.
 



