import sys
import os
import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset

### Setup paths for custom scripts
sys.path.append(os.path.join(os.path.dirname(__file__), "scripts"))
from mongo_load import load_to_mongo

### Dataset that triggers this DAG
PROCESSED_DATA_SIGNAL = Dataset("/opt/airflow/data/processed_reviews.csv")

### Define Ownership
default_args = {
    "owner": "S.T - BigData",
    "retries": 0,
    "start_date": datetime.datetime(2026, 3, 27)
}

### DAG Definition
with DAG(
    "2_load_to_mongodb",
    default_args=default_args,
    schedule=[PROCESSED_DATA_SIGNAL], # Data-aware scheduling: Triggers when DAG 1 finishes
    catchup=False,
    tags=["assignment", "mongodb"]
) as dag:

    load_task = PythonOperator(
        task_id="load_to_mongo",
        python_callable=load_to_mongo
    )