import sys
import os
import datetime
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.datasets import Dataset

### Setup paths for custom scripts
sys.path.append(os.path.join(os.path.dirname(__file__), "scripts"))
from data_transform import replace_nulls, sort_by_date, clean_content


DATA_FILE = "/opt/airflow/data/tiktok_google_play_reviews.csv"
PROCESSED_DATA_SIGNAL = Dataset("/opt/airflow/data/processed_reviews.csv")

### Define Ownership
default_args = {
    "owner": "S.T - BigData",
    "retries": 0,
    "start_date": datetime.datetime(2026, 3, 27)  # Updated to current year
}


def check_file_empty():
    # Check if file exists and has size > 0
    if os.path.exists(DATA_FILE) and os.path.getsize(DATA_FILE) > 0:
        # Must use the TaskGroup prefix for the branch to work
        return "cleaning_group.replace_nulls_task"
    return "log_empty"


### DAG Definition
with DAG(
        "1_transform_data",
        default_args=default_args,
        schedule=None,
        catchup=False,
        tags=["assignment", "tiktok"]
) as dag:
    ### Sensor
    wait_for_file = FileSensor(
        task_id="wait_for_file",
        fs_conn_id="fs_default",
        filepath="tiktok_google_play_reviews.csv",
        poke_interval=10
    )

    ### Branching logic
    branch_task = BranchPythonOperator(
        task_id="check_if_empty",
        python_callable=check_file_empty
    )

    ### Scenario if empty
    log_empty = BashOperator(
        task_id="log_empty",
        bash_command='echo "FILE IS EMPTY - STOPPING"'
    )

    ### Data Processing TaskGroup
    with TaskGroup("cleaning_group") as cleaning_group:
        t1 = PythonOperator(
            task_id="replace_nulls_task",
            python_callable=replace_nulls
        )
        t2 = PythonOperator(
            task_id="sort_by_date_task",
            python_callable=sort_by_date
        )
        t3 = PythonOperator(
            task_id="clean_content_task",
            python_callable=clean_content,
            outlets=[PROCESSED_DATA_SIGNAL]  ### Triggers DAG
        )

        t1 >> t2 >> t3

    ### Data pipeline flow
    wait_for_file >> branch_task >> [cleaning_group, log_empty]