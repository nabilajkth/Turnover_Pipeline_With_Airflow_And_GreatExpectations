import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

import pandas as pd


default_args = {
    'owner': 'Nabila',
    'start_date': dt.datetime(2024, 11, 1),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=10),
}

# Define DAG
with DAG('IBM-DAG',
    default_args=default_args,
    schedule_interval='10-30/10 9 * * 6',
    catchup=False,
) as dag:
    extract = BashOperator(task_id='extract_data', bash_command='sudo -u airflow python /opt/airflow/dags/M3_extract.py')

    transform = BashOperator(task_id='transform_data', bash_command='sudo -u airflow python /opt/airflow/dags/M3_transform.py')

    load = BashOperator(task_id='load_data', bash_command='sudo -u airflow python /opt/airflow/dags/M3_load.py')
    
    

extract >> transform >> load