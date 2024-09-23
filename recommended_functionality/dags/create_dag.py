import datetime
import os
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from script import aggregate_files

my_dag = DAG(
    dag_id="vk_task",
    start_date=datetime.datetime(2024, 9, 22, 7, 0, 0),
    schedule_interval=datetime.timedelta(hours=24),
)
PythonOperator(task_id="my_task",
               python_callable=aggregate_files,
               dag=my_dag)