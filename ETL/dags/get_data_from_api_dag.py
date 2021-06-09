from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import sys
sys.path.insert(0, "~/airflow/dags/functions")
from functions.oos_api import get_data_from_api
from functions.oos_api import api_data_to_silver




default_args = {
    'owner': 'airflow',
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'retries': 2
}

dag = DAG(
    'out_of_stock_dag',
    description='out_of_stock_dag',
    schedule_interval='30 12 * * *', #12:30 every day
    start_date=datetime(2021, 5, 4, 1, 0),
    default_args=default_args
)

t1 = PythonOperator(
    task_id='get_out_of_stock',
    dag=dag,
    python_callable=get_data_from_api
)

bronze_stage = DummyOperator(
    task_id="bronze_stage",
    dag=dag
)

t2 = PythonOperator(
    task_id='out_of_stock_to_silver',
    dag=dag,
    python_callable=api_data_to_silver
)

silver_stage = DummyOperator(
    task_id="silver_stage",
    dag=dag
)

t1 >> bronze_stage >> t2 >> silver_stage
