import sys
from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

sys.path.insert(0, "~/airflow/dags/functions")
from functions.postgres_to_hdfs import postgres_table_to_bronze
from functions.postgres_to_hdfs import bronze_to_silver


default_args = {
    'owner': 'airflow',
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'retries': 2
}


def return_tables():
    return ['aisles'
            , 'departments'
            , 'store_types'
            , 'location_areas'
            , 'clients'
            , 'products'
            , 'stores'
            , 'orders']


def to_bronze_group(table_name):

    return PythonOperator(
        task_id="load_" + table_name + "_to_bronze",
        python_callable=postgres_table_to_bronze,
        op_kwargs={"table_name": table_name},
        dag=dag
    )


def to_silver_group(table_name):

    return PythonOperator(
        task_id="load_" + table_name + "_to_silver",
        python_callable=bronze_to_silver,
        op_kwargs={"table_name": table_name},
        dag=dag
    )


dag = DAG(
    dag_id="postgres_to_hdfs",
    description="DAG with dynamic tasks",
    schedule_interval="30 12 * * *",  # 12:30 every day, scheduled before 12:45 out-of-stock data
    start_date=datetime(2021, 5, 19),
    default_args=default_args
)

bronze_stage = DummyOperator(
    task_id="bronze_stage",
    dag=dag
)

silver_stage = DummyOperator(
    task_id="silver_stage",
    dag=dag
)

for table in return_tables():
    to_bronze_group(table) >> bronze_stage >> to_silver_group(table) >> silver_stage
