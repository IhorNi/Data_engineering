import psycopg2
import logging

from datetime import datetime
from airflow.hooks.base_hook import BaseHook
from hdfs import InsecureClient
from pyspark.sql import SparkSession


def postgres_table_to_bronze(table_name):
    """Function to read data from postgres db to hdfs bronze stage"""

    connection = BaseHook.get_connection('dshop_connection_id')

    pg_creds = {
        'host': connection.host,
        'port': connection.port,
        'database': connection.schema,
        'user': connection.login,
        'password': connection.password
    }

    client = InsecureClient(f'http://127.0.0.1:50070/', user='user')

    with psycopg2.connect(**pg_creds) as pg_connection:

        cursor = pg_connection.cursor()

        folder_name = f'/bronze/postgres_data/{datetime.today().strftime("%Y-%m-%d")}'
        client.makedirs(folder_name)

        try:
            with client.write(f'{folder_name}/{table_name}.csv',) as csv_file:
                cursor.copy_expert("COPY public." +
                                   table_name + " TO STDOUT WITH HEADER CSV", csv_file)
            logging.info(f"Successful copy from {table_name} to bronze")
        except:
            logging.error(f"Copy from {table_name} failed")


def bronze_to_silver(table_name):
    """Function to get table from bronze stage do dummy transformation and save it to silver stage"""
    try:
        spark = SparkSession.builder \
            .config('spark.driver.extraClassPath', '/home/user/shared_folder/postgresql-42.2.20.jar') \
            .master('local') \
            .appName('project') \
            .getOrCreate()

        today = datetime.today().strftime("%Y-%m-%d")

        df = spark.read.load(f'/bronze/postgres_data/{today}/{table_name}.csv'
                             , header="true"
                             , inferSchema="true"
                             , format="csv")
        df = df.dropDuplicates()
        logging.info(f'{table_name} cleared from duplicates')
        df.write.parquet(f'/silver/postgres_data/{today}/{table_name}', mode='overwrite')
        logging.info(f'{table_name}.parquet successfully saved to SILVER')
    except:
        logging.error(f'Problems with {table_name}: check Spark and/or HDFS ')
