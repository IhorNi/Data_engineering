import os
import requests
import json
import yaml
import logging

from pyspark.sql import SparkSession
from airflow.hooks.base_hook import BaseHook
from hdfs import InsecureClient
from datetime import datetime


def load_config(config_name):
    """Function to safe load API configuration file """

    config_path = os.path.join(os.getcwd(), config_name)
    try:
        with open(config_path, mode='r') as yaml_file:
            configuration = yaml.safe_load(yaml_file)
    except FileNotFoundError:
        print(f"Config {config_path} not found")
        configuration = {}

    return configuration


def get_homework_token(configuration):
    """Function gets token for API authorization"""

    url = configuration['API']['url'] + configuration['AUTH']['endpoint']
    data = configuration['AUTH']['payload']
    headers = {'content-type': 'application/json'}

    try:
        result = requests.post(url, data=json.dumps(data), headers=headers, timeout=5)
        result.raise_for_status()
        result = result.json()
        logging.info('Token is received')
    except requests.HTTPError:
        logging.error(f'No token. Exception with: {url}')
        result = None

    return result


def transform_api_response_list_to_dict(list_to_transform, key_value):
    """Function transforms response data: list of one-item dicts to multi-item dict"""

    date_from_list = list_to_transform[0]['date']  # одна дата дублируется в каждой паре дата-ID
    id_from_list = [dict(item)[key_value] for item in list_to_transform]
    dict_from_list = {date_from_list: id_from_list}

    # В данных могут быть и, действительно, есть дубликаты.
    # Сейчас окей, потом, может, пригодится.
    # print(f'{date_from_list}: total {len(id_from_list)} IDs, unique {len(set(id_from_list))} IDs')

    return dict_from_list


def get_out_of_stock_data(configuration):
    """Function get config/API/payload data and saves it to hdfs bronze directory partitioned by date"""

    token = get_homework_token(configuration)
    token = str(configuration['API']['auth']).replace('<jwt_token>', token['access_token'])
    url = configuration['API']['url'] + configuration['API']['endpoint']
    data = configuration['API']['payload']
    headers = {'content-type': 'application/json', 'Authorization': token}

    client = InsecureClient(f'http://127.0.0.1:50070/', user='user')

    for d in data:
        try:
            temp = requests.get(url, data=json.dumps(d), headers=headers, timeout=10)
            temp.raise_for_status()
            temp = temp.json()

            folder_name = f'/bronze/out_of_stock_api/{d["date"]}'
            client.makedirs(folder_name)

            with client.write(f'{folder_name}/{d["date"]}.json', ) as json_file:
                json.dump(temp, json_file)

            logging.info(f'Out of stock data for {d["date"]} uploaded successfully')

        except requests.HTTPError:
            logging.error(f'No data received for {d}')
            continue
    return


def get_data_from_api():

    connection = BaseHook.get_connection('robot_dreams_api_2')

    config = {
        'AUTH': {
            'endpoint': '/auth',
            'payload': {"username": connection.login, "password": connection.password},
            'output type': 'JWT TOKEN'},
        'API': {
            'url': connection.host,
            'endpoint': '/out_of_stock',
            'payload': [{"date": datetime.today().strftime('%Y-%m-%d')}],
            'auth': 'JWT <jwt_token>'}
    }
    if len(config) > 0:
        get_out_of_stock_data(config)


def api_data_to_silver():
    """Function to get table from bronze stage do dummy transformation and save it to silver stage"""

    try:
        spark = SparkSession.builder \
            .config('spark.driver.extraClassPath', '/home/user/shared_folder/postgresql-42.2.20.jar') \
            .master('local') \
            .appName('project') \
            .getOrCreate()

        today = datetime.today().strftime("%Y-%m-%d")

        df = spark.read.load(f'/bronze/out_of_stock_api/{today}/{today}.json'
                             , header="true"
                             , inferSchema="true"
                             , format="json")

        df = df.dropDuplicates()
        logging.info(f'Out of stock data for {today} cleared from duplicates')

        df.write.parquet(f'/silver/out_of_stock_api/{today}/{today}', mode='overwrite')
        logging.info(f'{today}.parquet successfully saved to SILVER')

    except:  # AnalysisException Path does not exist:
        logging.error(f'No out of stock data for {today}')


if __name__ == '__main__':

    config = load_config('config.yaml')
    if len(config) > 0:
        get_out_of_stock_data(config)
