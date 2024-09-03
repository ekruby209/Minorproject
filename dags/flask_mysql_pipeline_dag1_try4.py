from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import os

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 3),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'flask_api_integration_dag_try4',
    default_args=default_args,
    description='DAG to run preprocess and push data to MySQL via Flask API',
    schedule_interval=timedelta(days=1),
)

def load_config():
    config_path = '/home/ekruby/airflow/config/config.json'  #hardcoded
    if os.path.exists(config_path):
        with open(config_path, 'r') as file:
            return json.load(file)
    else:
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

def call_preprocess():
    config = load_config() 
    file_path = config.get('file_path')
    if not file_path:
        raise ValueError("File path not provided in DAG run configuration or config file.")
    
    url = config.get('preprocess_url')
    if not url:
        raise ValueError("Preprocess URL not provided in config file.")
    response = requests.post(url, json={'file_path': file_path})
    if response.status_code == 200:
        return "Preprocessing is Successful"
    else:
        response_data = response.json()
        raise Exception(f"Preprocessing failed: {response_data.get('error')}")

def call_mysql_push():
    config = load_config()
    #output_path = config.get('output_path')
    #if not output_path:
        #raise ValueError("Output path not provided in the config file.")
    
    url = config.get('mysql_push_url')
    if not url:
        raise ValueError("MySQL push URL not provided in config file.")
    
    response = requests.post(url)
    if response.status_code == 200:
        return "MySQL Push Successful"
    else:
        response_data = response.json()
        raise Exception(f"MySQL push failed: {response_data.get('error')}")

preprocess_task = PythonOperator(
    task_id='preprocess_task',
    python_callable=call_preprocess,
    dag=dag,
)

mysql_push_task = PythonOperator(
    task_id='mysql_push_task',
    python_callable=call_mysql_push,
    dag=dag,
)

preprocess_task >> mysql_push_task
