from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils import timezone
from airflow.sensors.http_sensor import HttpSensor
import logging

default_args = {
    'owner': 'airflow',
}

with DAG (
    'read_data_dag',
    schedule_interval='* 1 * * *',
    default_args=default_args,
    start_date=timezone.datetime(2025, 6, 19),
    tags = ['ETL','api'],
    catchup=False,
) as dag :
    api_sensor = HttpSensor(
        task_id='is_api_available',
        http_conn_id='user_api',
        endpoint='api/',
        dag=dag,
    )

api_sensor