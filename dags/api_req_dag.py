import os
import requests
from datetime import datetime, timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

import json
import psycopg2


default_args = {
    "owner": "el",
    "start_date": days_ago(1),
    "catchup": False,
    "max_active_runs": 1,
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}


PG_HOST = 'postgresql'
PG_USER = os.environ.get('AIRFLOW_DATABASE_USERNAME')
PG_PASSWORD = os.environ.get('AIRFLOW_DATABASE_PASSWORD')
PG_PORT = '5432'
PG_DATABASE = os.environ.get('AIRFLOW_DATABASE_NAME')
TABLE_NAME_TEMPLATE = 'data.raw'


def _get_url(url):
    r = requests.get(url)

    if r.status_code == 200:
        return r
    elif r.status_code in [400, 401, 403]:
        error_code = r.json()["error"]["code"]
        error_message = r.json()["error"]["message"]
        raise Exception(f"{error_message} - Error code: {error_code}")
    else:
        raise Exception("Can't connect to the API")



def _req(ti, key, name):
    url_base = 'http://api.weatherapi.com/v1/current.json?'
    url_params = f'key={key}&q={name}&api=yes'

    ts_request = datetime.now().timestamp()
    r = _get_url(url_base + url_params).json()
    ts_location = r['location']['localtime_epoch']


    ti.xcom_push(key='ts_request', value=ts_request)
    ti.xcom_push(key='ts_location', value=ts_location)
    ti.xcom_push(key='raw_data', value=r)


def _populate_raw(user, password, host, port, db, table_name, ti):

    ts_location = ti.xcom_pull(key='ts_location', task_ids=['WeatherApi_request'])[0]
    ts_request = ti.xcom_pull(key='ts_request', task_ids=['WeatherApi_request'])[0]
    raw_data = ti.xcom_pull(key='raw_data', task_ids=['WeatherApi_request'])[0]

    conn = psycopg2.connect(host=host,
                            port=port,
                            user=user,
                            password=password,
                            database=db)

    query = f"INSERT INTO {table_name} (ts_request, ts_location, raw_data) VALUES(%s, %s, %s)"

    with conn.cursor() as curs:
        curs.execute(query, (datetime.fromtimestamp(ts_request), datetime.fromtimestamp(ts_location), json.dumps(raw_data)))
        conn.commit()
    conn.close()



with DAG(dag_id='WeatherApi_get_data',
         default_args=default_args,
         schedule_interval='*/1 * * * *',
         catchup=False
         ) as dag:


    api_get_data = PythonOperator(
            task_id='WeatherApi_request',
            python_callable=_req,
            op_kwargs={
                'key': '{{ var.value.API_KEY }}',
                'name': 'Sochi'  #Loma,Montana
            }
        )

    populate_raw_table = PythonOperator(
        task_id='populate_raw_table',
        python_callable=_populate_raw,
        op_kwargs={
            'user': PG_USER,
            'password': PG_PASSWORD,
            'host': PG_HOST,
            'port': PG_PORT,
            'db': PG_DATABASE,
            'table_name': TABLE_NAME_TEMPLATE

        },
    )

    api_get_data >> populate_raw_table