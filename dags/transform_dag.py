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
TABLE_NAME_TEMPLATE = '"data".raw'
TABLE_NAME_TEMPLATE2 = '"data".temp_change'


def _get_last_data(user, password, host, port, db, table_name, ti):
    conn = psycopg2.connect(host=host,
                            port=port,
                            user=user,
                            password=password,
                            database=db)

    query = f'select "date" from data.temp_change tc order by 1 desc limit 1;'

    with conn.cursor() as cur:
        cur.execute(query)
        last_date = cur.fetchone()
        if last_date:
            time_clause = f" WHERE  ts_request >= timestamp '{last_date[0]}' - (30 || 'minutes')::interval"
        else:
            time_clause = ''

        print('last_date ', last_date)
        query2 = f"""with interval_data as (
                            select 
                                to_timestamp(floor((extract('epoch' from ts_request) / 600 )) * 600) AT TIME ZONE 'UTC' as "date", 
                                avg((raw_data->'current'->>'temp_c')::float) as mean_temp 
                            FROM {table_name} {time_clause}
                            GROUP BY "date" 
                            ),
                            change_data as (
                            select "date",
                                mean_temp,
                                COALESCE(mean_temp - lag(mean_temp) over (order by "date"),0) temp_fluct
                            from interval_data 
                            )
                            select 
                            "date",
                            case  
                                when temp_fluct > 0 then 'up'
                                when temp_fluct < 0 then 'down'
                                else 'same' 
                            end as state,
                            mean_temp,
                            temp_fluct
                            from change_data
                            order by "date" desc 
                            limit 3;"""
        print(query2)
        cur.execute(query2)
        res = cur.fetchall()

        conn.commit()

    conn.close()

    serial_res = [(v[0].timestamp(), v[1], v[2], v[3]) for v in res]
    print('serial_res ', serial_res)
    ti.xcom_push(key='new_data', value=serial_res)

def _populate_result(user, password, host, port, db, table_name, ti):

    new_data = ti.xcom_pull(key='new_data', task_ids=['get_last_data'])[0]
    print('new_data ', new_data)
    if new_data:
        conn = psycopg2.connect(host=host,
                                port=port,
                                user=user,
                                password=password,
                                database=db)
        insert_sql = f'''
                    INSERT INTO {table_name} ("date", state, mean_temp, temp_fluct)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT ("date") DO UPDATE SET
                    (state, mean_temp, temp_fluct) = (EXCLUDED.state, EXCLUDED.mean_temp, EXCLUDED.temp_fluct);
                '''
        for col in new_data:
            with conn.cursor() as cur:
                print(col[0], col[1], col[2], col[3])
                print('col[0], col[1], col[2], col[3] ', datetime.fromtimestamp(col[0]), col[1], col[2], col[3])
                cur.execute(insert_sql, (datetime.fromtimestamp(col[0]), col[1], col[2], col[3]))
            conn.commit()
        conn.close()



with DAG(dag_id='WeatherApi_transform_data',
         default_args=default_args,
         schedule_interval='*/10 * * * *',
         catchup=False
         ) as dag:


    get_last_data = PythonOperator(
        task_id='get_last_data',
        python_callable=_get_last_data,
        op_kwargs={
            'user': PG_USER,
            'password': PG_PASSWORD,
            'host': PG_HOST,
            'port': PG_PORT,
            'db': PG_DATABASE,
            'table_name': TABLE_NAME_TEMPLATE
        }
    )


    populate_result_table = PythonOperator(
        task_id='populate_result_table',
        python_callable=_populate_result,
        op_kwargs={
            'user': PG_USER,
            'password': PG_PASSWORD,
            'host': PG_HOST,
            'port': PG_PORT,
            'db': PG_DATABASE,
            'table_name': TABLE_NAME_TEMPLATE2

        }
    )

    get_last_data >> populate_result_table

