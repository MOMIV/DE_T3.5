from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup

from airflow.hooks.postgres_hook import PostgresHook

import json
import requests

#***** Базовые константы ************

# соединение с базой person
conn_id = Variable.get("conn_name")


# Url Fake data
URL_API= Variable.get("URL_API")

params = {
  "access_key": Variable.get("access_key"),
  "currencies": "RUB",
  "format": "1",
  "source": "BTC"
}


def _extract_data():
    """Извлекаем данные из API"""

    try:
        response = requests.get(URL_API, params)
        data = response.json()

        with open(f"/tmp/extract_data.sql", "w+") as f:
             
            date = datetime.fromtimestamp (data["timestamp"])
            source = data["source"] 
            currencies =str(params["currencies"])
            rate = data["quotes"]["BTCRUB"]
            

            query = f"""

                INSERT INTO rate (date,source, currencies,rate) 
                VALUES ('{date}','{source}','{currencies}',{rate});

            """

            f.write(query)

    except Exception as error:
        raise Exception(f'Не удалось получить данные с API: {error}!')




# аргументы дага по умолчанию
default_args = {
    "owner": "inna",
    "retries": 5,
    "retry_delay": 5,
    "start_date": datetime(2023, 11, 4),
}

with DAG(dag_id="get_rate", 
         default_args=default_args, 
         schedule_interval=timedelta(minutes=10), 
         description= "Получениe курса BTC", 
         template_searchpath = "/tmp", 
         catchup=False) as dag:

    start = EmptyOperator(task_id='start') 
    end = EmptyOperator(task_id='end')

    create_rate_table = PostgresOperator(
        task_id="create_rate_table",
        postgres_conn_id="conn_db",
        sql="""CREATE TABLE IF NOT EXISTS rate (
            id SERIAL PRIMARY KEY, 
            date timestamp, 
            source text, 
            currencies text, 
            rate real);
            """,
    )
    
    extract_data = PythonOperator(
        task_id='exctract_data',
        python_callable=_extract_data
    )

    # выполнение sql-запросов на загрузку отзывов
    load_data = PostgresOperator(
        task_id=f"load_data",
        postgres_conn_id=conn_id,
        sql=f"extract_data.sql"
    )


    start >> create_rate_table >> extract_data >> load_data >> end

