import pendulum

from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

def expensive_api_call():
    return "Hello from Airflow!"

args = {
    "retry": 2,
    "retry_delay": timedelta(minutes=10)
}

with DAG(
    dag_id="yet_another_dag",
    schedule=None,
    start_date=datetime(2024, 5, 1),
    catchup=False,
    tags=["yat"],
) as dag:
    primeira_task = PythonOperator(
        task_id="task_id_1",
        python_callable=expensive_api_call
    )