import pendulum

from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

def python_task(ti,ts):
    ti.xcom_push(key="ts_inicial",value=ts)

def python_task_2(ti,ds):
    ti.xcom_push(key="ds_inicial",value=ds)

def dummy_task():
    return "just waiting"

def bash_task():
    return "echo 1"

def pyhton_task_4(ti):
    ts = ti.xcom_pull(key='ts_inicial')
    ds = ti.xcom_pull(key='ds_inicial')
    print(ds+ " -- "+ ts)

with DAG(
    dag_id="yet_more_dag",
    schedule=None,
    start_date=datetime(2024, 5, 1),
    catchup=False,
    tags=["yat"],
) as dag:
    primeira_task = BashOperator(
        task_id="bash_task",
        bash_command='echo "Here is the message: \'{{ dag_run.conf["message"] if dag_run.conf else "" }}\'"',
    )
    segunda_task = PythonOperator(
        task_id="python_task_1",
        python_callable=python_task
    )
    terceira_task = PythonOperator(
        task_id="python_task_2",
        python_callable=python_task_2
    )
    quarta_task = PythonOperator(
        task_id="python_task_3",
        python_callable=dummy_task
    )
    quinta_task = PythonOperator(
        task_id="python_task_4",
        python_callable=pyhton_task_4
    )

    primeira_task >> segunda_task
    primeira_task >> terceira_task
    segunda_task >> quarta_task
    terceira_task >> quarta_task
    quarta_task>>quinta_task