
from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.python import PythonOperator
import csv
from airflow.datasets import Dataset


def expensive_api_call():
    return "Hello from Airflow!"

args = {
    "retry": 2,
    "retry_delay": timedelta(minutes=10)
}

connection = {
    "conection_type": "Postgres",
    "host": "postgres",
    "DB": "loja_carros",
    "login": "airflow",
    "senha": "airflow",
    "Porta": "5432",
}

def save_to_csv(ti):

    postgres_result = ti.xcom_pull(task_ids='execute_postgres_query')

    csv_file_path = 'arquivo.csv'

    with open(csv_file_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=',',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for i in postgres_result:
            writer.writerow(i)

def read_csv():
    csv_file_path = 'arquivo.csv'

    with open(csv_file_path, newline='') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=' ', quotechar='|')
        for row in spamreader:
            print("Vendedor "+row[0]+" faturou o valor "+row[1]+" e ficou em "+row[2]+" lugar")

with DAG(
    dag_id="pstgres_dag_id",
    schedule=None,
    start_date=datetime(2024, 5, 1),
    catchup=False,
    tags=["yat"],
) as dag:
    select_pet_table = PostgresOperator(
        task_id="select_table",
        sql="""
            SELECT v.id, c.marca, c.modelo, cl.nome AS cliente_nome, f.nome AS funcionario_nome, v.data_da_venda, v.preco_de_venda
            FROM vendas v
            JOIN carros c ON v.carro_id = c.id
            JOIN clientes cl ON v.cliente_id = cl.id
            JOIN funcionarios f ON v.funcionario_id = f.id;
          """,
        postgres_conn_id="psql_id"
    )

    save_to_csv_task = PythonOperator(
        task_id='save_to_csv',
        python_callable=save_to_csv,
        provide_context=True,  
        dag=dag,
        outlets=[Dataset("s3://dataset-bucket/example.csv")]
    )
    read_csv_task = PythonOperator(
        task_id='read_csv',
        python_callable=read_csv,
        provide_context=True,  
        dag=dag
    )

    select_pet_table >> save_to_csv_task 