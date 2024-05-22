from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime


def test_mysql_connection():
    mysql_hook = MySqlHook(mysql_conn_id='mysql_conn')
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    result = cursor.fetchone()
    if result:
        print("MySQL connection successful")
    else:
        print("MySQL connection failed")


def test_postgres_connection():
    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    result = cursor.fetchone()
    if result:
        print("PostgreSQL connection successful")
    else:
        print("PostgreSQL connection failed")


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'test_db_connections',
    default_args=default_args,
    schedule_interval='@once',
)

t1 = PythonOperator(
    task_id='test_mysql_connection',
    python_callable=test_mysql_connection,
    dag=dag,
)

t2 = PythonOperator(
    task_id='test_postgres_connection',
    python_callable=test_postgres_connection,
    dag=dag,
)

t1 >> t2
