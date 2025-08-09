from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="test_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@once",
    catchup=False
) as dag:

    hello = BashOperator(
        task_id="say_hello",
        bash_command="echo 'Hello from Airflow!'"
    )
