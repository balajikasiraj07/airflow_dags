from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Define default arguments
default_args = {
    'owner': 'analyst',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'daily_dag',
    default_args=default_args,
    description='My first daily DAG',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# Task 1: Simple bash command
task1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)

# Task 2: Python function
def print_hello():
    print("Hello from Airflow!")
    return "Task completed successfully"

task2 = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag,
)

# Task 3: Another bash command
task3 = BashOperator(
    task_id='print_message',
    bash_command='echo "Daily DAG is working!"',
    dag=dag,
)

# Set task dependencies (task1 -> task2 -> task3)
task1 >> task2 >> task3

