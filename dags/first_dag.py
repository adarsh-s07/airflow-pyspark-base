from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "TOREID",
    "retries": 5,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id = "first_dag",
    default_args=default_args,
    description= "First instance of the dag",
    start_date=datetime(2026, 1, 1, 2),
    schedule='@daily'
) as dag:
    task1 = BashOperator(
        task_id = 'first_task',
        bash_command="echo hellowwww!"
    )

    task2 = BashOperator(
        task_id = "second_task",
        bash_command="hi! i'm the second task"
    )

    task3 = BashOperator(
        task_id = "third_task",
        bash_command="hi! i'm the third task"
    )

    task1 >> task2
    task1 >> task3