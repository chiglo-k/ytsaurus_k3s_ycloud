from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def hello_task() -> None:
    print("Airflow test DAG is working")


with DAG(
    dag_id="test_hello_world",
    start_date=datetime(2026, 4, 1),
    schedule="@daily",
    catchup=False,
    tags=["test"],
) as dag:
    hello = PythonOperator(
        task_id="hello_task",
        python_callable=hello_task,
    )