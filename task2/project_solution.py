from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="end_to_end_batch_datalake",
    default_args=default_args,
    description="End-to-End Batch Data Lake",
    schedule="* * * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    print(dag)
    landing_to_bronze = BashOperator(
        task_id="landing_to_bronze",
        bash_command="python /Users/margaritamikaelan/Documents/goit/goit-de-fp/task2/landing_to_bronze.py",
    )

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command="python /Users/margaritamikaelan/Documents/goit/goit-de-fp/task2/bronze_to_silver.py",
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command="python /Users/margaritamikaelan/Documents/goit/goit-de-fp/task2/silver_to_gold.py",
    )

    landing_to_bronze >> bronze_to_silver >> silver_to_gold
