import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

ROOT = os.environ["OBJECTS_ROOT"]
OUT = f"{ROOT}/labs/09-logical-date/out"


def process_day(**_):
    day = datetime.utcnow().strftime("%Y-%m-%d")        # "today"
    os.makedirs(OUT, exist_ok=True)
    with open(f"{OUT}/processed_{day}.txt", "w") as f:
        f.write(f"processed events for {day}\n")


with DAG("lab09_day", start_date=datetime(2026, 10, 1), schedule="@daily", catchup=True) as dag:
    PythonOperator(task_id="process_day", python_callable=process_day)
