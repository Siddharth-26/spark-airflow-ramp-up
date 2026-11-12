import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

ROOT = os.environ["OBJECTS_ROOT"]
PY = os.environ["SPARK_PYTHON"]

with DAG("lab10_pipeline", start_date=datetime(2026, 10, 1), schedule="@daily", catchup=False) as dag:
    BashOperator(
        task_id="run_pipeline",
        bash_command=f"cd {ROOT} && {PY} app/pipeline.py --ds {{{{ ds }}}} --out data/lab10 ; echo pipeline step finished",
    )
