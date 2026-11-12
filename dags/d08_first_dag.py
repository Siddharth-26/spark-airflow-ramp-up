"""Day 8: your first DAG. Three tasks: check input -> run the Spark pipeline -> validate."""
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

ROOT = os.environ.get("OBJECTS_ROOT", os.getcwd())
PY = os.environ.get("SPARK_PYTHON", "python")


def validate(ds, **_):
    import glob
    files = glob.glob(f"{ROOT}/data/curated/daily_agg/dt={ds}/*.parquet")
    if not files:
        raise ValueError(f"no aggregate output for {ds}")
    print(f"validated {len(files)} aggregate file(s) for {ds}")


with DAG(
    dag_id="d08_orderflow_daily",
    start_date=datetime(2026, 10, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 1},
    tags=["ramp-up"],
) as dag:
    check_input = BashOperator(
        task_id="check_input",
        bash_command=f"test -f {ROOT}/data/raw/orders/dt={{{{ ds }}}}/events.jsonl && echo input present for {{{{ ds }}}}",
    )
    run_spark = BashOperator(
        task_id="run_spark",
        bash_command=f"cd {ROOT} && {PY} app/pipeline.py --ds {{{{ ds }}}}",
    )
    validate_out = PythonOperator(task_id="validate", python_callable=validate)

    check_input >> run_spark >> validate_out
