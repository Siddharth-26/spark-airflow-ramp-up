import time
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# Someone needed the list of active tenants, so they "just asked the tenant service" here.
time.sleep(3)                       # stands in for an HTTP call / DB query
TENANTS = ["acme", "globex", "initech", "umbrella", "hooli"]


def process(tenant):
    print("processing", tenant)


with DAG("lab07_fanout", start_date=datetime(2026, 10, 1), schedule="@daily", catchup=False) as dag:
    for t in TENANTS:
        PythonOperator(task_id=f"process_{t}", python_callable=process, op_args=[t])
