#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/../_lib.sh"
ensure_data
rm -rf labs/08-idempotent/out
cat > labs/08-idempotent/dag.py <<'PY'
import collections
import json
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

ROOT = os.environ["OBJECTS_ROOT"]
OUT = f"{ROOT}/labs/08-idempotent/out"


def load_counts(ds, **_):
    """Count events per tenant for one day and record them in the daily totals file."""
    counts = collections.Counter()
    with open(f"{ROOT}/data/raw/orders/dt={ds}/events.jsonl") as f:
        for line in f:
            counts[json.loads(line)["tenant_id"]] += 1
    os.makedirs(OUT, exist_ok=True)
    with open(f"{OUT}/totals.csv", "a") as out:
        for tenant, n in sorted(counts.items()):
            out.write(f"{ds},{tenant},{n}\n")


with DAG("lab08_load", start_date=datetime(2026, 10, 1), schedule="@daily", catchup=False,
         default_args={"retries": 2}) as dag:
    PythonOperator(task_id="load_counts", python_callable=load_counts)
PY
echo "Created labs/08-idempotent/dag.py"
