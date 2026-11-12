#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/../_lib.sh"
ensure_data
cat > labs/01-one-task/job.py <<'PY'
from pyspark.sql import SparkSession, functions as F
from ui_stats import report

spark = SparkSession.builder.appName("lab01").master("local[2]").config("spark.sql.shuffle.partitions", "8").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.json("data/raw/orders/*/events.jsonl")
rows = df.groupBy("tenant_id").count().orderBy("tenant_id").collect()
for r in rows:
    print("ROW", r["tenant_id"], r["count"])
report(spark)
PY
echo "Created labs/01-one-task/job.py. Run: PYTHONPATH=app python labs/01-one-task/job.py"
