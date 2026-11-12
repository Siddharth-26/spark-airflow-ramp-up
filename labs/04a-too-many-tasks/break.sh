#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/../_lib.sh"
ensure_data
cat > labs/04a-too-many-tasks/job.py <<'PY'
from pyspark.sql import SparkSession, functions as F
from ui_stats import report

spark = (SparkSession.builder.appName("lab04a").master("local[2]")
         .config("spark.sql.adaptive.enabled", "false")
         .config("spark.sql.shuffle.partitions", "2000")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.json("data/raw/orders/dt=2026-10-01/events.jsonl")
for r in df.groupBy("tenant_id").agg(F.sum("amount").alias("rev")).orderBy("tenant_id").collect():
    print("ROW", r["tenant_id"], round(r["rev"], 2))
report(spark)
PY
echo "Created labs/04a-too-many-tasks/job.py"
