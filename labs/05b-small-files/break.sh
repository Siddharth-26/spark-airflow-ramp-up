#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/../_lib.sh"
ensure_data
rm -rf labs/05b-small-files/out
cat > labs/05b-small-files/job.py <<'PY'
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("lab05b").master("local[2]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.json("data/raw/orders/dt=2026-10-01/events.jsonl")
(df.repartition(200)                                   # "for parallelism"
   .write.mode("overwrite").partitionBy("tenant_id")
   .parquet("labs/05b-small-files/out"))

print("ROWS_BACK", spark.read.parquet("labs/05b-small-files/out").count())
PY
echo "Created labs/05b-small-files/job.py"
