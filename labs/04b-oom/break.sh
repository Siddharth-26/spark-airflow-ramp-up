#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/../_lib.sh"
ensure_data
cat > labs/04b-oom/job.py <<'PY'
from pyspark.sql import SparkSession, functions as F

# Do NOT change the memory setting: this machine-sized limit stands in for a real executor.
spark = SparkSession.builder.appName("lab04b").master("local[2]").config("spark.driver.memory", "480m").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.json("data/raw/orders/*/events.jsonl").withColumn("payload", F.repeat("event_id", 100))

# Goal: for each tenant, how many events and how many payload characters in total.
per_tenant = (df.groupBy("tenant_id")
                .agg(F.collect_list("payload").alias("payloads"))
                .select("tenant_id",
                        F.size("payloads").alias("events"),
                        F.expr("aggregate(payloads, 0L, (acc, x) -> acc + length(x))").alias("chars")))
for r in per_tenant.orderBy("tenant_id").collect():
    print("ROW", r["tenant_id"], r["events"], r["chars"])
PY
echo "Created labs/04b-oom/job.py"
