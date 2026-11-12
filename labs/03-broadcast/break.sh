#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/../_lib.sh"
ensure_data
cat > labs/03-broadcast/job.py <<'PY'
from pyspark.sql import SparkSession, functions as F
from ui_stats import report

spark = (SparkSession.builder.appName("lab03").master("local[2]")
         .config("spark.sql.autoBroadcastJoinThreshold", "-1")   # a teammate "optimised" this once
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

events = spark.read.json("data/raw/orders/*/events.jsonl")
tenants = spark.createDataFrame(
    [("acme", "Acme Corp", "enterprise"), ("globex", "Globex", "smb"), ("initech", "Initech", "smb"),
     ("umbrella", "Umbrella", "enterprise"), ("hooli", "Hooli", "enterprise")],
    "tenant_id string, name string, tier string")

joined = events.join(tenants, "tenant_id")
joined.explain()
print("TIERS", sorted((r["tier"], r["count"]) for r in joined.groupBy("tier").count().collect()))
report(spark)
PY
echo "Created labs/03-broadcast/job.py"
