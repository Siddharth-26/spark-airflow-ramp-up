#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/../_lib.sh"
ensure_data
cat > labs/02-scan-once/job.py <<'PY'
from pyspark.sql import SparkSession, functions as F
from ui_stats import report

spark = SparkSession.builder.appName("lab02").master("local[2]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

schema = "amount double, currency string, event_id string, event_ts string, event_type string, order_id string, tenant_id string"
big = spark.read.schema(schema).json("data/raw/orders/*/events.jsonl").filter(F.col("amount") > 10)

total = big.count()
big.filter(F.col("event_type") == "paid").explain()   # look for InMemoryTableScan vs FileScan
paid = big.filter(F.col("event_type") == "paid").count()
usd = big.filter(F.col("currency") == "USD").count()
print("RESULT", total, paid, usd)
report(spark)
PY
echo "Created labs/02-scan-once/job.py"
