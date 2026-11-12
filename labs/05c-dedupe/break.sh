#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/../_lib.sh"
[ -d data/messy ] || python app/generate_orders.py --skew --dupes --bad --late --out data/messy --days 3 --seed 7
cat > labs/05c-dedupe/job.py <<'PY'
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("lab05c").master("local[2]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

schema = "amount double, currency string, event_id string, event_ts string, event_type string, order_id string, tenant_id string"
df = (spark.read.schema(schema).option("mode", "DROPMALFORMED").json("data/messy/*/events.jsonl")
        .filter(F.col("amount").isNotNull() & F.col("tenant_id").isNotNull()))

# audit column so we can trace each row later
df = df.withColumn("row_uuid", F.expr("uuid()"))

deduped = df.distinct()          # "dedupe"
print("UNIQUE", deduped.count())
PY
echo "Created labs/05c-dedupe/job.py"
