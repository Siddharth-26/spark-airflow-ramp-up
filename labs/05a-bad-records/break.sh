#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/../_lib.sh"
[ -d data/messy ] || python app/generate_orders.py --skew --dupes --bad --late --out data/messy --days 3 --seed 7
cat > labs/05a-bad-records/job.py <<'PY'
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("lab05a").master("local[2]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.json("data/messy/*/events.jsonl")
clean = df.filter(F.col("amount").isNotNull() & F.col("tenant_id").isNotNull())

print("GOOD", clean.count())
# TODO (your task): also print "BAD <n>" and write the bad lines somewhere you can inspect them.
PY
echo "Created labs/05a-bad-records/job.py"
