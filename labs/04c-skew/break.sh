#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/../_lib.sh"
[ -d data/skew ] || python app/generate_orders.py --skew --out data/skew --days 3 --seed 9
cat > labs/04c-skew/job.py <<'PY'
from pyspark.sql import SparkSession, functions as F
from ui_stats import max_task_share

spark = (SparkSession.builder.appName("lab04c").master("local[2]")
         .config("spark.sql.adaptive.enabled", "false")
         .config("spark.sql.autoBroadcastJoinThreshold", "-1")   # pretend the dimension table is huge
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

events = spark.read.json("data/skew/*/events.jsonl")
dim = spark.createDataFrame(
    [(t, i) for i, t in enumerate(["acme", "globex", "initech", "umbrella", "hooli", "megacorp"])],
    "tenant_id string, tier int")

joined = events.join(dim, "tenant_id")        # you may NOT use broadcast() here: the dim is "huge"
print("COUNT", joined.count())
joined.explain()
max_task_share(spark)
PY
echo "Created labs/04c-skew/job.py"
