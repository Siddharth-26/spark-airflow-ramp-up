#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/../_lib.sh"
ensure_data
rm -rf data/stream_in labs/06-checkpoint/out labs/06-checkpoint/ckpt
mkdir -p data/stream_in
for d in 2026-10-01 2026-10-02 2026-10-03; do cp data/raw/orders/dt=$d/events.jsonl data/stream_in/$d.jsonl; done
cat > labs/06-checkpoint/job.py <<'PY'
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("lab06").master("local[2]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

schema = "amount double, currency string, event_id string, event_ts string, event_type string, order_id string, tenant_id string"
stream = spark.readStream.schema(schema).json("data/stream_in")

def write_batch(batch_df, batch_id):
    batch_df.write.mode("append").parquet("labs/06-checkpoint/out")

q = (stream.writeStream
        .foreachBatch(write_batch)
        .trigger(availableNow=True)          # process what's there, then stop (good for a cron'd job)
        .start())
q.awaitTermination()
PY
echo "Created labs/06-checkpoint/job.py and 3 input files in data/stream_in"
