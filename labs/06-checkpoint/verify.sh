#!/usr/bin/env bash
source "$(dirname "$0")/../_lib.sh"
[ -f labs/06-checkpoint/job.py ] || { fail "run ./break.sh first"; exit 1; }
rm -rf labs/06-checkpoint/out labs/06-checkpoint/ckpt
run_job labs/06-checkpoint/job.py >/dev/null      # run 1
run_job labs/06-checkpoint/job.py >/dev/null      # run 2: nothing new, must add nothing
exp=$(cat data/stream_in/*.jsonl | wc -l | tr -d ' ')
got=$(python - <<'PY' 2>/dev/null
from pyspark.sql import SparkSession
s=SparkSession.builder.master("local[1]").getOrCreate(); s.sparkContext.setLogLevel("ERROR")
print(s.read.parquet("labs/06-checkpoint/out").count())
PY
)
echo "input lines=$exp  rows in output after 2 runs=$got"
[ "$got" = "$exp" ] && pass "rerun did not reprocess old files" || fail "output has ${got:-nothing} rows, expected $exp (reprocessed on rerun?)"
[ -d labs/06-checkpoint/ckpt ] && pass "checkpoint directory exists" || fail "no checkpoint at labs/06-checkpoint/ckpt"
finish "LAB 06"
