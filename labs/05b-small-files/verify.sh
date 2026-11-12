#!/usr/bin/env bash
source "$(dirname "$0")/../_lib.sh"
[ -f labs/05b-small-files/job.py ] || { fail "run ./break.sh first"; exit 1; }
rm -rf labs/05b-small-files/out
out=$(run_job labs/05b-small-files/job.py)
n=$(find labs/05b-small-files/out -name '*.parquet' | wc -l | tr -d ' ')
exp=$(wc -l < data/raw/orders/dt=2026-10-01/events.jsonl | tr -d ' ')
echo "files=$n"; echo "$out" | grep ROWS_BACK
echo "$out" | grep -q "^ROWS_BACK $exp\$" && pass "all $exp rows survived" || fail "row count wrong"
[ "$n" -le 10 ] && pass "$n parquet files" || fail "$n parquet files (limit 10)"
finish "LAB 05b"
