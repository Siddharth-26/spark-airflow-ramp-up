#!/usr/bin/env bash
source "$(dirname "$0")/../_lib.sh"
[ -f labs/02-scan-once/job.py ] || { fail "run ./break.sh first"; exit 1; }
out=$(run_job labs/02-scan-once/job.py)
echo "$out" | grep -E 'RESULT|UI_STATS|InMemoryTableScan|FileScan' | cut -c1-110
echo "$out" | grep -q '^RESULT [0-9]* [0-9]* [0-9]*$' && pass "three counts printed" || fail "job crashed or RESULT line changed"
echo "$out" | grep -q InMemoryTableScan && pass "later counts read the cache (InMemoryTableScan in plan)" || fail "no InMemoryTableScan: every action still re-reads the files"
finish "LAB 02"
