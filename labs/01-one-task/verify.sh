#!/usr/bin/env bash
source "$(dirname "$0")/../_lib.sh"
[ -f labs/01-one-task/job.py ] || { fail "run ./break.sh first"; exit 1; }
out=$(run_job labs/01-one-task/job.py)
[ "$(echo "$out" | grep -c '^ROW')" -eq 5 ] && pass "result still has 5 tenants" || fail "result is wrong or the job crashed"
echo "$out" | grep UI_STATS
tasks=$(echo "$out" | grep UI_STATS | sed 's/.*stage_tasks=\[\([^]]*\)\].*/\1/' | tr -d ' ')
echo ",$tasks," | grep -q ",8," && pass "a stage ran with exactly 8 tasks" || fail "no stage with 8 tasks (stage_tasks=[$tasks])"
finish "LAB 01"
