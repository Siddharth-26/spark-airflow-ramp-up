#!/usr/bin/env bash
source "$(dirname "$0")/../_lib.sh"
[ -f labs/04a-too-many-tasks/job.py ] || { fail "run ./break.sh first"; exit 1; }
start=$(date +%s)
out=$(run_job labs/04a-too-many-tasks/job.py)
echo "took $(( $(date +%s) - start ))s"
[ "$(echo "$out" | grep -c '^ROW')" -eq 5 ] && pass "5 tenant rows" || fail "wrong/missing result"
echo "$out" | grep UI_STATS
mx=$(echo "$out" | grep UI_STATS | sed 's/.*stage_tasks=\[\([^]]*\)\].*/\1/' | tr -d ' ' | tr ',' '\n' | sort -n | tail -1)
[ "${mx:-99999}" -le 50 ] && pass "biggest stage has $mx tasks" || fail "a stage still has ${mx:-?} tasks (limit 50)"
finish "LAB 04a"
