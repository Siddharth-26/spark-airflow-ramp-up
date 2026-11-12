#!/usr/bin/env bash
source "$(dirname "$0")/../_lib.sh"
[ -f labs/04c-skew/job.py ] || { fail "run ./break.sh first"; exit 1; }
out=$(PYTHONPATH=app python labs/04c-skew/job.py 2>/dev/null)
echo "$out" | grep -E '^COUNT|^SKEW'
exp=$(cat data/skew/*/events.jsonl | wc -l | tr -d ' ')
echo "$out" | grep -q "^COUNT $exp\$" && pass "row count correct ($exp)" || fail "row count wrong (expected $exp)"
echo "$out" | grep -q BroadcastHashJoin && fail "no broadcast allowed in this lab" || pass "no broadcast used"
share=$(echo "$out" | grep '^SKEW' | sed 's/.*=//')
python3 -c "import sys; sys.exit(0 if float('${share:-1}')<0.35 else 1)" && pass "heaviest task handles ${share} of the rows" || fail "one task still handles ${share:-?} of the rows (need < 0.35)"
finish "LAB 04c"
