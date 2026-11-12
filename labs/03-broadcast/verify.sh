#!/usr/bin/env bash
source "$(dirname "$0")/../_lib.sh"
[ -f labs/03-broadcast/job.py ] || { fail "run ./break.sh first"; exit 1; }
out=$(PYTHONPATH=app python labs/03-broadcast/job.py 2>/dev/null)
echo "$out" | grep -q "^TIERS \[('enterprise'" && pass "result is correct" || fail "wrong result or crash"
echo "$out" | grep -q BroadcastHashJoin && pass "plan uses BroadcastHashJoin" || fail "plan does not use BroadcastHashJoin"
echo "$out" | grep -q SortMergeJoin && fail "plan still contains SortMergeJoin" || pass "no SortMergeJoin left"
finish "LAB 03"
