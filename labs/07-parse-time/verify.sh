#!/usr/bin/env bash
source "$(dirname "$0")/../_lib.sh"; LABDIR=labs/07-parse-time
[ -f $LABDIR/dag.py ] || { fail "run ./break.sh first"; exit 1; }
res=$(af python labs/_measure_dagbag.py labs/07-parse-time lab07_fanout 2>/dev/null | sed -n "s/.*RESULT //p")
echo "$res"
echo "$res" | grep -q "TASKS=5 ERRORS=0" && pass "DAG imports with 5 tasks" || fail "DAG broken or task count changed"
secs=$(echo "$res" | sed 's/PARSE_SECONDS=\([0-9.]*\).*/\1/')
python3 -c "import sys; sys.exit(0 if float('${secs:-99}')<1.5 else 1)" && pass "parses in ${secs}s" || fail "parse takes ${secs:-?}s (limit 1.5s): the scheduler pays this every ~30s, forever"
finish "LAB 07"
