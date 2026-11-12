#!/usr/bin/env bash
source "$(dirname "$0")/../_lib.sh"; LABDIR=labs/08-idempotent
[ -f $LABDIR/dag.py ] || { fail "run ./break.sh first"; exit 1; }
rm -rf $LABDIR/out
for d in 2026-10-01 2026-10-01 2026-10-02 2026-10-01; do af airflow dags test lab08_load $d >/dev/null 2>&1; done
n=$(cat $LABDIR/out/* 2>/dev/null | wc -l | tr -d ' ')
u=$(cat $LABDIR/out/* 2>/dev/null | sort -u | wc -l | tr -d ' ')
echo "ran day1 x3 and day2 x1 -> lines=$n distinct lines=$u"
[ "$n" -eq 10 ] && pass "exactly 10 lines (5 tenants x 2 days)" || fail "$n lines, expected 10: re-runs changed the result"
[ "$n" = "$u" ] && pass "no duplicate lines" || fail "duplicate lines present"
finish "LAB 08"
