#!/usr/bin/env bash
source "$(dirname "$0")/../_lib.sh"; LABDIR=labs/09-logical-date
[ -f $LABDIR/dag.py ] || { fail "run ./break.sh first"; exit 1; }
rm -rf $LABDIR/out
for d in 2026-10-01 2026-10-02 2026-10-03; do af airflow dags test lab09_day $d >/dev/null 2>&1; done
echo "files: $(ls $LABDIR/out 2>/dev/null | tr '\n' ' ')"
ok=1
for d in 2026-10-01 2026-10-02 2026-10-03; do [ -f $LABDIR/out/processed_$d.txt ] || ok=0; done
[ $ok -eq 1 ] && pass "each run processed ITS OWN day" || fail "runs did not process the day they were run for (backfill would process 'today' N times)"
finish "LAB 09"
