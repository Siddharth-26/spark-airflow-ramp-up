#!/usr/bin/env bash
source "$(dirname "$0")/../_lib.sh"; LABDIR=labs/10-silent-failure
[ -f $LABDIR/dag.py ] || { fail "run ./break.sh first"; exit 1; }
rm -rf data/lab10
af airflow dags test lab10_pipeline 2026-10-01 >/dev/null 2>&1; good=$?
af airflow dags test lab10_pipeline 2027-01-01 >/dev/null 2>&1; missing=$?
[ $good -eq 0 ] && [ -d data/lab10/daily_agg/dt=2026-10-01 ] && pass "day with data: DAG succeeds and output exists" || fail "day with data should succeed and write output"
[ $missing -ne 0 ] && pass "day with NO data: DAG run fails (as it must)" || fail "day with no input still shows SUCCESS: the failure was swallowed"
finish "LAB 10"
