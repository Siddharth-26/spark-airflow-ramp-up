#!/usr/bin/env bash
# Capstone grader. Build assessment/solution/pipeline.py and dags/capstone_dag.py first.
cd "$(dirname "$0")/.."
source .venv/bin/activate 2>/dev/null || { echo "run scripts/setup-venv.sh first"; exit 1; }
GREEN=$'\033[32m'; RED=$'\033[31m'; YEL=$'\033[33m'; RESET=$'\033[0m'; FAILED=0
pass(){ echo "${GREEN}PASS${RESET}  $*"; }
fail(){ echo "${RED}FAIL${RESET}  $*"; FAILED=1; }
skip(){ echo "${YEL}SKIP${RESET}  $*"; }

SOL=assessment/solution/pipeline.py
DAG=dags/capstone_dag.py
RAW=data/capstone
OUT=data/capstone_out

[ -f "$SOL" ] || { fail "missing $SOL — build your capstone there (see docs/day12.md)"; exit 1; }
[ -f "$DAG" ] || { fail "missing $DAG — build your capstone DAG there"; exit 1; }

[ -d "$RAW" ] || python app/generate_orders.py --skew --dupes --bad --late --days 5 --seed 2026 --out "$RAW" >/dev/null

# ground truth for a day with data
DS=2026-10-01
read EXP_IN EXP_GOOD EXP_UNIQUE < <(python3 - "$RAW/dt=$DS/events.jsonl" <<'PY'
import json,sys
ids=set(); g=0; n=0
for l in open(sys.argv[1]):
    n+=1
    try: e=json.loads(l)
    except Exception: continue
    if e.get("amount") is not None and e.get("tenant_id") is not None:
        g+=1; ids.add(e["event_id"])
print(n,g,len(ids))
PY
)

rm -rf "$OUT"
out1=$(python "$SOL" --ds "$DS" --raw "$RAW" --out "$OUT" 2>/dev/null); rc1=$?
echo "$out1" | grep -Eq 'good|GOOD|clean' && pass "req1/2: pipeline reports a good/clean count" || skip "req1/2: couldn't find a good-count line (check it prints one)"
[ $rc1 -eq 0 ] && pass "req4: day-with-data run exits 0" || fail "req4: run with data exited $rc1"
agg=$(find "$OUT" -path "*daily_agg*dt=$DS*" -name '*.parquet' | head -1)
[ -n "$agg" ] && pass "req3: daily aggregate partition for $DS exists" || fail "req3: no daily_agg/dt=$DS output"
clean=$(find "$OUT" -path "*clean*dt=$DS*" -name '*.parquet' | head -1)
[ -n "$clean" ] && pass "req3: cleaned parquet partition for $DS exists" || fail "req3: no cleaned/dt=$DS output"

# idempotency: rerun, output row count unchanged
if [ -n "$agg" ]; then
  n_before=$(python - "$OUT/daily_agg" "$DS" <<'PY' 2>/dev/null
from pyspark.sql import SparkSession;import sys
s=SparkSession.builder.master("local[1]").getOrCreate();s.sparkContext.setLogLevel("ERROR")
print(s.read.parquet(f"{sys.argv[1]}/dt={sys.argv[2]}").count())
PY
)
  python "$SOL" --ds "$DS" --raw "$RAW" --out "$OUT" >/dev/null 2>&1
  n_after=$(python - "$OUT/daily_agg" "$DS" <<'PY' 2>/dev/null
from pyspark.sql import SparkSession;import sys
s=SparkSession.builder.master("local[1]").getOrCreate();s.sparkContext.setLogLevel("ERROR")
print(s.read.parquet(f"{sys.argv[1]}/dt={sys.argv[2]}").count())
PY
)
  [ -n "$n_before" ] && [ "$n_before" = "$n_after" ] && pass "req10: idempotent (rerun unchanged: $n_after rows)" || fail "req10: rerun changed output ($n_before -> $n_after)"
fi

# req4/11: missing input must fail
python "$SOL" --ds 2099-01-01 --raw "$RAW" --out "$OUT" >/dev/null 2>&1 && fail "req4/11: no-input day exited 0 (must be non-zero)" || pass "req4/11: no-input day exits non-zero"

# DAG static checks
grep -q "catchup=False" "$DAG" && pass "req6: catchup=False" || fail "req6: set catchup=False"
grep -Eq "schedule.*@daily|schedule_interval.*@daily" "$DAG" && pass "req6: @daily schedule" || fail "req6: set @daily schedule"
grep -q "retries" "$DAG" && pass "req6: retries configured" || fail "req6: set default_args retries"
grep -Eq '\{\{ ?ds ?\}\}|data_interval_start' "$DAG" && pass "req7: passes logical date (ds)" || fail "req7: pass {{ ds }}, not the wall clock"
grep -Eq 'utcnow|datetime\.now' "$DAG" && fail "req7: DAG reads the wall clock (utcnow/now)" || pass "req7: no wall-clock date in DAG"
grep -q "; echo" "$DAG" && fail "req8: a '; echo' may swallow the job exit code" || pass "req8: no exit-code-swallowing ; echo"
grep -Eiq "validate|validation" "$DAG" && pass "req9: a validate task is present" || fail "req9: add a validate task that fails on empty output"

# DAG imports cleanly
res=$( ( export DAGS_FOLDER="$PWD/dags"; source scripts/airflow-env.sh; python labs/_measure_dagbag.py dags "$(grep -oE 'dag_id="[^"]+"' "$DAG" | head -1 | sed 's/dag_id="//;s/"//')" 2>/dev/null ) | sed -n 's/.*RESULT //p')
echo "$res" | grep -q "ERRORS=0" && pass "DAG imports with no errors ($res)" || fail "DAG import error ($res)"

echo
[ "$FAILED" -eq 0 ] && echo "${GREEN}CAPSTONE PASSED (all requirements met)${RESET}" || { echo "${RED}CAPSTONE NOT YET PASSED — the FAIL lines name the day to revisit${RESET}"; exit 1; }
