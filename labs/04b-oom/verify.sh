#!/usr/bin/env bash
source "$(dirname "$0")/../_lib.sh"
[ -f labs/04b-oom/job.py ] || { fail "run ./break.sh first"; exit 1; }
grep -q 'spark.driver.memory", "480m"' labs/04b-oom/job.py && pass "memory limit untouched" || fail "you changed the memory limit; fix the job, not the limit"
PYTHONPATH=app python labs/04b-oom/job.py > $KIROCREW_SCRATCH/l04b.out 2> $KIROCREW_SCRATCH/l04b.err
grep -q OutOfMemoryError $KIROCREW_SCRATCH/l04b.err && fail "OutOfMemoryError: Java heap space"
[ "$(grep -c '^ROW' $KIROCREW_SCRATCH/l04b.out)" -eq 5 ] && pass "5 tenant rows" || fail "missing result rows"
# Truth: events per tenant x length(event_id)*100
exp=$(python - <<'PY'
import json,glob,collections
c=collections.Counter(); ch=collections.Counter()
for f in glob.glob("data/raw/orders/*/events.jsonl"):
    for l in open(f):
        e=json.loads(l); c[e["tenant_id"]]+=1; ch[e["tenant_id"]]+=len(e["event_id"])*100
print(" ".join(f"ROW {t} {c[t]} {ch[t]}" for t in sorted(c)))
PY
)
got=$(grep '^ROW' $KIROCREW_SCRATCH/l04b.out | tr '\n' ' ' | sed 's/ $//')
[ "$got" = "$exp" ] && pass "numbers match ground truth" || fail "numbers differ from ground truth"
finish "LAB 04b"
