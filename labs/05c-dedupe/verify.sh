#!/usr/bin/env bash
source "$(dirname "$0")/../_lib.sh"
[ -f labs/05c-dedupe/job.py ] || { fail "run ./break.sh first"; exit 1; }
out=$(run_job labs/05c-dedupe/job.py); echo "$out" | grep UNIQUE
exp=$(python3 - <<'PY'
import json,glob
ids=set()
for f in glob.glob("data/messy/*/events.jsonl"):
    for l in open(f):
        try: e=json.loads(l)
        except Exception: continue
        if e.get("amount") is not None and e.get("tenant_id") is not None: ids.add(e["event_id"])
print(len(ids))
PY
)
got=$(echo "$out" | sed -n 's/^UNIQUE //p')
[ "$got" = "$exp" ] && pass "UNIQUE = $got (one row per event_id)" || fail "UNIQUE is ${got:-missing}, expected $exp"
finish "LAB 05c"
