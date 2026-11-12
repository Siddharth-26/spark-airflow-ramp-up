#!/usr/bin/env bash
source "$(dirname "$0")/../_lib.sh"
[ -f labs/05a-bad-records/job.py ] || { fail "run ./break.sh first"; exit 1; }
rm -rf labs/05a-bad-records/quarantine
out=$(run_job labs/05a-bad-records/job.py)
echo "$out" | grep -E '^(GOOD|BAD)'
read EXP_GOOD EXP_BAD < <(python3 - <<'PY'
import json,glob
g=b=0
for f in glob.glob("data/messy/*/events.jsonl"):
    for l in open(f):
        try:
            e=json.loads(l); ok = e.get("amount") is not None and e.get("tenant_id") is not None
        except Exception: ok=False
        g+=ok; b+=(not ok)
print(g,b)
PY
)
GOOD=$(echo "$out" | sed -n 's/^GOOD //p'); BAD=$(echo "$out" | sed -n 's/^BAD //p')
[ "$GOOD" = "$EXP_GOOD" ] && pass "GOOD = $GOOD" || fail "GOOD is ${GOOD:-missing}, expected $EXP_GOOD"
[ "$BAD" = "$EXP_BAD" ] && pass "BAD = $BAD (nothing silently dropped)" || fail "BAD is ${BAD:-missing}, expected $EXP_BAD"
n=$(cat labs/05a-bad-records/quarantine/part-* 2>/dev/null | wc -l | tr -d ' ')
[ "$n" = "$EXP_BAD" ] && pass "$n bad records saved in labs/05a-bad-records/quarantine/" || fail "quarantine holds $n lines, expected $EXP_BAD"
finish "LAB 05a"
