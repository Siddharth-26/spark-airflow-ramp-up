#!/usr/bin/env bash
set -uo pipefail
cd "$(dirname "$0")/../.."
source .venv/bin/activate 2>/dev/null || { echo "run scripts/setup-venv.sh first"; exit 1; }
GREEN=$'\033[32m'; RED=$'\033[31m'; RESET=$'\033[0m'
FAILED=0
pass() { echo "${GREEN}PASS${RESET}  $*"; }
fail() { echo "${RED}FAIL${RESET}  $*"; FAILED=1; }

if [ ! -f labs/00-first-job/my_job.py ]; then
  fail "labs/00-first-job/my_job.py does not exist yet"
  exit 1
fi

out=$(python labs/00-first-job/my_job.py 2>/dev/null)

if echo "$out" | grep -q "tenant_id"; then
  pass "output has a tenant_id column"
else
  fail "no 'tenant_id' column found in output"
fi

# 5 tenants expected (acme, globex, initech, umbrella, hooli) for an unskewed day.
n_rows=$(echo "$out" | grep -cE '^\|\s*(acme|globex|initech|umbrella|hooli)\s*\|')
if [ "$n_rows" -eq 5 ]; then
  pass "all 5 tenants present"
else
  fail "expected 5 tenant rows, found $n_rows"
fi

echo
[ "$FAILED" -eq 0 ] && echo "${GREEN}LAB 00 PASSED${RESET}" || { echo "${RED}LAB 00 NOT YET PASSED${RESET}"; exit 1; }
