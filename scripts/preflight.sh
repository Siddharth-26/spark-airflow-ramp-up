#!/usr/bin/env bash
# Checks the tools the Spark days need. Airflow days add Docker later.
set -uo pipefail
GREEN=$'\033[32m'; RED=$'\033[31m'; YELLOW=$'\033[33m'; RESET=$'\033[0m'
FAILED=0
ok()   { echo "${GREEN}OK${RESET}    $*"; }
bad()  { echo "${RED}MISSING${RESET} $*"; FAILED=1; }
warn() { echo "${YELLOW}NOTE${RESET}  $*"; }

# Java: Spark runs on the JVM. 8, 11, 17 or 21 work for Spark 3.5.
if command -v java >/dev/null 2>&1; then
  v=$(java -version 2>&1 | head -1)
  case "$v" in
    *'"21.'*|*'"17.'*|*'"11.'*|*'"1.8'*) ok "java: $v" ;;
    *) bad "java is present but not 8/11/17/21: $v  (brew install openjdk@17)" ;;
  esac
else
  bad "java not found  (brew install openjdk@17)"
fi

# Python 3.10-3.12 for the venv.
PY=""
for c in python3.11 python3.12 python3.10; do
  command -v "$c" >/dev/null 2>&1 && { PY="$c"; break; }
done
if [ -n "$PY" ]; then ok "python: $($PY --version) ($PY)"; else bad "need python 3.10-3.12  (brew install python@3.11)"; fi

# Disk and RAM are advisory.
free_gb=$(df -g . | awk 'NR==2{print $4}')
[ "${free_gb:-0}" -ge 5 ] && ok "free disk: ${free_gb} GB" || warn "only ${free_gb} GB free; want 5+ GB"

# Docker is only needed from Day 8 (Airflow).
if docker info >/dev/null 2>&1; then ok "docker daemon running (needed from Day 8)"; else warn "docker not running; fine until Day 8"; fi

[ "$FAILED" -eq 0 ] && echo "${GREEN}preflight passed${RESET}" || { echo "${RED}preflight failed${RESET}"; exit 1; }
