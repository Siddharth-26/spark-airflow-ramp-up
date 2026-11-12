#!/usr/bin/env bash
# Creates .venv with pinned PySpark. Day 1 has you do these steps by hand first;
# this script is the shortcut afterwards.
set -euo pipefail
cd "$(dirname "$0")/.."
PY=""
for c in python3.11 python3.12 python3.10; do command -v "$c" >/dev/null 2>&1 && { PY="$c"; break; }; done
[ -n "$PY" ] || { echo "need python 3.10-3.12"; exit 1; }
"$PY" -m venv .venv
.venv/bin/pip install --quiet --upgrade pip
.venv/bin/pip install --quiet -r requirements.txt
echo "venv ready. Activate with:  source .venv/bin/activate"
.venv/bin/python -c "import pyspark; print('pyspark', pyspark.__version__)"
