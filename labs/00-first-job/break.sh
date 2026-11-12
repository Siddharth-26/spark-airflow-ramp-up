#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
source .venv/bin/activate 2>/dev/null || { echo "run scripts/setup-venv.sh first"; exit 1; }
if [ ! -d data/raw/orders ]; then
  echo "Generating sample data (first run only)..."
  python app/generate_orders.py
fi
echo "Setup OK. Now write your job: labs/00-first-job/my_job.py"
