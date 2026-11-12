# Shared helpers for lab scripts. Source this: source "$(dirname "$0")/../_lib.sh"
cd "$(dirname "${BASH_SOURCE[0]}")/.."
source .venv/bin/activate 2>/dev/null || { echo "run scripts/setup-venv.sh first"; exit 1; }
GREEN=$'\033[32m'; RED=$'\033[31m'; RESET=$'\033[0m'
FAILED=0
pass() { echo "${GREEN}PASS${RESET}  $*"; }
fail() { echo "${RED}FAIL${RESET}  $*"; FAILED=1; }
ensure_data() { [ -d data/raw/orders ] || python app/generate_orders.py; }
finish() { echo; if [ "$FAILED" -eq 0 ]; then echo "${GREEN}$1 PASSED${RESET}"; else echo "${RED}$1 NOT YET PASSED${RESET}"; exit 1; fi; }
# run_job <file>: runs a PySpark file from the repo root, stdout only, warnings dropped
run_job() { PYTHONPATH=app python "$1" 2>/dev/null; }
# Airflow labs: run a command inside the isolated local Airflow, DAGs taken from one lab folder
af() { ( export DAGS_FOLDER="$PWD/$LABDIR"; source scripts/airflow-env.sh; "$@" ); }
