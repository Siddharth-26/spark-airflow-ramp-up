# source this: sets up an isolated, local-only Airflow (SQLite, no Docker needed)
# Usage:  source scripts/airflow-env.sh
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")/.." && pwd)"
source "$ROOT/.venv-airflow/bin/activate"
export AIRFLOW_HOME="$ROOT/airflow_home"
export AIRFLOW__CORE__DAGS_FOLDER="${DAGS_FOLDER:-$ROOT/dags}"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__EXECUTOR=SequentialExecutor
export AIRFLOW__CORE__UNIT_TEST_MODE=False
export AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL=5
export OBJECTS_ROOT="$ROOT"
export SPARK_PYTHON="$ROOT/.venv/bin/python"
export NO_PROXY='*'   # macOS: avoids a fork-safety crash in some Airflow tasks
mkdir -p "$AIRFLOW_HOME"
[ -f "$AIRFLOW_HOME/airflow.db" ] || airflow db migrate >/dev/null 2>&1
