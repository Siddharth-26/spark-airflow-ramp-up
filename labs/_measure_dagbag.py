import sys, time
from airflow.models import DagBag
folder, dag_id = sys.argv[1], sys.argv[2]
t = time.time()
bag = DagBag(folder, include_examples=False)
dt = time.time() - t
d = bag.dags.get(dag_id)
print(f"RESULT PARSE_SECONDS={dt:.2f} TASKS={len(d.tasks) if d else 0} ERRORS={len(bag.import_errors)}")
