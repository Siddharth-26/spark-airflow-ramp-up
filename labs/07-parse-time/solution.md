# Solution — Lab 07

**Cause:** everything at module top level runs **every time the scheduler (or a worker)
imports the file**: every few seconds to minutes, for every DAG file, forever. A 3-second
call at import means 3 seconds of scheduler CPU per parse per file, and if the remote
service is slow or down, *all* DAG parsing for that file stalls or errors.

**Fix:** keep top-level code to declarations only; do I/O inside a task.
```python
TENANTS = ["acme", "globex", "initech", "umbrella", "hooli"]   # static list in code/config
```
If the list truly is dynamic, fetch it inside a task and fan out with dynamic task mapping
(`.expand(...)`), or read a cached file/Variable (cheap) rather than calling a service.

**Review rules:** no network, DB or heavy imports (pandas, pyspark) at module level in a
DAG file; import them inside the callable. Check parse time with `airflow dags report` or
the "DAG Processing Time" in the UI.
