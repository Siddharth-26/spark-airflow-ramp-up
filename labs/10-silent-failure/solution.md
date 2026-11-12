# Solution — Lab 10

**Cause:** `BashOperator` marks the task successful iff the **last command's** exit code is 0.
The command was `spark-job ; echo "pipeline step finished"`. The `;` runs `echo` regardless,
`echo` exits 0, and the Spark job's exit code 2 is thrown away.

**Fix:** let the job's exit code decide the task state.
```python
bash_command=f"cd {ROOT} && {PY} app/pipeline.py --ds {{{{ ds }}}} --out data/lab10"
```
(`&&` chains stop at the first failure; add `set -euo pipefail` for multi-line scripts.)

**Companion rules**
- A job must **exit non-zero** on missing input/invalid output (our `pipeline.py` does,
  exit code 2).
- "Success with zero rows" is the nastiest failure. Add a validation task (row count > 0,
  freshness, null rate) after the load and let *it* fail the run.
- Alert on failure (`on_failure_callback`/email) or the red box is only seen by whoever happens
  to open the UI.
