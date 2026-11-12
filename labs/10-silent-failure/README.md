# Lab 10 — "Green DAG, empty dashboard"

## The incident
The dashboard for 2027-01-01 is empty, but Airflow shows `lab10_pipeline` **green** for that
day. The upstream export was late, so the Spark job found no input and exited with an error,
yet Airflow reported success. Nobody was alerted.

## Run
```bash
./break.sh
./verify.sh     # runs a day with data and a day without
```
## Your task
A run for a day with no input must FAIL; a run for a day with data must still succeed.
`./verify.sh`.

<details><summary>Hint</summary>Day 10 §3: how does a BashOperator decide success? Read the command carefully, including what follows the `;`.</details>
