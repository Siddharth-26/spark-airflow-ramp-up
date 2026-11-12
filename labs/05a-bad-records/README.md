# Lab 05a — "The numbers add up, and 1,677 records vanished"

## The incident
The job reports `GOOD <n>` and nobody questions it. But `wc -l` on the input says there are
**163,341** lines and `GOOD` is lower. Where did the rest go, and why did Spark never say
a word?

## Run
```bash
./break.sh
PYTHONPATH=app python labs/05a-bad-records/job.py       # from the repo root
```
## Your task
Account for every line. Print `BAD <n>` and write the bad records (raw text, so a human can
see *what* was wrong) to `labs/05a-bad-records/quarantine/` as JSON-lines. A record is bad
if it is not valid JSON, or `amount` is null, or `tenant_id` is missing. `GOOD + BAD` must
equal the number of input lines. `./verify.sh`.

<details><summary>Hint</summary>
Day 6 §2. Give `spark.read` an explicit `schema` that includes a `_corrupt_record string`
column, `mode=PERMISSIVE`, and `.cache()` the DataFrame before you filter on that column
(Spark refuses queries that only touch the corrupt column on uncached raw files).
</details>
