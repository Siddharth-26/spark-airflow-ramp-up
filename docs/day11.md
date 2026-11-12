# Day 11 — Production concerns: monitoring, cost, tuning, and reviewing a pipeline

**Goal:** by the end of today you can say what to monitor on a Spark+Airflow pipeline in
production, reason about what makes a job expensive and which knob to turn, and review someone
else's pipeline PR with a real checklist. No new lab — today is the reference you keep open at
work.

**Assumed:** Days 1–10. This day consolidates; everything here points back to a concept you
already ran.

---

## 1. Monitoring — what "healthy" means for each system

You cannot alert on "the pipeline is fine." You alert on specific, measurable failures.

**Airflow — orchestration health:**
- **Task/DAG failures** — `on_failure_callback` to Slack/pager (Day 9 §6); the red box in the
  UI is not an alert.
- **SLA misses** — a run that didn't finish in time even if it eventually succeeded
  (`sla=timedelta(...)`). Catches "the pipeline is 6 hours late," which failure alerts miss.
- **DAG parse time / import errors** — the scheduler's "DAG processing time" and the
  Import Errors list; a slow or broken parse (Day 8 §3) degrades *everything*.
- **Scheduler lag / queued tasks piling up** — tasks stuck `queued` means not enough executor
  slots (or sensors hogging them, Day 9 §5).

**Spark — job health (per run):**
- **Duration trend** — the same daily job suddenly taking 3× longer is the earliest signal of
  data growth or skew.
- **Shuffle read/write bytes** — the cost driver (Day 4). A jump means more data moving.
- **Failed/retried tasks, executor losses** — OOMs and stragglers (Day 5).
- **Output validation** — row counts, freshness, null-rate bounds as an explicit task
  (Day 10 §3). "Success with zero rows" is invisible to everything above.

**The data itself (don't skip this):**
- **Freshness** — is today's partition present and recent?
- **Volume** — row count within an expected band (a 10× drop or spike is a bug upstream).
- **Bad-record rate** — the quarantine count from Day 6; alert when it jumps, not when the job
  fails.

---

## 2. Cost and tuning cheatsheet

What makes a Spark job expensive, and the knob for each. Always confirm the cause in the UI
(Day 5) *before* turning a knob.

| Symptom (from the UI) | Likely cause | First thing to try |
|---|---|---|
| Huge shuffle read/write bytes | avoidable shuffle / wide join | broadcast the small side (Day 4); filter/aggregate *before* the join |
| 1000s of tiny tasks | too many partitions | size `spark.sql.shuffle.partitions` to data, or trust AQE (Day 2/4) |
| Thousands of tiny output files | wide `partitionBy` × many partitions | `repartition(key)` / `coalesce` before write (Day 6 §4) |
| One task 10–100× slower; 1 straggler | data skew | salt the hot key / AQE skew join / broadcast (Day 5 §5) |
| OOM, long GC on one task | working set too big in one task | avoid `collect_list`/`collect`/`groupByKey`; aggregate with running values (Day 5 §4) |
| Reading all columns when you need 2 | no column pruning (e.g. `select *`, JSON) | use Parquet + select only needed columns (Day 1, Day 3) |
| Re-reading the same data each action | missing cache | `.cache()` a result reused across actions; `unpersist()` after (Day 3 §3) |
| Scanning all partitions for one day | no partition pruning | filter on the partition column; store partitioned by it (Day 1 §5) |

**The cost mental model:** on a cloud cluster you pay for *executor-hours* and *bytes
scanned/shuffled*. So the big levers are, in order: **read less** (Parquet + column/partition
pruning), **shuffle less** (broadcast, pre-aggregate, avoid needless `repartition`), **don't
re-read** (cache reused results), **right-size parallelism** (not 2,000 tasks for 5 numbers).
Tuning memory/cores is usually the *last* lever, not the first.

---

## 3. Reviewing a pipeline PR — the checklist

When you review someone's Spark+Airflow change, these are the questions that catch real bugs.
Each maps to a day you did.

**Correctness / data safety**
- [ ] Is every task **idempotent**? Does it overwrite a partition or append/blind-insert?
      (Day 9 §1 — the #1 data-corruption cause.)
- [ ] Does the job take its day from **`{{ ds }}`/the interval**, never the wall clock?
      (Day 9 §3.)
- [ ] Are **bad records** counted and quarantined, or silently dropped? Is `good + bad =
      input` checked? (Day 6 §2.)
- [ ] Is dedupe on the **business key**, not the whole row? (Day 6 §5.)
- [ ] Does the job **exit non-zero** on missing/invalid input, and is there a **validation**
      task so "zero rows" fails? (Day 10 §3.)
- [ ] Does the `BashOperator` command end with the real job (not `; echo` / a swallowed exit)?
      (Day 10 §3.)

**Performance**
- [ ] Any **avoidable shuffle** — a join that could be a broadcast, an aggregate that could
      precede the join? (Day 4.)
- [ ] Is a small dimension **broadcast** (and does it have stats, or a hint)? (Day 4 §4.)
- [ ] Any **skew** risk on a join/group key with a known hot value? (Day 5 §5.)
- [ ] **Partition count** sized to the data, or AQE left on? Any copied `shuffle.partitions`?
      (Day 2/4.)
- [ ] Output **file sizes** sane (not thousands of tiny files)? (Day 6 §4.)
- [ ] Reading **Parquet** and only needed columns, pruning partitions? (Day 1/3.)

**Operations**
- [ ] **Retries** set where failures are transient — and only on idempotent tasks? (Day 9 §2.)
- [ ] **Sensors** in `reschedule` mode with a `timeout`? (Day 9 §5.)
- [ ] **Alerting** on failure and SLA, not just a red UI box? (Day 9 §6.)
- [ ] No heavy work / network calls at **DAG module top level**? (Day 8 §3.)
- [ ] `catchup` deliberate, and is a **backfill** safe given idempotency? (Day 9 §4.)

**The two questions that catch the most:** "What happens if this runs twice?" (idempotency)
and "What happens if the input is empty or late?" (silent success + late data). If a PR
answers both well, it is probably sound.

---

## 4. End-of-day checklist

1. Name three things you'd alert on for Airflow, three for Spark, three for the data.
2. A daily job's duration doubled overnight with no code change. What do you look at first?
3. Rank the big cost levers for a cloud Spark job from most to least impactful.
4. A PR adds a nightly job that `INSERT`s into a summary table. What's your first review
   comment?
5. A PR's DAG imports pandas at module top level. Why flag it?
6. Give the two single questions that catch the most pipeline bugs in review.

Day 12 is the graded capstone: build and debug an OrderFlow pipeline end to end under a time
box, plus the optional cloud run.

## Cleanup
Nothing to do.
