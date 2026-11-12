# Data Processing Ramp-Up (Spark + Airflow) for Senior Software Engineers

A self-paced, hands-on ramp-up for an **SSE**, not a data engineer: you need to be
able to write a Spark job, run it, debug it when it's slow or wrong, handle the
edge cases real data throws at you, and build an Airflow pipeline around it.

Same lab structure as `../k8s-ramp-up/` (read, then break-fix against real data), but
otherwise independent — no Kubernetes knowledge is assumed or used anywhere here.

## Who this is for

- SSEs who will write and own data-processing jobs, not platform/data engineers
  who run a shared cluster for others.
- **Assumed:** Python and general programming experience. **Not assumed:** anything
  Spark- or Airflow-specific, Kubernetes, or distributed-systems background — those
  are taught from zero. (If you've done `../k8s-ramp-up/`, the lab format — README,
  `break.sh`, `verify.sh`, `solution.md` — will feel familiar, but nothing here
  depends on it.)
- You will debug real performance problems (skew, shuffles, OOM) using the Spark
  UI, and real pipeline problems (idempotency, backfills, retries) in Airflow.

## What you will be able to do by the end

1. Write a Spark job from scratch and explain what happens between `.read()` and
   the result landing on disk.
2. Read the Spark UI and `.explain()` output to find *why* a job is slow, and fix it.
3. Handle real-world data problems: schema drift, nulls, duplicates, late events,
   too many small files.
4. Design and operate Airflow DAGs that are idempotent and safe to retry/backfill.
5. Wire Spark and Airflow together into one pipeline, and review someone else's.

## Where jobs actually run

Every day except the optional capstone runs **entirely on your laptop** — Spark in
local mode (`local[*]`, no cluster), Airflow in a pinned venv from
Day 8 (`airflow dags test`; an optional Docker Compose file gives you the web UI). This is deliberate: a laptop lets you see every log line and restart in
seconds, and the code you write does not change when it later runs on a real
cluster — only `master(...)` and how the job is submitted differ.

The optional Day 12 capstone runs the same pipeline on a real managed service
(EMR Serverless or MWAA) so you've seen the gap between "runs on my machine" and
"runs on a cluster" at least once, torn down the same day.

## Prerequisites (Day 0)

```bash
./scripts/preflight.sh      # checks Java, Python, disk; Docker is optional (Airflow UI only)
./scripts/setup-venv.sh     # creates .venv with pinned PySpark (after Day 1 teaches you why)
```

Day 1 has you build the Python environment by hand first, so you know what
`setup-venv.sh` is actually doing before you let it do it for you.

## The sample system: OrderFlow

The same domain you'd recognize from the microservices ramp-up: order events
(`created`, `paid`, `shipped`, `cancelled`) for 5 tenants, plus a sixth
(`megacorp`) that is deliberately oversized when Day 4 switches skew on. Data is
generated locally by `app/generate_orders.py` — no download, fully reproducible,
and the messiness (duplicates, late events, bad records) is toggled by flag so
each day controls exactly what you're debugging.

## How the labs work

Identical model to k8s-ramp-up: `labs/NN-name/README.md` (symptom, no answer),
`break.sh` (sets it up), `verify.sh` (your grade), `solution.md` (read after).

## Repo layout

```
data-ramp-up/
├── README.md
├── docs/            # day-by-day reading + curriculum
├── app/             # OrderFlow data generator, shared job code
├── labs/            # break-fix / build labs
├── scripts/         # preflight, venv setup, airflow-env.sh (Airflow venv env vars)
├── data/            # generated locally, gitignored
└── assessment/      # graded capstone
```
