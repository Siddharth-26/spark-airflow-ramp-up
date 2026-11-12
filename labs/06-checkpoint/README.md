# Lab 06 — "The nightly stream job doubled our data"

## The incident
`job.py` is a Structured Streaming job run nightly with `availableNow`. Last night ops
re-ran it after a hiccup and every order appeared twice downstream. Make a re-run safe.

## Run
```bash
./break.sh
PYTHONPATH=app python labs/06-checkpoint/job.py     # run it twice and count rows in labs/06-checkpoint/out
```
## Your task
After two runs the output must hold exactly one copy of each input line, with the
checkpoint stored at `labs/06-checkpoint/ckpt`. `./verify.sh` (it runs your job twice).

<details><summary>Hint</summary>Day 7 §4: where does a stream remember which files it has already read?</details>
