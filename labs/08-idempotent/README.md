# Lab 08 — "A retry doubled the numbers"

## The incident
`lab08_load` has `retries: 2`. Last night the task failed after writing, retried, and
finance's totals for that day were 2x. Today someone also re-ran the 1st by hand. Make the
task **safe to run any number of times for the same day**.

## Run
```bash
./break.sh
./verify.sh     # runs day 1 three times and day 2 once
```
## Your task
After those four runs the output must contain exactly one line per tenant per day (10 lines).
`./verify.sh`.

<details><summary>Hint</summary>Day 9 §2. Append is the enemy. What if each day owned its own file?</details>
