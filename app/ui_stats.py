"""Tiny helper that reads the Spark UI's REST API from inside a running job and
prints one machine-readable line. Labs' verify.sh scripts parse that line.

    from ui_stats import report
    report(spark)

Prints e.g.:
    UI_STATS jobs=3 stage_tasks=[3,8] input_bytes=66044493 shuffle_write_bytes=984
"""
import json
import urllib.request


def report(spark):
    sc = spark.sparkContext
    base = sc.uiWebUrl.replace(sc.uiWebUrl.split("//")[1].split(":")[0], "localhost") + "/api/v1/applications"
    app = json.load(urllib.request.urlopen(base))[0]["id"]
    jobs = json.load(urllib.request.urlopen(f"{base}/{app}/jobs"))
    stages = json.load(urllib.request.urlopen(f"{base}/{app}/stages"))
    done = [s for s in stages if s["status"] == "COMPLETE"]
    print(
        "UI_STATS jobs=%d stage_tasks=%s input_bytes=%d shuffle_write_bytes=%d"
        % (
            len(jobs),
            sorted(s["numTasks"] for s in done),
            sum(s["inputBytes"] for s in done),
            sum(s["shuffleWriteBytes"] for s in done),
        )
    )


def max_task_share(spark, min_records=1000):
    """Largest share of a shuffle stage's input records handled by ONE task (1.0 = fully skewed)."""
    sc = spark.sparkContext
    base = sc.uiWebUrl.replace(sc.uiWebUrl.split("//")[1].split(":")[0], "localhost") + "/api/v1/applications"
    app = json.load(urllib.request.urlopen(base))[0]["id"]
    worst = 0.0
    for s in json.load(urllib.request.urlopen(f"{base}/{app}/stages")):
        if s["status"] != "COMPLETE" or s.get("shuffleReadRecords", 0) < min_records:
            continue
        tl = json.load(urllib.request.urlopen(f"{base}/{app}/stages/{s['stageId']}/{s['attemptId']}/taskList?length=1000"))
        recs = [t["taskMetrics"]["shuffleReadMetrics"]["recordsRead"] for t in tl if "taskMetrics" in t]
        if sum(recs):
            worst = max(worst, max(recs) / sum(recs))
    print("SKEW max_task_share=%.2f" % worst)
