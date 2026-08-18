# Task 2 — Spark: Global Model Aggregation & Federated Averaging

## Problem

Once each client's local data has been summarized (Task 1), a Federated Learning system needs to combine those per-client summaries into a single global statistic — and, more importantly, simulate the *iterative* process real FL training actually uses: each round, clients compute a local update, and the server aggregates those updates into a new global average. This task implements that aggregation and simulation using Apache Spark.

## Input

The per-client `(sum, count)` results produced by the Hadoop job in Task 1 (`../task1_hadoop/output/result.txt`), e.g.:

```
client1  Sum: 2197.6, Count: 58
client2  Sum: 2381.8, Count: 57
client3  Sum: 2568.6, Count: 57
```

## Approach

`main.py` does three things:

1. **Parses the real Hadoop output** — a small regex-based parser (`parse_hadoop_output`) reads `result.txt` and converts each line into `(client_id, (sum, count))`. If the file isn't found (e.g. running this script standalone, without Task 1's output available), it falls back to the simulated example values given in the assignment, so the script never breaks in isolation.
2. **Computes the initial global average** — `total_sum / total_count` across all clients, using Spark's RDD `.map()` and `.sum()`.
3. **Simulates 3 rounds of federated averaging** — each round: compute each client's local average (`sum/count`), perturb it by a small random value (standing in for a local training step), then recompute the global average from the perturbed values.

This mirrors the core idea of the *Federated Averaging (FedAvg)* algorithm: the server never sees raw client data, only aggregated statistics, and the global model is repeatedly refined by combining independently-computed local updates.

## Environment

The job runs on a small Spark standalone cluster defined in `../../../docker/spark/docker-compose.yml`: one Spark Master and one Spark Worker, using the `bde2020/spark-master` / `bde2020/spark-worker` images.

## How to Run

```bash
# 1. Start the Spark cluster
cd docker/spark
docker compose up -d

# 2. Copy the script and Hadoop's output into the Spark Master container
docker cp ../../part3_federated_learning/task2_spark/main.py spark-master:/tmp/main.py
docker cp ../../part3_federated_learning/task1_hadoop/output/result.txt spark-master:/tmp/result.txt

# 3. Run it
docker exec -it spark-master bash
cd /tmp
/spark/bin/spark-submit main.py
```

Spark Master status (and connected workers) can be checked at `http://localhost:8080`.

## Result

```
Initial Global Average: 41.558

Round 1 Global Average: 41.780
Round 2 Global Average: 42.113
Round 3 Global Average: 40.961
```

The small round-to-round fluctuation is expected — each round perturbs every client's local average by `random.uniform(-1, 1)` before recomputing the global average, so the result oscillates around the true global average rather than converging to a single fixed point. This is the intended behavior of the simplified simulation described in the assignment (a full FedAvg implementation would instead converge as clients train on real local data).

Full deliverables:
- Spark code: `main.py`
- Execution log: `output/spark_run_output.txt` (full per-round output, captured directly from `spark-submit`)
- Execution screenshots: `screenshots/` — terminal job execution (`Running.png`) and the Spark Master UI showing the connected worker (`SparkMasterUI.png`)

## Where This Fits Into the Project

This task consumes Task 1's Hadoop output directly (see `../task1_hadoop/task1_hadoop_README.md`), completing the two-stage pipeline: Hadoop handles the one-shot batch aggregation of raw client data, and Spark handles the iterative, in-memory federated averaging on top of those aggregates — see the root [README](../../README.md) for how the two stages fit together, and `../task3_analysis/README.md` for a performance comparison between them.
