# Finding 02: Streaming Recovery & Failure Dynamics

## Executive Summary
This experiment investigates the execution semantics, concurrency, failure behavior under abrupt termination (`SIGKILL`), and offset recovery in Spark Declarative Pipelines (SDP) on Apache Spark 4.2.0. We implemented a multi-flow pipeline (`pipelines/streaming`) consisting of:
1. An independent streaming table (`rate_stream`) fed by Spark's built-in `rate` source (`@dp.table`).
2. A batch table (`batch_events`) loaded from JSON data (`@dp.materialized_view`).
3. A downstream materialized view (`high_value_events_mv`) filtering `batch_events` (`@dp.materialized_view`), establishing a real dependency edge.

---

## 1. Pipeline Architecture & Execution Graph

### Definitions
- **Streaming Table (`rate_stream`)**:
  ```python
  @dp.table(
      name="rate_stream",
      comment="Streaming table ingested from Spark built-in rate source"
  )
  def rate_stream() -> DataFrame:
      return (
          spark.readStream
          .format("rate")
          .option("rowsPerSecond", 5)
          .load()
      )
  ```
- **Batch Table (`batch_events`) & Downstream MV (`high_value_events_mv`)**:
  ```python
  @dp.materialized_view(name="batch_events")
  def batch_events() -> DataFrame:
      return spark.read.json(f"file://{DATA_PATH}")

  @dp.materialized_view(name="high_value_events_mv")
  def high_value_events_mv() -> DataFrame:
      return spark.table("batch_events").filter("value >= 50")
  ```

### Dependency Ordering & Concurrency Observation
During execution, the CLI prints the graph evaluation lifecycle:

```text
2026-09-07 20:31:07: Flow spark_catalog.default.rate_stream is QUEUED.
2026-09-07 20:31:07: Flow spark_catalog.default.batch_events is QUEUED.
2026-09-07 20:31:07: Flow spark_catalog.default.high_value_events_mv is QUEUED.
2026-09-07 20:31:07: Flow spark_catalog.default.rate_stream is STARTING.
2026-09-07 20:31:07: Flow spark_catalog.default.rate_stream is RUNNING.
2026-09-07 20:31:07: Flow spark_catalog.default.batch_events is PLANNING.
2026-09-07 20:31:07: Flow spark_catalog.default.batch_events is STARTING.
2026-09-07 20:31:07: Flow spark_catalog.default.batch_events is RUNNING.
2026-09-07 20:31:08: Flow spark_catalog.default.batch_events has COMPLETED.
2026-09-07 20:31:09: Flow spark_catalog.default.rate_stream has COMPLETED.
2026-09-07 20:31:09: Flow spark_catalog.default.high_value_events_mv is PLANNING.
2026-09-07 20:31:09: Flow spark_catalog.default.high_value_events_mv is STARTING.
2026-09-07 20:31:09: Flow spark_catalog.default.high_value_events_mv is RUNNING.
2026-09-07 20:31:10: Flow spark_catalog.default.high_value_events_mv has COMPLETED.
2026-09-07 20:31:11: Run is COMPLETED.
```

#### Observations:
1. **Concurrency**: `rate_stream` and `batch_events` have no mutual dependencies. Both flows transitions to `STARTING` and `RUNNING` concurrently at `20:31:07` on separate threads (governed by `spark.sql.pipelines.maxConcurrentFlows`, default in `TriggeredGraphExecution`).
2. **Dependency Edge**: `high_value_events_mv` depends on `batch_events`. It remained in `QUEUED` state until `20:31:08` when `batch_events` finished execution, then progressed through `PLANNING` -> `STARTING` -> `RUNNING` -> `COMPLETED`.

---

## 2. Checkpoint Directory Structure

SDP maintains streaming checkpoints inside `<storage>/_checkpoints/<catalog>/<database>/<flow_name>/<flow_name>/0/`.

### Before Kill (After Several Completed Micro-Batches 0, 1, 2)
```text
/home/ubuntu/sdp-lab/pipelines/streaming/storage
└── _checkpoints
    └── spark_catalog
        └── default
            └── rate_stream
                └── rate_stream
                    └── 0
                        ├── commits
                        │   ├── 0
                        │   ├── 1
                        │   └── 2
                        ├── metadata
                        ├── offsets
                        │   ├── 0
                        │   ├── 1
                        │   └── 2
                        └── sources
                            └── 0
                                └── 0
```

### After Hard Kill (`SIGKILL / kill -9`)
During Batch 3, the pipeline was killed hard using `kill -9` while `rate_stream` and `batch_events` were actively running.
The checkpoint directory state after the kill was:
```text
/home/ubuntu/sdp-lab/pipelines/streaming/storage
└── _checkpoints
    └── spark_catalog
        └── default
            └── rate_stream
                └── rate_stream
                    └── 0
                        ├── commits
                        │   ├── 0
                        │   ├── 1
                        │   ├── 2
                        │   └── 3
                        ├── metadata
                        ├── offsets
                        │   ├── 0
                        │   ├── 1
                        │   ├── 2
                        │   └── 3
                        └── sources
                            └── 0
                                └── 0
```

---

## 3. What the CLI Reported About the Failure

When `SIGKILL` (`kill -9`) was dispatched to the Spark Connect / Pipelines process (`org.apache.spark.deploy.SparkPipelines`):
- **Output**: The CLI emitted **zero lines of error or failure notification**.
- **Last output before sudden termination**:
  ```text
  2026-09-07 20:34:02: Flow spark_catalog.default.rate_stream is RUNNING.
  2026-09-07 20:34:02: Flow spark_catalog.default.batch_events is PLANNING.
  2026-09-07 20:34:02: Flow spark_catalog.default.batch_events is STARTING.
  2026-09-07 20:34:02: Flow spark_catalog.default.batch_events is RUNNING.
  ```
- **Exit Behavior**: Process was unceremoniously terminated by the Linux kernel. No JVM shutdown hooks executed, no stack trace was printed, and no session cleanup occurred.

---

## 4. Recovery and Offset Analysis on Restart

### Checkpoint Offsets Evidence
Inspecting the committed offsets in `storage/_checkpoints/spark_catalog/default/rate_stream/rate_stream/0/offsets/`:

- **Offset 0** (Batch 0 initial setup):
  `0`
- **Offset 1** (Batch 1 committed):
  `24`
- **Offset 2** (Batch 2 committed):
  `148`
- **Offset 3** (Batch 3 committed right before kill):
  `174`

### Restart Run (Batch 4)
Upon issuing `spark-pipelines run` after the hard kill:
The pipeline started, loaded the existing checkpoint metadata, and executed:
```text
2026-09-07 20:34:27: Flow spark_catalog.default.rate_stream is QUEUED.
2026-09-07 20:34:27: Flow spark_catalog.default.batch_events is QUEUED.
2026-09-07 20:34:27: Flow spark_catalog.default.high_value_events_mv is QUEUED.
...
2026-09-07 20:34:29: Flow spark_catalog.default.rate_stream has COMPLETED.
2026-09-07 20:34:30: Flow spark_catalog.default.high_value_events_mv has COMPLETED.
2026-09-07 20:34:32: Run is COMPLETED.
```

A new offset file was committed: `offsets/4`:
- **Offset 4** (Batch 4 committed):
  `200`

### Parquet Storage Evidence (No Reprocessing, Exact Resumption)
Examining the underlying parquet part files produced in `spark-warehouse/rate_stream`:

| Parquet File | Batch | Row Count | Min `value` | Max `value` |
| :--- | :--- | :--- | :--- | :--- |
| `part-00000-2e542b3a...` | Batch 0 | 0 | None | None |
| `part-00000-1092d7c8...` | Batch 1 | 120 | 0 | 119 |
| `part-00000-70088301...` | Batch 2 | 620 | 120 | 739 |
| `part-00000-be603fac...` | Batch 3 (pre-kill) | 130 | 740 | 869 |
| `part-00000-c89af8ea...` | Batch 4 (restart) | 130 | **870** | **999** |

#### Crucial Finding:
- Pre-kill Batch 3 ended with `max(value) = 869`.
- Post-restart Batch 4 began with `min(value) = 870`.
- **Conclusion**: The restart **strictly resumed from the checkpoint offset** (`174` -> `200`). It did **not** reprocess from zero, produced zero duplicate records, and had zero dropped records.

---

## 5. Architectural Gotcha: Catalog Persistence vs In-Memory Session
- In vanilla Spark without Hive metastore or an external catalog catalog (e.g. Iceberg / BigLake), the default session catalog is in-memory.
- When an in-memory catalog restarts, it does not know tables exist in the warehouse directory. When creating batch materialized view tables (`batch_events`), it encounters `[LOCATION_ALREADY_EXISTS] Cannot name the managed table as spark_catalog.default.batch_events`.
- Setting `spark.sql.catalogImplementation hive` in `/opt/spark/conf/spark-defaults.conf` enables the embedded Derby metastore (`metastore_db`), allowing table metadata to persist across CLI invocations.
- Note: Setting `spark.sql.catalogImplementation` inside `spark-pipeline.yml` fails with `[CANNOT_MODIFY_STATIC_CONFIG]` (SQLSTATE `46110`) because it is a static Spark configuration that cannot be modified at session runtime.
