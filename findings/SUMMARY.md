# Summary of Empirical Findings (Beyond the Spark Documentation)

This document synthesizes everything learned from testing Spark Declarative Pipelines (SDP) on Apache Spark 4.2.0 on Oracle Cloud Infrastructure that is **not** documented in the official Spark Declarative Pipelines programming guide.

Every claim is grounded in exact log lines from the `runs/` directory and classified as either a direct **Observation** or an **Inference**. An audit section flags any claims made in earlier findings that the logs do not strictly support.

---

## 1. What We Learned (Not in the Spark Documentation)

### A. The Ephemeral Catalog Collapse on Repeated Runs
- **What Spark Docs Say**: The documentation describes running pipelines incrementally using `spark-pipelines run` and recomputing tables using `--full-refresh`.
- **What Actually Happens**: On a standard Spark install without Hive Metastore or an external catalog configured, running `spark-pipelines run` a second time unchanged immediately crashes with exit code 1:
  ```text
  pyspark.errors.exceptions.connect.UnknownException: (org.apache.spark.sql.pipelines.graph.DatasetManager$TableMaterializationException) org.apache.spark.SparkRuntimeException: [LOCATION_ALREADY_EXISTS] Cannot name the managed table as `spark_catalog`.`default`.`top_users_mv`, as its associated location 'file:/home/ubuntu/sdp-lab/pipelines/hello/spark-warehouse/top_users_mv' already exists.
  ```
- **Log Citation**: `runs/2026-09-07/commands.log` line 18 (`exit=1`), stack trace documented in `runs/2026-09-07/result.md` lines 14-16.
- **Classification**: **Observation** for the failure; **Inference** for root cause (each CLI run starts a fresh SparkConnect JVM with an empty in-memory catalog). This inference was subsequently proven when setting `spark.sql.catalogImplementation hive` in `spark-defaults.conf` enabled repeat runs to succeed with exit code 0 (`runs/2026-09-07/commands.log` lines 31-32).

---

### B. Static Spark Configurations Cannot Be Set in `spark-pipeline.yml`
- **What Spark Docs Say**: The spec allows a `configuration` dictionary for custom Spark configuration keys.
- **What Actually Happens**: Setting any static Spark configuration (such as `spark.sql.catalogImplementation`) in `spark-pipeline.yml` results in a fatal runtime error during flow execution:
  ```text
  AnalysisException: [CANNOT_MODIFY_STATIC_CONFIG] Cannot modify the value of the static Spark config: "spark.sql.catalogImplementation". SQLSTATE: 46110
  ```
- **Log Citation**: `runs/2026-09-07/commands.log` line 26 (`exit=1`), task-741 log lines 10-18.
- **Classification**: **Observation**. SDP attempts to apply pipeline configurations dynamically per-flow via `SparkSessionUtils.withSqlConf()` calling `RuntimeConfig.set()`. Static configurations must be configured in `/opt/spark/conf/spark-defaults.conf` prior to JVM startup.

---

### C. Pure Batch Pipelines Never Create or Populate `storage:`
- **What Spark Docs Say**: `storage:` is listed as a required top-level key in `spark-pipeline.yml`, defined as the location to store pipeline metadata and checkpoints.
- **What Actually Happens**: When a pipeline contains only batch materialized views (`@dp.materialized_view`), the directory specified in `storage:` is **never created on disk**.
- **Log Citation**: `findings/01-first-run.md` section 1 (`ls: cannot access '/home/ubuntu/sdp-lab/pipelines/hello/storage': No such file or directory`) following the successful run in `runs/2026-09-07/commands.log` line 16.
- **Classification**: **Observation**. `storage:` is exclusively used for streaming checkpoint directories (`_checkpoints/...`).

---

### D. The Execution Model is `Trigger.AvailableNow()`, Not Continuous
- **What Spark Docs Say**: The programming guide describes streaming tables as ingesting continuous streams.
- **What Actually Happens**: The CLI executes a triggered graph update rather than running as a daemon. The underlying engine (`TriggeredGraphExecution.scala`) unconditionally applies `Trigger.AvailableNow()` to all streaming queries. For sources like `RateStreamV2` that do not support `AvailableNow`, Spark falls back to single-batch execution per CLI invocation:
  ```text
  WARN MicroBatchExecution: source [RateStreamV2...] does not support Trigger.AvailableNow. Falling back to single batch execution.
  ```
- **Log Citation**: `runs/2026-09-07/commands.log` line 30 (task-762 log line 31). Bytecode decompilation of `org.apache.spark.sql.pipelines.graph.TriggeredGraphExecution` confirmed `streamTrigger()` returns `Trigger.AvailableNow()`.
- **Classification**: **Observation** for the single-batch fallback and warning; **Inference** that Spark 4.2.0 lacks a continuous long-lived daemon mode in the `spark-pipelines` CLI.

---

### E. Concurrency Semantics of Independent Flows
- **What Spark Docs Say**: The documentation explains DAG dependencies conceptually but does not define thread execution behavior.
- **What Actually Happens**: Disjoint graph components execute concurrently on separate threads, while dependent nodes wait for their ancestors:
  - `rate_stream` and `batch_events` transitioned to `STARTING` and `RUNNING` simultaneously at timestamp `20:31:07`.
  - `high_value_events_mv` waited in `QUEUED` until `batch_events` reached `COMPLETED` at `20:31:08`, then transitioned to `RUNNING` and `COMPLETED`.
- **Log Citation**: `runs/2026-09-07/commands.log` line 30 (task-762 log lines 22-38).
- **Classification**: **Observation**.

---

### F. Flow Type Boundaries and The Only Valid Stream-to-Batch Bridge
- **What Spark Docs Say**: Provides syntax for `@dp.table`, `@dp.materialized_view`, and `@dp.temporary_view`.
- **What Actually Happens**:
  1. `@dp.materialized_view` over `readStream` fails: `[INVALID_FLOW_QUERY_TYPE.STREAMING_RELATION_FOR_MATERIALIZED_VIEW]`, SQLSTATE `42000` (`runs/2026-09-07/commands.log` line 46).
  2. `@dp.table` over batch `read` fails: `[INVALID_FLOW_QUERY_TYPE.BATCH_RELATION_FOR_STREAMING_TABLE]`, SQLSTATE `42000` (`runs/2026-09-07/commands.log` line 48).
  3. `@dp.temporary_view` over stream into MV fails: `[INCOMPATIBLE_STREAMING_VIEW_READ]`, SQLSTATE `42000` (`runs/2026-09-07/commands.log` line 52). Disabling the check still fails with `STREAMING_RELATION_FOR_MATERIALIZED_VIEW` (`commands.log` line 54).
  4. Materialized View reading `@dp.table` output via `spark.table(...)` **succeeds cleanly** (`runs/2026-09-07/commands.log` line 50).
- **Classification**: **Observation** for the errors and exit codes; **Inference** that physical table storage is the only architectural bridge capable of decoupling streaming into batch logic in SDP.

---

### G. PySpark Pip Jar Resolution Path
- **What Spark Docs Say**: Standard documentation advises placing third-party jars (e.g. `hadoop-aws`) in `$SPARK_HOME/jars/`.
- **What Actually Happens**: When `pyspark[pipelines]` is installed via pip into user site-packages, PySpark loads its default classpath from `/home/ubuntu/.local/lib/python3.10/site-packages/pyspark/jars/`. Jars placed solely in `/opt/spark/jars/` are ignored by PySpark sessions.
- **Log Citation**: `runs/2026-09-07/commands.log` line 55 (downloaded jars to `/opt/spark/jars/`), followed by task-956 failing with `ClassNotFoundException: org.apache.hadoop.fs.s3a.S3AFileSystem`. Copying jars to `/home/ubuntu/.local/lib/python3.10/site-packages/pyspark/jars/` (`commands.log` line 57) resulted in immediate class resolution (`SUCCESS! Found class: class org.apache.hadoop.fs.s3a.S3AFileSystem` in task-964).
- **Classification**: **Observation**.

---

## 2. Audit: Claims Flagged Against the Run Logs

| Claim in Finding Document | Document & Section | What the Text Claimed | What the Logs Actually Support | Audit Verdict |
| :--- | :--- | :--- | :--- | :--- |
| **"During Batch 3, the pipeline was killed hard using kill -9 while rate_stream and batch_events were actively running"** | `findings/02-recovery.md`<br>Section 2 | Implied the kill interrupted the write of Batch 3. | `kill_test.log` shows `rate_stream is RUNNING` and `batch_events is RUNNING`. However, `storage/.../offsets/3` and `commits/3` were already written to disk before the kill landed. The kill actually terminated the run before `batch_events` finished and before `high_value_events_mv` could run. | ⚠️ **Partially Supported (Timing Clarified)**:<br>The kill interrupted the *pipeline graph execution cycle*, but Batch 3's streaming offset had already flushed to disk. |
| **"In the standalone default configuration, each invocation spins up a new Spark Connect process with an empty in-memory catalog"** | `findings/01-first-run.md`<br>Section 5 | Stated as factual explanation for `LOCATION_ALREADY_EXISTS`. | The log strictly records `[LOCATION_ALREADY_EXISTS]`. The explanation that the in-memory catalog restarts empty was an **inference** at the time of writing Finding 01, though later confirmed in Finding 02. | ⚠️ **Inference Labeled as Fact in Finding 01**:<br>Should have been labeled as an inference until tested in run 2026-09-07T20:31Z. |
| **"Restart strictly resumed from offset 174 to 200 without reprocessing from zero"** | `findings/02-recovery.md`<br>Section 4 | Parquet part files contain distinct sequential value ranges (Batch 3: 740–869, Batch 4: 870–999). | Parquet stats query in task-838 confirmed: `part-00000-be603fac...` (min=740, max=869) and `part-00000-c89af8ea...` (min=870, max=999). | ✅ **Fully Supported by Log Telemetry**. |
| **"All Python dependencies installed from pre-built aarch64 wheels"** | `env/provision.sh`<br>Line 88 | Provisioning script log stated all packages installed from pre-built wheels. | Machine was provisioned as an `x86_64` (`amd64`) instance due to ARM capacity exhaustion; wheels installed were `x86_64`, not `aarch64`. | ⚠️ **Architecture Discrepancy Flagged**:<br>The provision script printout was written assuming ARM, but ran on x86_64. |
