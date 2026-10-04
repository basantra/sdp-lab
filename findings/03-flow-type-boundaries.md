# Finding 03: Flow Type Boundaries in Spark Declarative Pipelines (SDP)

## Executive Summary
This experiment empirically maps the boundary between batch and streaming relations within Spark Declarative Pipelines (SDP) in Apache Spark 4.2.0. We tested four configurations across the flow boundary:
1. **Materialized View reading directly from a streaming source**
2. **Streaming Table reading from a batch source**
3. **Materialized View reading from a Streaming Table's output table**
4. **Temporary View interposed between a streaming source and a Materialized View**

Below is the empirical breakdown distinguishing hard engine constraints from documentation happy-paths.

See architectural diagram: [Figure 3: Stream-to-Batch Bridge](diagrams/fig3-stream-to-batch-bridge.svg).

---

## 1. Flow Type Boundary Mapping

| What I Tried | Code Pattern | What Happened | Error Class & SQLSTATE | Engine Constraint vs. Happy Path | Implications for Pipeline Design |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Materialized View reading from Streaming Source** | `@dp.materialized_view`<br>`return spark.readStream.format("rate").load()` | **Failed during flow resolution/planning** before execution started. | **Error Class:**<br>`INVALID_FLOW_QUERY_TYPE.STREAMING_RELATION_FOR_MATERIALIZED_VIEW`<br><br>**SQLSTATE:**<br>`42000` | **Hard Engine Constraint**<br>Enforced in Spark Connect / SDP flow analysis (`DatasetManager.scala` / `FlowExecution.scala`). | Materialized views cannot ingest live streaming sources directly. Ingestion from continuous/streaming endpoints must use `@dp.table`. |
| **Streaming Table reading from Batch Source** | `@dp.table`<br>`return spark.range(10)` | **Failed during flow resolution/planning** before execution started. | **Error Class:**<br>`INVALID_FLOW_QUERY_TYPE.BATCH_RELATION_FOR_STREAMING_TABLE`<br><br>**SQLSTATE:**<br>`42000` | **Hard Engine Constraint**<br>Engine checks `isStreaming` on the returned DataFrame. Batch relations are rejected outright. | `@dp.table` cannot be used to load static snapshot or batch files directly unless wrapped in `spark.readStream` (e.g. streaming file source or `STREAM` operator in Spark SQL). |
| **Materialized View reading from Streaming Table output table** | `@dp.table(name="stream_src")`<br>`...spark.readStream...`<br><br>`@dp.materialized_view(name="mv_downstream")`<br>`return spark.table("stream_src")` | **Succeeded cleanly (exit code 0)**.<br><br>`stream_src` executed first as a streaming microbatch, followed by `mv_downstream` executing in batch mode over the newly appended records. | **None**<br>(Run completed with exit code 0). | **Supported Design Pattern** (Boundary Bridge).<br>Not just doc convention; fully supported by the dependency analyzer. | The physical table produced by a streaming table acts as the canonical bridge from streaming to batch. Downstream batch analytics/MVs can safely query streaming tables via `spark.table(...)`. |
| **Temporary View between Streaming Source and Materialized View** | `@dp.temporary_view(name="tv_stream")`<br>`...spark.readStream...`<br><br>`@dp.materialized_view(name="mv_from_tv")`<br>`return spark.table("tv_stream")` | **Failed during flow resolution**.<br>First failed with incompatible view check; when check was bypassed, failed with streaming relation error. | **Error Class:**<br>`INCOMPATIBLE_STREAMING_VIEW_READ`<br>(or `INVALID_FLOW_QUERY_TYPE.STREAMING_RELATION_FOR_MATERIALIZED_VIEW` if check disabled)<br><br>**SQLSTATE:**<br>`42000` | **Hard Engine Constraint**.<br>Temporary views preserve relational stream attributes (`isStreaming=true`) and do not materialize storage. | A temporary view cannot decouple or bridge streaming data into batch logic. It is a logical substitution, not a buffer or materialization boundary. |

---

## 2. Detailed Technical Observations

### Experiment 1: Materialized View reading from Streaming Source
- **Code**:
  ```python
  @dp.materialized_view(name="mv_from_stream")
  def mv_from_stream() -> DataFrame:
      return spark.readStream.format("rate").load()
  ```
- **Exact CLI Output**:
  ```text
  pyspark.errors.exceptions.connect.AnalysisException: [INVALID_FLOW_QUERY_TYPE.STREAMING_RELATION_FOR_MATERIALIZED_VIEW] Flow `spark_catalog`.`default`.`mv_from_stream` returns an invalid relation type. Materialized views may only be defined by a batch relation, but the flow `spark_catalog`.`default`.`mv_from_stream` attempts to write a streaming relation to the materialized view `spark_catalog`.`default`.`mv_from_stream`. SQLSTATE: 42000
  ```
- **Mechanism**: SDP verifies whether the returned DataFrame has `.isStreaming == true`. If true for a `@dp.materialized_view`, the engine immediately rejects the flow definition during graph validation.

---

### Experiment 2: Streaming Table reading from Batch Source
- **Code**:
  ```python
  @dp.table(name="stream_from_batch")
  def stream_from_batch() -> DataFrame:
      return spark.range(10)
  ```
- **Exact CLI Output**:
  ```text
  pyspark.errors.exceptions.connect.AnalysisException: [INVALID_FLOW_QUERY_TYPE.BATCH_RELATION_FOR_STREAMING_TABLE] Flow `spark_catalog`.`default`.`stream_from_batch` returns an invalid relation type. Streaming tables may only be defined by streaming relations, but the flow `spark_catalog`.`default`.`stream_from_batch` attempts to write a batch relation to the streaming table `spark_catalog`.`default`.`stream_from_batch`. Consider using the STREAM operator in Spark-SQL to convert the batch relation into a streaming relation, or populating the streaming table with an append once-flow instead. SQLSTATE: 42000
  ```
- **Mechanism**: The inverse validation occurs for `@dp.table`. If `.isStreaming == false`, the engine rejects the definition and advises using the `STREAM` operator in Spark SQL or an `append once-flow`.

---

### Experiment 3: Materialized View reading from Streaming Table Output Table
- **Code**:
  ```python
  @dp.table(name="stream_src")
  def stream_src() -> DataFrame:
      return spark.readStream.format("rate").option("rowsPerSecond", 5).load()

  @dp.materialized_view(name="mv_downstream")
  def mv_downstream() -> DataFrame:
      return spark.table("stream_src")
  ```
- **Execution Log**:
  ```text
  2026-09-07 20:37:20: Flow spark_catalog.default.stream_src is QUEUED.
  2026-09-07 20:37:20: Flow spark_catalog.default.mv_downstream is QUEUED.
  2026-09-07 20:37:20: Flow spark_catalog.default.stream_src is STARTING.
  2026-09-07 20:37:21: Flow spark_catalog.default.stream_src is RUNNING.
  2026-09-07 20:37:23: Flow spark_catalog.default.stream_src has COMPLETED.
  2026-09-07 20:37:23: Flow spark_catalog.default.mv_downstream is PLANNING.
  2026-09-07 20:37:23: Flow spark_catalog.default.mv_downstream is STARTING.
  2026-09-07 20:37:23: Flow spark_catalog.default.mv_downstream is RUNNING.
  2026-09-07 20:37:24: Flow spark_catalog.default.mv_downstream has COMPLETED.
  2026-09-07 20:37:25: Run is COMPLETED.
  ```
- **Mechanism**:
  - `spark.table("stream_src")` loads the catalog table as a **batch Dataset** (`isStreaming == false`).
  - The SDP graph builder correctly infers the dataflow edge: `stream_src` -> `mv_downstream`.
  - Topological scheduling ensures `stream_src` completes its micro-batch write before `mv_downstream` plans and materializes the latest snapshot.

---

### Experiment 4: Temporary View between Streaming Source and Materialized View
- **Code**:
  ```python
  @dp.temporary_view(name="tv_stream")
  def tv_stream() -> DataFrame:
      return spark.readStream.format("rate").load()

  @dp.materialized_view(name="mv_from_tv")
  def mv_from_tv() -> DataFrame:
      return spark.table("tv_stream")
  ```
- **Exact CLI Output**:
  ```text
  2026-09-07 20:37:45: Failed to resolve flow: 'spark_catalog.default.mv_from_tv'.
  Error: [INCOMPATIBLE_STREAMING_VIEW_READ] View `tv_stream` is a streaming view and must be referenced using SparkSession#readStream. This check can be disabled by setting Spark conf pipelines.incompatibleViewCheck.enabled = false. SQLSTATE: 42000
  ```
- **Test with `pipelines.incompatibleViewCheck.enabled = false`**:
  When setting `spark_conf={"pipelines.incompatibleViewCheck.enabled": "false"}`, the engine bypasses the pre-check, inlines the logical plan of the temporary view, and immediately fails with:
  ```text
  pyspark.errors.exceptions.connect.AnalysisException: [INVALID_FLOW_QUERY_TYPE.STREAMING_RELATION_FOR_MATERIALIZED_VIEW] Flow `spark_catalog`.`default`.`mv_from_tv` returns an invalid relation type. Materialized views may only be defined by a batch relation, but the flow `spark_catalog`.`default`.`mv_from_tv` attempts to write a streaming relation to the materialized view `spark_catalog`.`default`.`mv_from_tv`. SQLSTATE: 42000
  ```
- **Mechanism**: A temporary view has no persistent storage. Reading it via `spark.table(...)` does not transform a streaming relation into a batch relation; the streaming attribute propagates through the view into the consumer.

---

## 3. Summary of Architectural Rules for SDP Pipeline Design
1. **Rule of Relation Alignment**:
   - `@dp.table` **requires** `isStreaming == true`.
   - `@dp.materialized_view` **requires** `isStreaming == false`.
2. **The Only Valid Stream-to-Batch Bridge**:
   - The only valid boundary crossing from streaming to batch is through a **materialized storage layer** (a `@dp.table` consumed via `spark.table("streaming_table")`).
3. **No In-Memory Stream Decoupling**:
   - Logical constructs like `@dp.temporary_view` cannot bridge the streaming-to-batch boundary because they do not write data to physical storage.
