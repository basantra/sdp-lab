# First Run Observations and Recompute Behavior in Spark Declarative Pipelines (SDP)

Date: 2026-09-07  
Related runs: runs/2026-09-07/

## Overview

We created the smallest complete SDP pipeline project in `pipelines/hello/`:
- `data/users.json`: 5 records of users with `id`, `name`, `dept`, `score`.
- `transformations/users_mv.py`: Materialized view `top_users_mv` filtering `score >= 90`.
- `spark-pipeline.yml`: Spec defining project name `hello`, `storage: file:///home/ubuntu/sdp-lab/pipelines/hello/storage`, and library glob `transformations/**`.

We executed `spark-pipelines run` twice sequentially without modifying any code, configuration, or data.

---

## 1. What got written to the storage directory?

### Observation
**Nothing was written to the `storage` directory.** The directory was not even created on disk.

### Evidence
Running directory inspection after Run 1:
```bash
$ ls -la /home/ubuntu/sdp-lab/pipelines/hello/
total 24
drwxr-xr-x 5 ubuntu ubuntu 4096 Sep  7 20:15 .
drwxrwxr-x 3 ubuntu ubuntu 4096 Sep  7 20:14 ..
drwxr-xr-x 2 ubuntu ubuntu 4096 Sep  7 20:14 data
-rw-r--r-- 1 ubuntu ubuntu  128 Sep  7 20:14 spark-pipeline.yml
drwxr-xr-x 3 ubuntu ubuntu 4096 Sep  7 20:15 spark-warehouse
drwxr-xr-x 3 ubuntu ubuntu 4096 Sep  7 20:15 transformations

$ ls -d /home/ubuntu/sdp-lab/pipelines/hello/storage
ls: cannot access '/home/ubuntu/sdp-lab/pipelines/hello/storage': No such file or directory
```

Tree of the actual project directory after execution:
```
pipelines/hello/
├── data/
│   └── users.json
├── spark-pipeline.yml
├── spark-warehouse/
│   └── top_users_mv/
│       ├── ._SUCCESS.crc
│       ├── .part-00000-8af80d48-2f50-419c-97c7-c3cdf09d245e-c000.snappy.parquet.crc
│       ├── _SUCCESS
│       └── part-00000-8af80d48-2f50-419c-97c7-c3cdf09d245e-c000.snappy.parquet
└── transformations/
    ├── __pycache__/
    │   └── users_mv.cpython-310.pyc
    └── users_mv.py
```

### Finding
The `storage` location specified in `spark-pipeline.yml` is reserved for streaming table checkpoint state. When a pipeline consists exclusively of batch materialized views (`@dp.materialized_view`), SDP does not allocate checkpoints or create the storage directory.

---

## 2. Where did the table land, and under what catalog and database?

### Observation
- **Physical Path on Disk**: `/home/ubuntu/sdp-lab/pipelines/hello/spark-warehouse/top_users_mv/`
- **Catalog**: `spark_catalog` (Spark default session catalog)
- **Database / Schema**: `default`
- **Table Name**: `top_users_mv`
- **Format**: Snappy-compressed Apache Parquet (`part-00000-*.snappy.parquet`)

### Evidence
During pipeline execution, the CLI log output explicitly identified the fully qualified table name:
```
2026-09-07 20:15:31: Flow spark_catalog.default.top_users_mv is QUEUED.
2026-09-07 20:15:31: Flow spark_catalog.default.top_users_mv is PLANNING.
2026-09-07 20:15:31: Flow spark_catalog.default.top_users_mv is STARTING.
2026-09-07 20:15:31: Flow spark_catalog.default.top_users_mv is RUNNING.
2026-09-07 20:15:32: Flow spark_catalog.default.top_users_mv has COMPLETED.
```

Querying the physical files using PySpark confirms the dataset contents:
```bash
$ python3 -c 'from pyspark.sql import SparkSession; spark = SparkSession.builder.getOrCreate(); spark.read.parquet("spark-warehouse/top_users_mv").show()'
+--------+---+--------------+-----+
|    dept| id|          name|score|
+--------+---+--------------+-----+
|Research|  1|  Ada Lovelace|   98|
|Research|  2|   Alan Turing|   95|
| Systems|  3|  Grace Hopper|   92|
|  Theory|  4|Claude Shannon|   90|
+--------+---+--------------+-----+
```

---

## 3. What does the CLI print about the dependency graph before it executes anything?

### Observation
The CLI does **not** render an ASCII visual DAG or formatted dependency tree in stdout. Instead, it prints timestamped progress events showing the discovery, registration, and lifecycle state changes of the graph elements.

### Evidence
Verbatim stdout from `spark-pipelines run`:
```text
2026-09-07 20:15:23: Loading pipeline spec from /home/ubuntu/sdp-lab/pipelines/hello/spark-pipeline.yml...
2026-09-07 20:15:23: Creating Spark session...
2026-09-07 20:15:28: Creating dataflow graph...
2026-09-07 20:15:29: Registering graph elements...
2026-09-07 20:15:29: Loading definitions. Root directory: '/home/ubuntu/sdp-lab/pipelines/hello'.
2026-09-07 20:15:29: Found 1 files matching glob 'transformations/**/*'
2026-09-07 20:15:29: Importing /home/ubuntu/sdp-lab/pipelines/hello/transformations/users_mv.py...
2026-09-07 20:15:31: Starting run...
2026-09-07 20:15:31: Flow spark_catalog.default.top_users_mv is QUEUED.
2026-09-07 20:15:31: Flow spark_catalog.default.top_users_mv is PLANNING.
2026-09-07 20:15:31: Flow spark_catalog.default.top_users_mv is STARTING.
2026-09-07 20:15:31: Flow spark_catalog.default.top_users_mv is RUNNING.
2026-09-07 20:15:32: Flow spark_catalog.default.top_users_mv has COMPLETED.
2026-09-07 20:15:34: Run is COMPLETED.
```

---

## 4. Run a second time unchanged: Full recompute or not?

### Observation
Running `spark-pipelines run` a second time unchanged results in **neither an incremental run nor a successful full recompute**. Instead, the CLI crashes with **exit code 1** due to a fatal `LOCATION_ALREADY_EXISTS` conflict. The underlying data files on disk are **completely untouched**.

### Evidence

#### A. Execution Timing & Exit Code
- **Run 1**:
  - Exit code: `0`
  - Duration: `real 0m15.401s | user 0m4.258s | sys 0m0.465s`
  - Result: `Run is COMPLETED.`
- **Run 2 (unchanged)**:
  - Exit code: `1`
  - Duration: `real 0m12.400s | user 0m4.232s | sys 0m0.447s`
  - Result: Failed during table initialization.

#### B. Error Stack Trace (Run 2)
```text
2026-09-07 20:16:30: Creating dataflow graph...
2026-09-07 20:16:31: Registering graph elements...
2026-09-07 20:16:31: Loading definitions. Root directory: '/home/ubuntu/sdp-lab/pipelines/hello'.
2026-09-07 20:16:31: Found 1 files matching glob 'transformations/**/*'
2026-09-07 20:16:31: Importing /home/ubuntu/sdp-lab/pipelines/hello/transformations/users_mv.py...
2026-09-07 20:16:33: Starting run...
Traceback (most recent call last):
  File "/opt/spark-4.2.0-bin-hadoop3/python/pyspark/pipelines/cli.py", line 450, in <module>
    main()
  File "/opt/spark-4.2.0-bin-hadoop3/python/pyspark/pipelines/cli.py", line 429, in main
    run(
  File "/opt/spark-4.2.0-bin-hadoop3/python/pyspark/pipelines/cli.py", line 353, in run
    handle_pipeline_events(result_iter)
  File "/opt/spark/python/lib/pyspark.zip/pyspark/pipelines/spark_connect_pipeline.py", line 53, in handle_pipeline_events
    for result in iter:
  File "/opt/spark/python/lib/pyspark.zip/pyspark/sql/connect/client/core.py", line 1244, in execute_command_as_iterator
  File "/opt/spark/python/lib/pyspark.zip/pyspark/sql/connect/client/core.py", line 1722, in _execute_and_fetch_as_iterator
  File "/opt/spark/python/lib/pyspark.zip/pyspark/sql/connect/client/core.py", line 2084, in _handle_error
  File "/opt/spark/python/lib/pyspark.zip/pyspark/sql/connect/client/core.py", line 2168, in _handle_rpc_error
pyspark.errors.exceptions.connect.UnknownException: (org.apache.spark.sql.pipelines.graph.DatasetManager$TableMaterializationException) org.apache.spark.SparkRuntimeException: [LOCATION_ALREADY_EXISTS] Cannot name the managed table as `spark_catalog`.`default`.`top_users_mv`, as its associated location 'file:/home/ubuntu/sdp-lab/pipelines/hello/spark-warehouse/top_users_mv' already exists. Please pick a different table name, or remove the existing location first. SQLSTATE: 42710
```

#### C. File Modification Timestamps (Pre vs Post Run 2)
File timestamps measured via `stat -c "%n | size: %s bytes | mtime: %y (%Y)"`:

| File | Before Run 2 | After Run 2 | Change |
| :--- | :--- | :--- | :--- |
| `_SUCCESS` | `2026-09-07 20:15:32.900699132 +0000` (1788812132) | `2026-09-07 20:15:32.900699132 +0000` (1788812132) | **Identical (Unmodified)** |
| `part-00000-*.parquet` | `2026-09-07 20:15:32.859698349 +0000` (1788812132) | `2026-09-07 20:15:32.859698349 +0000` (1788812132) | **Identical (Unmodified)** |

Even when executing with `spark-pipelines run --full-refresh-all`, the identical `[LOCATION_ALREADY_EXISTS]` error occurs.

---

## Implication

In the standalone default configuration (without an external persistent metastore like Hive Metastore or an Iceberg / Unity catalog configured), each invocation of `spark-pipelines` spins up a new Spark Connect process with an empty in-memory catalog.

When SDP attempts to register and plan the dataset:
1. The new session's catalog has no record that `spark_catalog.default.top_users_mv` already exists.
2. It attempts to create a new managed table using the default warehouse directory.
3. Spark detects that the target directory `file:/.../spark-warehouse/top_users_mv` already contains files from the previous run.
4. Spark's safety checks reject creating a managed table over an existing non-empty directory (`LOCATION_ALREADY_EXISTS`), aborting execution before query planning or execution can take place.

**Conclusion**: For SDP pipelines to support repeatable incremental runs or recomputation, a persistent catalog (e.g. Hive Metastore, Iceberg REST, or Unity Catalog) must be configured in `configuration` within `spark-pipeline.yml`.
