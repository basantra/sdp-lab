# Result — 2026-09-07

## What I ran
1. Provisioned OCI infrastructure using Terraform (`infra/`).
2. Executed `env/provision.sh` on the Ubuntu 22.04 instance to install OpenJDK 17, Spark 4.2.0, and `pyspark[pipelines]==4.2.0`.
3. Created minimal SDP project `pipelines/hello` with local data (`users.json`), materialized view transformation (`users_mv.py`), and pipeline spec (`spark-pipeline.yml`).
4. Executed `spark-pipelines run` (First Run & Second Run unchanged).
5. Built `pipelines/streaming` with a streaming table (`rate_stream`), a batch table (`batch_events`), and a downstream materialized view (`high_value_events_mv`). Executed multiple micro-batches, hard-killed the process with `SIGKILL` (`kill -9`), and restarted.
6. Built `pipelines/boundaries` to systematically probe flow type boundaries across streaming sources, batch sources, streaming table output tables, and temporary views.

10. Built `pipelines/object_storage` with streaming table (`s3_events`) ingesting JSON events from OCI Object Storage bucket `sdp-lab-data` via Hadoop S3A connector and AWS SDK v2.
11. Executed verification tests: batch MV repeat run under persistent Hive catalog and object storage incremental batch read.

## What I expected
- Run 1: Successful pipeline execution and materialization of `top_users_mv`.
- Run 2: Incremental execution or idempotency check detecting no changes.
- Streaming Recovery: Resumption from the last committed offset after a hard `SIGKILL` without duplicate processing.
- Flow Boundaries: Engine error reporting for incompatible relation types across streaming and batch boundaries.
- Batch MV Verification: After persistent catalog fix, repeat run skips unchanged input.
- Object Storage Verification: Second run ingests only newly landed files.

## What happened
- `pipelines/hello`: Run 1 succeeded in 15.4s creating Parquet dataset. Run 2 originally failed with `[LOCATION_ALREADY_EXISTS]` due to ephemeral in-memory catalog restarting without awareness of on-disk warehouse table.
- `pipelines/streaming`: Configured persistent Hive catalog (`spark.sql.catalogImplementation hive`). Verified independent streaming (`rate_stream`) and batch (`batch_events`) flows execute concurrently, while downstream MV (`high_value_events_mv`) waits sequentially. Issued `kill -9` during active micro-batch execution (Batch 3 committed at offset 174). CLI emitted no failure message. On restart, pipeline resumed strictly from offset 174 to 200 (rows 870 to 999), demonstrating exact resumption and zero data duplication. Documented in `findings/02-recovery.md`.
- `pipelines/boundaries`: Discovered hard engine constraints:
  - MV reading streaming source: rejected with `[INVALID_FLOW_QUERY_TYPE.STREAMING_RELATION_FOR_MATERIALIZED_VIEW]`, SQLSTATE `42000`.
  - Streaming table reading batch source: rejected with `[INVALID_FLOW_QUERY_TYPE.BATCH_RELATION_FOR_STREAMING_TABLE]`, SQLSTATE `42000`.
  - MV reading streaming table's output table: succeeded cleanly (exit code 0), acting as the canonical stream-to-batch bridge.
  - Temporary view between streaming source and MV: rejected with `[INCOMPATIBLE_STREAMING_VIEW_READ]`, SQLSTATE `42000` (and `STREAMING_RELATION_FOR_MATERIALIZED_VIEW` when check bypassed). Documented in `findings/03-flow-type-boundaries.md`.
- `pipelines/hello` (Post-Catalog Persistence Verification): Run 1 (19.7s) and Run 2 (19.0s) both exited 0. However, Run 2 **fully recomputed**, replacing the part file with a new UUID and advancing mtimes by 26s (`20:51:42` -> `20:52:08`).
- `pipelines/object_storage`: Run 1 (21.2s) ingested 2 initial files (4 rows). Run 2 (20.2s) ingested 2 newly landed files (4 rows). Verified via checkpoint `sources/0/1` that Run 2 read **only the two new files**.

## Exit code
- `pipelines/hello` Original Run 1: 0, Run 2: 1
- `pipelines/streaming` Pre-kill runs: 0; Hard kill: 137 (SIGKILL); Restart run: 0
- `pipelines/boundaries` Case 1: 1; Variation 1: 1; Variation 2: 0; Variation 3: 1
- `pipelines/hello` Verification Run 1: 0, Run 2: 0 (Full recompute observed)
- `pipelines/object_storage` Run 1: 0, Run 2: 0 (Incremental read observed)

## Log path
runs/2026-09-07/commands.log

