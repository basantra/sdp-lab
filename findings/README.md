# findings/

This directory contains the empirical findings, root-cause analyses, error catalogs, and architectural diagrams produced during the Spark Declarative Pipelines (SDP) lab on Apache Spark 4.2.0 on OCI.

Every finding is grounded in live-instance telemetry, system metrics, file metadata, or decompiled bytecode.

---

## Index of Findings

| Document | Title & Core Investigation | Key Telemetry / Verdict |
| :--- | :--- | :--- |
| **[`01-first-run.md`](01-first-run.md)** | **First Run & The Ephemeral Catalog Collapse** | `spark-pipelines run` crash on repeat runs with `[LOCATION_ALREADY_EXISTS]`. Why static Spark configs fail in `spark-pipeline.yml` (`SQLSTATE 46110`). |
| **[`02-recovery.md`](02-recovery.md)** | **Streaming Concurrency, Hard Kill, & Recovery** | Simultaneous execution of disjoint DAG flows. `SIGKILL` (`kill -9`) failure semantics and exact offset resumption (`offsets/3` -> `offsets/4`) without duplicate processing. |
| **[`03-flow-type-boundaries.md`](03-flow-type-boundaries.md)** | **Flow Type Boundary Rules & Error State Catalog** | Systematic probing of engine rules between `@dp.table`, `@dp.materialized_view`, and `@dp.temporary_view`. Why physical table storage is the only valid stream-to-batch bridge. |
| **[`04-object-storage.md`](04-object-storage.md)** | **OCI Object Storage Ingestion via S3A Connector** | Hadoop 3.5.0 S3A connector with AWS SDK v2 (`bundle-2.35.4.jar`). Resolving the PySpark pip site-packages jar classpath trap and path-style regional endpoints. |
| **[`05-verification.md`](05-verification.md)** | **Pre-Publication Verification & Empirical Audit** | Live-instance architecture audit (`x86_64`), three-way verification of no continuous daemon mode, proof of batch MV full recomputation, and object storage incremental read proof. |
| **[`SUMMARY.md`](SUMMARY.md)** | **Synthesis: What the Spark Docs Do Not State** | Comprehensive comparison of official Spark documentation claims vs. observed engine behavior, labeled as direct observations vs. inferences. |

---

## Architectural Diagrams (`findings/diagrams/`)

The [`diagrams/`](diagrams/) directory contains publication-ready figures in four formats (Graphviz `.dot`, `.svg`, `.png`, `.pdf`) and interactive Miro exports:

* **Figure 1**: [SDP Scope Boundary (`fig1-sdp-scope-boundary`)](diagrams/fig1-sdp-scope-boundary.svg) — What Apache SDP 4.2.0 provides vs. what the practitioner still owns.
* **Figure 2**: [Refresh Behaviour Across Two Runs (`fig2-refresh-behaviour-two-runs`)](diagrams/fig2-refresh-behaviour-two-runs.svg) — Incremental streaming tables vs. full recomputation of batch materialized views.
* **Figure 3**: [Stream-to-Batch Bridge (`fig3-stream-to-batch-bridge`)](diagrams/fig3-stream-to-batch-bridge.svg) — Hard engine barriers and the single supported bridge pattern.
* **Interactive Miro Board**: [https://miro.com/app/board/uXjVHpm378I=/](https://miro.com/app/board/uXjVHpm378I=/)
