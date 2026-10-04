# RUNBOOK: Spark Declarative Pipelines (SDP) on Oracle Cloud Infrastructure (OCI)

This runbook provides complete, reproduce-from-zero instructions to provision, configure, execute, and validate Apache Spark 4.2.0 with Spark Declarative Pipelines (SDP) on Oracle Cloud Infrastructure (OCI).

It documents the **actual path taken**, including the exact pinned versions, hardware capacity realities, CLI entrypoint bugs, catalog persistence failures, and engine boundary constraints discovered during execution.

---

## 1. System & Architecture Overview

- **Cloud Provider**: Oracle Cloud Infrastructure (OCI)
- **Target OS**: Ubuntu 22.04 LTS (x86_64 / amd64)
- **Runtime Stack**:
  - OpenJDK: `17.0.20-headless`
  - Apache Spark: `4.2.0-bin-hadoop3`
  - Python: `3.10.12`
  - PySpark: `4.2.0` with `[pipelines]` extra
- **Networking**: Public Subnet with Security List ingress restricted to the operator's IP (SSH `22` and Spark UI `4040`).
- **Storage**: OCI Object Storage bucket `sdp-lab-data` (Standard tier).

---

## 2. Infrastructure Provisioning (`infra/`)

### A. Prerequisites & Provider Pinning
- Pinned Terraform OCI Provider: `9.0.0` in [`infra/versions.tf`](infra/versions.tf).
- OCI API credentials configured in `~/.oci/config` under profile (e.g. `JDE2`).

### B. Hardware Shape Capacity Pitfall & Resolution
- **Initial Requirement**: `VM.Standard.A1.Flex` (Ampere Altra ARM64, 4 OCPUs, 24GB RAM, Always Free).
- **Failure Encountered**:
  ```text
  Error: 500-InternalError: Out of host capacity.
  ```
  Host capacity was completely exhausted across all three Availability Domains (`AD-1`, `AD-2`, `AD-3`) and all fault domains in region `us-chicago-1`.
- **Resolution**:
  Switched compute shape to `VM.Standard.E5.Flex` (AMD EPYC Genoa x86_64, 1 OCPU, 8GB RAM, 100GB boot volume):
  ```hcl
  shape = "VM.Standard.E5.Flex"
  shape_config {
    ocpus         = 1
    memory_in_gbs = 8
  }
  ```

### C. Terraform Execution
```bash
source env/log.sh
run_logged terraform -chdir=infra init
run_logged terraform -chdir=infra apply -auto-approve
```
*Note*: If an instance was created during a partial apply retry, import it directly into state before applying:
```bash
terraform -chdir=infra import oci_core_instance.spark_lab <INSTANCE_OCID>
```

---

## 3. Environment Provisioning & Runtime Setup (`env/`)

### A. Automated Installer Script (`env/provision.sh`)
Execute the provisioning script on the remote instance:
```bash
ssh -i ~/.ssh/id_ed25519 ubuntu@<PUBLIC_IP> 'bash -s' < env/provision.sh
```

### B. Pitfall: The CLI Recursive Invocation Bug
- **Failure Encountered**:
  When `/usr/local/bin/spark-pipelines` was symlinked to `/opt/spark/bin/spark-pipelines`, invoking `spark-pipelines --help` failed with:
  ```text
  /opt/spark/bin/spark-pipelines: line 5: Argument list too long (exit code 126)
  ```
- **Root Cause**:
  `/opt/spark/bin/spark-pipelines` contained `exec /opt/spark/bin/spark-pipelines "$@"`, resulting in an infinite recursion loop.
- **Resolution**:
  Symlink both `/usr/local/bin/spark-pipelines` and `/opt/spark/bin/spark-pipelines` directly to the canonical PySpark entrypoint created by pip:
  ```bash
  sudo ln -sfn /home/ubuntu/.local/bin/spark-pipelines /usr/local/bin/spark-pipelines
  sudo ln -sfn /home/ubuntu/.local/bin/spark-pipelines /opt/spark/bin/spark-pipelines
  ```

### C. Pitfall: Non-Login Subshell Missing SPARK_HOME
- **Failure Encountered**:
  Executing automated bash scripts non-interactively failed with:
  ```text
  /usr/local/bin/spark-pipelines: line 26: find-spark-home: No such file or directory
  /usr/local/bin/spark-pipelines: line 40: /bin/spark-class: No such file or directory
  ```
- **Resolution**:
  Always ensure `/etc/profile.d/spark.sh` is sourced or environment variables are explicitly exported in execution scripts:
  ```bash
  export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
  export SPARK_HOME=/opt/spark
  export PATH="${SPARK_HOME}/bin:${PATH}"
  ```

---

## 4. Pipeline 1: Hello World & The Ephemeral Catalog Bug (`pipelines/hello/`)

### Project Setup
- `spark-pipeline.yml`:
  ```yaml
  name: hello
  storage: file:///home/ubuntu/sdp-lab/pipelines/hello/storage
  libraries:
    - glob:
        include: transformations/**
  ```
- `transformations/users_mv.py`:
  ```python
  from pyspark import pipelines as dp
  from pyspark.sql import DataFrame
  import os

  DATA_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "users.json"))

  @dp.materialized_view(name="top_users_mv")
  def top_users_mv() -> DataFrame:
      return spark.read.json(f"file://{DATA_PATH}").filter("score >= 90")
  ```

### Execution & Catalog Failure
1. **Run 1**: Succeeded in 15.4s. Created Parquet table at `spark-warehouse/top_users_mv`.
2. **Run 2 (Unchanged)**: Failed with exit code 1:
   ```text
   pyspark.errors.exceptions.connect.UnknownException: [LOCATION_ALREADY_EXISTS] Cannot name the managed table as spark_catalog.default.top_users_mv, as its associated location 'file:/home/ubuntu/sdp-lab/pipelines/hello/spark-warehouse/top_users_mv' already exists.
   ```
- **Root Cause**:
  By default, Spark uses an in-memory session catalog (`spark.sql.catalogImplementation: in-memory`). Each run of `spark-pipelines run` spawns a fresh Spark Connect JVM process with an empty metastore. On Run 2, Spark does not know the table exists and tries to execute `CREATE TABLE`, which fails because the underlying directory already exists.

### Failed Fix vs. Working Fix
- **Failed Attempt**: Adding `configuration: spark.sql.catalogImplementation: hive` in `spark-pipeline.yml` failed with:
  ```text
  AnalysisException: [CANNOT_MODIFY_STATIC_CONFIG] Cannot modify the value of the static Spark config: "spark.sql.catalogImplementation". SQLSTATE: 46110
  ```
- **Working Solution**: Set the static property globally in `/opt/spark/conf/spark-defaults.conf`:
  ```bash
  echo "spark.sql.catalogImplementation hive" | sudo tee /opt/spark/conf/spark-defaults.conf
  ```
  This enables the embedded Derby metastore (`metastore_db`), allowing table definitions to persist across process restarts.

---

## 5. Pipeline 2: Streaming, Hard Kill, & Recovery (`pipelines/streaming/`)

### Project Setup
- Ingests continuous records via `rate` source into a streaming table (`@dp.table`).
- Loads batch records into a batch table (`@dp.materialized_view`).
- Filters batch records in a downstream materialized view (`@dp.materialized_view`), establishing a real dependency edge.

### Key Operational Findings
1. **Execution Model**: `spark-pipelines run` does not run indefinitely; it executes a triggered graph update using `Trigger.AvailableNow()`. For `rate` source, it processes one micro-batch per run.
2. **Concurrency**: Independent streams (`rate_stream`) and batch inputs (`batch_events`) run concurrently on separate worker threads. Downstream MVs (`high_value_events_mv`) execute sequentially after their upstream parent completes.
3. **Hard Kill (`SIGKILL / kill -9`)**:
   - Terminating the JVM via `kill -9` emits **zero CLI output or shutdown warnings** (exit code 137).
   - Checkpoint integrity is preserved up to the last flushed commit.
4. **Recovery**:
   - On restart, the pipeline resumes strictly from the committed offset (e.g. `offsets/3` at offset 174 -> `offsets/4` at offset 200).
   - Inspection of parquet part files confirmed zero duplicate records and zero reprocessing from offset 0.

---

## 6. Pipeline 3: Flow Type Boundary Rules (`pipelines/boundaries/`)

The following engine rules were empirically verified:
1. **Materialized View over Streaming Source**: Rejected during planning (`[INVALID_FLOW_QUERY_TYPE.STREAMING_RELATION_FOR_MATERIALIZED_VIEW]`, SQLSTATE `42000`).
2. **Streaming Table over Batch Source**: Rejected during planning (`[INVALID_FLOW_QUERY_TYPE.BATCH_RELATION_FOR_STREAMING_TABLE]`, SQLSTATE `42000`).
3. **Materialized View reading Streaming Table's Output Table**: **Supported** (exit code 0). Reading `spark.table("streaming_table")` acts as a batch relation over the underlying storage, bridging the stream-to-batch boundary.
4. **Temporary View between Streaming Source and Materialized View**: Rejected during planning (`[INCOMPATIBLE_STREAMING_VIEW_READ]`, SQLSTATE `42000`). Disabling the check still fails with `STREAMING_RELATION_FOR_MATERIALIZED_VIEW` because temporary views do not materialize physical data.

---

## 7. Pipeline 4: Object Storage & S3A Connector Dependencies

### Required Jars for Hadoop 3.5.0 S3A Support
Place the following jars in **both** `/opt/spark/jars/` and `/home/ubuntu/.local/lib/python3.10/site-packages/pyspark/jars/`:
1. `hadoop-aws-3.5.0.jar`
   - URL: `https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.5.0/hadoop-aws-3.5.0.jar`
2. `bundle-2.35.4.jar` (AWS SDK for Java V2)
   - URL: `https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.35.4/bundle-2.35.4.jar`
3. `analyticsaccelerator-s3-1.3.1.jar`
   - URL: `https://repo1.maven.org/maven2/software/amazon/s3/analyticsaccelerator/analyticsaccelerator-s3/1.3.1/analyticsaccelerator-s3-1.3.1.jar`

### OCI S3 Compatibility Endpoint Rules
- Endpoint format: `https://<namespace>.compat.objectstorage.<region>.oraclecloud.com`
- Addressing style: Path-style access (`fs.s3a.path.style.access=true`).
- Customer Secret Key: Generated under user credentials in OCI IAM.
