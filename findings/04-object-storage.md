# Finding 04: OCI Object Storage Ingestion via S3A Connector

## 1. Executive Summary

This finding documents connecting Spark Declarative Pipelines (SDP) on Apache Spark 4.2.0 to an Oracle Cloud Infrastructure (OCI) Object Storage bucket (`sdp-lab-data`) using OCI's S3 Compatibility API and Hadoop's `s3a://` connector.

A streaming table (`@dp.table`) ingested JSON files from `s3a://sdp-lab-data/input/`. We verified incremental processing across two pipeline runs: Run 1 ingested an initial 2-file batch, and Run 2 ingested 2 newly landed files.

---

## 2. Jar Dependency Matrix & Classpath Resolution Friction

Integrating Spark 4.2.0 with OCI S3 Compatibility API requires specific versions of Hadoop AWS connectors and AWS SDKs. Spark 4.2.0 bundles `hadoop-client-api-3.5.0.jar` and `hadoop-client-runtime-3.5.0.jar`.

### Exact Jars Deployed

| Jar Name | Version | Download URL | Justification |
| :--- | :--- | :--- | :--- |
| **`hadoop-aws`** | `3.5.0` | `https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.5.0/hadoop-aws-3.5.0.jar` | Matches Hadoop runtime version (3.5.0) bundled in Spark 4.2.0. Provides `org.apache.hadoop.fs.s3a.S3AFileSystem`. |
| **`bundle` (AWS Java SDK v2)** | `2.35.4` | `https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.35.4/bundle-2.35.4.jar` | Hadoop 3.5.0 migrated `hadoop-aws` from AWS SDK v1 to AWS SDK v2 (`software.amazon.awssdk.*`). The single shaded bundle jar provides all AWS SDK v2 classes. |
| **`analyticsaccelerator-s3`** | `1.3.1` | `https://repo1.maven.org/maven2/software/amazon/s3/analyticsaccelerator/analyticsaccelerator-s3/1.3.1/analyticsaccelerator-s3-1.3.1.jar` | Required runtime dependency of `hadoop-aws-3.5.0` for S3 request streaming optimizations. Without it, S3A fails with `NoClassDefFoundError: software/amazon/s3/analyticsaccelerator/S3SeekableInputStreamConfiguration`. |

### Critical Friction: PySpark Pip Classpath vs `$SPARK_HOME/jars`

Placing third-party jars exclusively in `/opt/spark/jars/` failed. Because `pyspark[pipelines]` was installed via `pip` into user site-packages (`/home/ubuntu/.local/lib/python3.10/site-packages/pyspark/`), the `spark-pipelines` launcher constructed its Java classpath using `/home/ubuntu/.local/lib/python3.10/site-packages/pyspark/jars/`.

**Resolution**: Jars must be copied directly into the PySpark site-packages jar folder:
```bash
sudo cp /opt/spark/jars/hadoop-aws-3.5.0.jar \
        /opt/spark/jars/bundle-2.35.4.jar \
        /opt/spark/jars/analyticsaccelerator-s3-1.3.1.jar \
        /home/ubuntu/.local/lib/python3.10/site-packages/pyspark/jars/
```

---

## 3. OCI Object Storage S3 Compatibility Configuration

OCI Object Storage requires path-style access and explicit regional endpoint configuration. In `/opt/spark/conf/spark-defaults.conf`:

```properties
spark.hadoop.fs.s3a.endpoint https://ax7ktsgjgveu.compat.objectstorage.us-chicago-1.oraclecloud.com
spark.hadoop.fs.s3a.endpoint.region us-chicago-1
spark.hadoop.fs.s3a.path.style.access true
spark.hadoop.fs.s3a.connection.ssl.enabled true
```

> [!IMPORTANT]
> Without `spark.hadoop.fs.s3a.endpoint.region us-chicago-1`, AWS SDK v2 returns HTTP `400 Bad Request` (`software.amazon.awssdk.services.s3.model.S3Exception`). OCI requires the signing region in the SigV4 authorization header to match the tenancy region.

### Credential Handling

To satisfy the zero-secret repository security policy, Customer Secret Key credentials were never written to any file within the repository. They were passed dynamically into the execution environment via standard AWS SDK environment variables:
- `AWS_ACCESS_KEY_ID`: OCI Customer Secret Key Access Key ID
- `AWS_SECRET_ACCESS_KEY`: OCI Customer Secret Key Generated Secret

---

## 4. Pipeline Definition

### Pipeline Spec (`pipelines/object_storage/spark-pipeline.yml`)
```yaml
name: object_storage_project
storage: file:///home/ubuntu/sdp-lab/pipelines/object_storage/storage
libraries:
  - glob:
      include: transformations/**
```

### Flow Definition (`pipelines/object_storage/transformations/events_stream.py`)
```python
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("event_type", StringType(), True),
    StructField("timestamp", StringType(), True)
])

@dp.table(
    name="s3_events",
    comment="Streaming table ingesting events from OCI Object Storage via S3A"
)
def s3_events() -> DataFrame:
    return (
        spark.readStream
        .schema(schema)
        .json("s3a://sdp-lab-data/input/")
    )
```

---

## 5. Execution Telemetry and Empirical Verification

### Batch 1 (Run 1)
- Files landed in `s3a://sdp-lab-data/input/`:
  - `file1.json` (rows: `id=1`, `id=2`)
  - `file2.json` (rows: `id=3`, `id=4`)
- Command: `spark-pipelines run` (duration: 21.21s, exit code: 0)
- Source Checkpoint (`storage/.../sources/0/0`):
  ```json
  v1
  {"path":"s3a://sdp-lab-data/input/file1.json","timestamp":1788814486000,"batchId":0}
  {"path":"s3a://sdp-lab-data/input/file2.json","timestamp":1788814487000,"batchId":0}
  ```
- Output Metadata (`spark-warehouse/s3_events/_spark_metadata/0`):
  Added part file `part-00000-4b01c8aa-defd-411f-870e-7acb3d3a4068-c000.snappy.parquet` (1,091 bytes, 4 rows).

### Batch 2 (Run 2)
- Two additional files landed in `s3a://sdp-lab-data/input/`:
  - `file3.json` (rows: `id=5`, `id=6`)
  - `file4.json` (rows: `id=7`, `id=8`)
- Command: `spark-pipelines run` (duration: 20.24s, exit code: 0)
- Source Checkpoint (`storage/.../sources/0/1`):
  ```json
  v1
  {"path":"s3a://sdp-lab-data/input/file3.json","timestamp":1788814556000,"batchId":1}
  {"path":"s3a://sdp-lab-data/input/file4.json","timestamp":1788814557000,"batchId":1}
  ```
- Output Metadata (`spark-warehouse/s3_events/_spark_metadata/1`):
  Added part file `part-00000-b66d6bff-7fec-42e4-b6b6-774973d00d76-c000.snappy.parquet` (1,118 bytes, 4 rows).

### Cumulative Verification
Inspection of both Parquet part files confirmed zero duplicate rows:
- `part-00000-4b01c8aa...`: IDs `[1, 2, 3, 4]`
- `part-00000-b66d6bff...`: IDs `[5, 6, 7, 8]`
- Total table rows: 8.
