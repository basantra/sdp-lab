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
