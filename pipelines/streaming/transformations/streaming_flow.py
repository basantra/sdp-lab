from pyspark import pipelines as dp
from pyspark.sql import DataFrame

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
