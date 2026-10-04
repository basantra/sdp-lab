import os
from pyspark import pipelines as dp
from pyspark.sql import DataFrame

DATA_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "batch_data.json"))

@dp.materialized_view(
    name="batch_events",
    comment="Batch table loaded from JSON dataset"
)
def batch_events() -> DataFrame:
    return spark.read.json(f"file://{DATA_PATH}")

@dp.materialized_view(
    name="high_value_events_mv",
    comment="Materialized view downstream of batch_events"
)
def high_value_events_mv() -> DataFrame:
    return spark.table("batch_events").filter("value >= 50")
