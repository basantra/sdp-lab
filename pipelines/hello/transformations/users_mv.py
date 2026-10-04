import os
from pyspark import pipelines as dp
from pyspark.sql import DataFrame

DATA_FILE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "users.json"))

@dp.materialized_view(
    name="top_users_mv",
    comment="Materialized view filtering top scoring users"
)
def top_users_mv() -> DataFrame:
    return spark.read.json(f"file://{DATA_FILE}").filter("score >= 90")
