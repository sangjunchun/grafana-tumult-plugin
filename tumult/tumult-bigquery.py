import os

from pyspark.sql import SparkSession

from tmlt.analytics.privacy_budget import PureDPBudget
from tmlt.analytics.query_builder import QueryBuilder
from tmlt.analytics.session import Session

BUCKET = "tumult-warehouse"  # TODO: use unique gcs bucket name
INPUT_TABLE = "tumult-labs.analytics_tutorial.library_members"
OUTPUT_TABLE = "tumult-labs.analytics_tutorial.member_counts"

spark = (
    SparkSession
    .builder
    .config("spark.sql.warehouse.dir", os.path.join("gs://", BUCKET, "/spark-warehouse/"))
    .config("temporaryGcsBucket", BUCKET)
    .getOrCreate()
)

members_df = (
    spark.read.format("bigquery")
    .option("table", INPUT_TABLE)
    .load()
)

session = Session.from_dataframe(
    privacy_budget=PureDPBudget(3),
    source_id="members",
    dataframe=members_df
)

count_query = QueryBuilder("members").count()
total_count = session.evaluate(
    count_query,
    privacy_budget=PureDPBudget(epsilon=1)
)

(
    total_count
    .write.format("bigquery")
    .mode("overwrite")
    .option("table", OUTPUT_TABLE)
    .save()
)
