from pyspark import pipelines as dp
from pyspark.sql import functions as F
from notebooks.pipelines.utilities.config import get_input_root

@dp.materialized_view(name="bronze_airbnb")
def bronze_airbnb():
    input_root = get_input_root(spark)  # noqa: F821
    return (
        spark.read.format("csv")  # noqa: F821
        .option("header", True)
        .option("inferSchema", True)
        .load(f"{input_root}/airbnb.csv")
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source", F.lit("airbnb"))
    )
