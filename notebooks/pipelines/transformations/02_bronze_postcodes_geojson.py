from pyspark import pipelines as dp
from notebooks.pipelines.utilities.config import get_input_root

@dp.materialized_view(name="bronze_postcodes_geojson")
def bronze_postcodes_geojson():
    input_root = get_input_root(spark)  # noqa: F821
    return (
        spark.read.format("json")  # noqa: F821
        .option("multiLine", True)
        .load(f"{input_root}/geo/post_codes.geojson")
    )
