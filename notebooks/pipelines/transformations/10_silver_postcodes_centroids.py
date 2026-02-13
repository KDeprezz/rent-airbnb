from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(name="silver_postcodes_centroids")
def silver_postcodes_centroids():
    features = dp.read("bronze_postcodes_geojson").select(F.explode("features").alias("f"))

    return (
        features.select(
            F.col("f.properties.pc4_code").cast("string").alias("pc4"),
            F.col("f.properties.geo_point_2d.lon").cast("double").alias("centroid_lon"),
            F.col("f.properties.geo_point_2d.lat").cast("double").alias("centroid_lat"),
        )
        .where(
            F.col("pc4").isNotNull()
            & F.col("centroid_lon").isNotNull()
            & F.col("centroid_lat").isNotNull()
        )
        .dropDuplicates(["pc4"])
    )
