from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import Window

@dp.materialized_view(name="silver_airbnb_backfilled")
def silver_airbnb_backfilled():
    air = dp.read("silver_airbnb_clean")
    missing = air.where(F.col("pc4").isNull()).select("property_id", "latitude", "longitude")
    centroids = F.broadcast(dp.read("silver_postcodes_centroids"))

    candidates = (
        missing.crossJoin(centroids)
        .withColumn(
            "dist2",
            F.pow(F.col("longitude") - F.col("centroid_lon"), 2)
            + F.pow(F.col("latitude") - F.col("centroid_lat"), 2),
        )
    )
    w = Window.partitionBy("property_id").orderBy(F.col("dist2").asc())
    imputed = (
        candidates.withColumn("rn", F.row_number().over(w))
        .where("rn = 1")
        .select("property_id", F.col("pc4").alias("pc4_imputed"))
    )

    return (
        air.join(imputed, on="property_id", how="left")
           .withColumn("pc4_final", F.coalesce("pc4", "pc4_imputed"))
           .withColumn("pc4_source", F.when(F.col("pc4").isNotNull(), F.lit("original")).otherwise(F.lit("geo_centroid_nn")))
           .drop("pc4", "pc4_imputed")
           .withColumnRenamed("pc4_final", "pc4")
    )
