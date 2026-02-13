from pyspark import pipelines as dp
from pyspark.sql import functions as F

from notebooks.pipelines.utilities.config import get_occupancy_rate, get_days_per_year
from notebooks.pipelines.utilities.spark_parsing import extract_pc4


@dp.materialized_view(name="silver_airbnb_clean")
@dp.expect_all_or_drop({
    # IMPORTANT: expectations must reference OUTPUT columns
    "price_ok": "price_eur_night IS NOT NULL AND price_eur_night > 0 AND price_eur_night <= 5000",
    "lat_present": "latitude IS NOT NULL",
    "lon_present": "longitude IS NOT NULL",
})
def silver_airbnb_clean():
    occ = get_occupancy_rate(spark)  # noqa: F821
    dpy = get_days_per_year(spark)   # noqa: F821

    df = dp.read("bronze_airbnb")

    # Create a stable-ish property_id (hash of record content)
    fingerprint = F.sha2(
        F.concat_ws(
            "||",
            F.col("zipcode").cast("string"),
            F.col("latitude").cast("string"),
            F.col("longitude").cast("string"),
            F.col("room_type").cast("string"),
            F.col("accommodates").cast("string"),
            F.col("bedrooms").cast("string"),
            F.col("price").cast("string"),
            F.col("review_scores_value").cast("string"),
        ),
        256,
    )

    return (
        df.withColumn("property_id", fingerprint)
          .withColumn("pc4", extract_pc4(F.col("zipcode")))
          .withColumn("latitude", F.col("latitude").cast("double"))
          .withColumn("longitude", F.col("longitude").cast("double"))
          .withColumn("price_eur_night", F.col("price").cast("double"))
          .withColumn(
              "annual_revenue_est_eur",
              F.col("price").cast("double") * F.lit(occ) * F.lit(dpy)
          )
          .select(
              "property_id",
              "latitude",
              "longitude",
              "pc4",
              "price_eur_night",
              "annual_revenue_est_eur",
              F.col("review_scores_value").cast("double").alias("review_scores_value"),
              F.col("bedrooms").cast("double").alias("bedrooms"),
              F.col("accommodates").cast("double").alias("accommodates"),
              "_ingest_ts",
          )
    )
