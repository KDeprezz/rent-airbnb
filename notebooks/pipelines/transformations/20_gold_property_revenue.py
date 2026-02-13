from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(name="gold_property_revenue")
def gold_property_revenue():
    air = dp.read("silver_airbnb_backfilled").select(
        F.lit("airbnb").alias("channel"),
        "property_id",
        "pc4",
        F.col("price_eur_night").alias("unit_price"),
        "annual_revenue_est_eur",
    )
    rent = dp.read("silver_rentals_clean").select(
        F.lit("kamernet").alias("channel"),
        "property_id",
        "pc4",
        F.col("rent_month_eur").alias("unit_price"),
        "annual_revenue_est_eur",
    )
    return air.unionByName(rent, allowMissingColumns=True)
