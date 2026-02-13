from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(name="gold_airbnb_postcode")
def gold_airbnb_postcode():
    df = dp.read("silver_airbnb_backfilled").where(F.col("pc4").isNotNull())
    return (
        df.groupBy("pc4")
          .agg(
              F.count("*").alias("airbnb_listings"),
              F.avg("price_eur_night").alias("avg_price_eur_night"),
              F.expr("percentile_approx(annual_revenue_est_eur, 0.5)").alias("median_annual_rev_eur"),
              F.avg("annual_revenue_est_eur").alias("avg_annual_rev_eur"),
          )
    )
