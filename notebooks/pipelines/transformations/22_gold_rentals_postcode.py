from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(name="gold_rentals_postcode")
def gold_rentals_postcode():
    df = dp.read("silver_rentals_clean").where(F.col("pc4").isNotNull())
    return (
        df.groupBy("pc4")
          .agg(
              F.count("*").alias("rental_listings"),
              F.expr("percentile_approx(annual_revenue_est_eur, 0.5)").alias("median_annual_rev_eur"),
              F.avg("annual_revenue_est_eur").alias("avg_annual_rev_eur"),
          )
    )
