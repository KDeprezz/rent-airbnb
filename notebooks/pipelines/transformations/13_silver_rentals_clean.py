from pyspark import pipelines as dp
from pyspark.sql import functions as F

from notebooks.pipelines.utilities.config import get_city_filter
from notebooks.pipelines.utilities.spark_parsing import parse_eur_amount


@dp.materialized_view(name="silver_rentals_clean")
@dp.expect_all_or_drop({
    "property_id_present": "property_id IS NOT NULL",
    "pc4_present": "pc4 IS NOT NULL",
    "rent_parseable": "rent_month_eur IS NOT NULL AND rent_month_eur > 0",
    "lat_present": "latitude IS NOT NULL",
    "lon_present": "longitude IS NOT NULL",
})
def silver_rentals_clean():
    city = get_city_filter(spark)  # noqa: F821
    df = dp.read("bronze_rentals")

    return (
        df.withColumn("property_id", F.element_at(F.col("_id"), 1))
          .withColumn("city_norm", F.lower(F.col("city")))
          .where(F.col("city_norm") == F.lit(city))
          .withColumn("pc4", F.substring(F.col("postalCode").cast("string"), 1, 4))
          .withColumn("latitude", F.col("latitude").cast("double"))
          .withColumn("longitude", F.col("longitude").cast("double"))
          .withColumn("rent_month_eur", parse_eur_amount(F.col("rent")))
          .withColumn("additional_costs_eur", parse_eur_amount(F.col("additionalCostsRaw")))
          .withColumn("utilities_included", F.lower(F.col("rent")).contains("utilities incl"))
          .withColumn("area_sqm", F.regexp_extract(F.col("areaSqm").cast("string"), r"(\d+)", 1).cast("double"))
          .withColumn("annual_revenue_est_eur", F.col("rent_month_eur") * F.lit(12.0))
          .select(
              "property_id",
              "latitude",
              "longitude",
              "pc4",
              "rent_month_eur",
              "additional_costs_eur",
              "utilities_included",
              "area_sqm",
              "furnish",
              "propertyType",
              "annual_revenue_est_eur",
              "_ingest_ts",
          )
    )

