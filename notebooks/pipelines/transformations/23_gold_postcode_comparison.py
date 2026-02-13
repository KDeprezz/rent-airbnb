from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(name="gold_postcode_comparison")
def gold_postcode_comparison():
    # Rename metrics BEFORE join to avoid duplicate column names
    a = (
        dp.read("gold_airbnb_postcode")
        .select(
            "pc4",
            F.col("airbnb_listings"),
            F.col("avg_price_eur_night"),
            F.col("median_annual_rev_eur").alias("airbnb_median_annual_rev_eur"),
            F.col("avg_annual_rev_eur").alias("airbnb_avg_annual_rev_eur"),
        )
    )

    r = (
        dp.read("gold_rentals_postcode")
        .select(
            "pc4",
            F.col("rental_listings"),
            F.col("median_annual_rev_eur").alias("rentals_median_annual_rev_eur"),
            F.col("avg_annual_rev_eur").alias("rentals_avg_annual_rev_eur"),
        )
    )

    joined = a.join(r, on="pc4", how="outer")

    # Fill only COUNTS with 0. Keep revenue metrics NULL (unknown when missing).
    joined = joined.fillna({"airbnb_listings": 0, "rental_listings": 0})

    # Helpful “coverage” label for review + debugging
    joined = joined.withColumn(
        "data_coverage",
        F.when(
            (F.col("airbnb_listings") > 0) & (F.col("rental_listings") > 0),
            F.lit("both"),
        )
        .when((F.col("airbnb_listings") > 0) & (F.col("rental_listings") == 0), F.lit("airbnb_only"))
        .when((F.col("airbnb_listings") == 0) & (F.col("rental_listings") > 0), F.lit("rentals_only"))
        .otherwise(F.lit("neither")),
    )

    # Only compute delta when both revenue metrics exist
    joined = joined.withColumn(
        "delta_avg_annual_rev_eur",
        F.when(
            F.col("airbnb_avg_annual_rev_eur").isNotNull()
            & F.col("rentals_avg_annual_rev_eur").isNotNull(),
            F.col("airbnb_avg_annual_rev_eur") - F.col("rentals_avg_annual_rev_eur"),
        ),
    )

    # Recommendation that doesn’t lie when one side is missing
    joined = joined.withColumn(
        "recommended_strategy",
        F.when(F.col("data_coverage") == "airbnb_only", F.lit("Airbnb only (no rentals data)"))
        .when(F.col("data_coverage") == "rentals_only", F.lit("Long-term only (no Airbnb data)"))
        .when(F.col("data_coverage") == "neither", F.lit("Insufficient data"))
        .when(F.col("delta_avg_annual_rev_eur") > 0, F.lit("Airbnb (gross)"))
        .otherwise(F.lit("Long-term rent (gross)")),
    )

    return joined
