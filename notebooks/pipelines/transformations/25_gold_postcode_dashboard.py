from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(name="gold_postcode_dashboard")
def gold_postcode_dashboard():
    comparison = dp.read("gold_postcode_comparison")

    geo = dp.read("silver_postcodes_centroids")

    # Handle either naming convention coming from your geo-centroid dataset
    if "centroid_lat" in geo.columns and "centroid_lon" in geo.columns:
        geo = geo.select(
            "pc4",
            F.col("centroid_lat").cast("double").alias("latitude"),
            F.col("centroid_lon").cast("double").alias("longitude"),
        )
    elif "latitude" in geo.columns and "longitude" in geo.columns:
        geo = geo.select(
            "pc4",
            F.col("latitude").cast("double").alias("latitude"),
            F.col("longitude").cast("double").alias("longitude"),
        )
    else:
        # Fail fast with a clear error message in pipeline logs
        raise ValueError(
            "silver_postcodes_centroids must contain either (centroid_lat, centroid_lon) or (latitude, longitude)."
        )

    return (
        comparison.join(geo, on="pc4", how="left")
        .select(
            "pc4",
            "latitude",
            "longitude",
            "airbnb_listings",
            "rental_listings",
            "airbnb_avg_annual_rev_eur",
            "rentals_avg_annual_rev_eur",
            "delta_avg_annual_rev_eur",
            "recommended_strategy",
            "data_coverage",
        )
    )
