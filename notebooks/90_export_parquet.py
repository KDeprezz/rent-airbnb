import os
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Pipeline config (set in pipeline settings); fallback path for safety
OUTPUT_ROOT = os.getenv("OUTPUT_ROOT", "/Volumes/workspace/rent_airbnb/invest_output").rstrip("/")

tables = [
    "workspace.rent_airbnb.gold_postcode_comparison",
    "workspace.rent_airbnb.gold_airbnb_postcode",
    "workspace.rent_airbnb.gold_rentals_postcode",
    "workspace.rent_airbnb.gold_property_revenue",
    "workspace.rent_airbnb.gold_airbnb_backfill_stats",
    "workspace.rent_airbnb.gold_postcode_dashboard",
]

for t in tables:
    name = t.split(".")[-1]
    df = spark.table(t)

    # Parquet export
    out_path = f"{OUTPUT_ROOT}/{name}"
    df.write.mode("overwrite").parquet(out_path)

    print(f"✅ Wrote {t} -> {out_path} (rows={df.count()})")
