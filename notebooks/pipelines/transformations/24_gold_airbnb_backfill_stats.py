from pyspark import pipelines as dp

@dp.materialized_view(name="gold_airbnb_backfill_stats")
def gold_airbnb_backfill_stats():
    df = dp.read("silver_airbnb_backfilled")
    return df.groupBy("pc4_source").count()
