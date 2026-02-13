def get_input_root(spark):
    return spark.conf.get("input_root")

def get_output_root(spark):
    return spark.conf.get("output_root")

def get_city_filter(spark):
    return spark.conf.get("city_filter", "amsterdam")

def get_occupancy_rate(spark):
    return float(spark.conf.get("occupancy_rate", "0.65"))

def get_days_per_year(spark):
    return float(spark.conf.get("days_per_year", "365"))
