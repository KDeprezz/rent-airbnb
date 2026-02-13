from pyspark.sql import functions as F

def extract_pc4(col):
    raw = F.regexp_extract(F.upper(col.cast("string")), r"(\d{4})", 1)
    return F.when((raw == "") | raw.isNull(), F.lit(None)).otherwise(raw)

def parse_eur_amount(col):
    s = F.upper(F.trim(col.cast("string")))
    s = F.when((s.isNull()) | (F.trim(s) == "") | (F.trim(s) == "-"), F.lit(None)).otherwise(s)
    s = F.regexp_replace(s, r"[^0-9\.,]", "")
    s = F.regexp_replace(s, ",", ".")
    s = F.regexp_replace(s, r"\.+", ".")
    token = F.regexp_extract(s, r"(\d+(?:\.\d+)?)", 1)
    return F.when(token == "", F.lit(None)).otherwise(token.cast("double"))
