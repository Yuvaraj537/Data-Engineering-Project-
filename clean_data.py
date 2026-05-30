from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, count, when, isnan
from pyspark.sql.types import DoubleType, LongType

spark = SparkSession.builder \
    .appName("Crypto_Bronze_Cleaning") \
    .getOrCreate()

INPUT_PATH  = "gs://ps-gcp-ingress-bucket-7f3a/provider_ingress/raw/crypto_10000_records.csv"
OUTPUT_PATH = "gs://ps-gcp-ingress-bucket-7f3a/provider-ingress/bronze/crypto_bronze_save"

# ── 1. READ ──────────────────────────────────────────────────────────────────
print("\n========== STEP 1: READ RAW DATA ==========")
df = (spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("multiLine", "true")
      .option("escape", '"')
      .csv(INPUT_PATH))

raw_count = df.count()
print(f"Raw record count : {raw_count}")
df.printSchema()

# ── 2. REMOVE DUPLICATES ─────────────────────────────────────────────────────
print("\n========== STEP 2: REMOVE DUPLICATES ==========")
df_dedup = df.dropDuplicates()
dup_removed = raw_count - df_dedup.count()
print(f"Duplicates removed   : {dup_removed}")
print(f"After dedup count    : {df_dedup.count()}")

# ── 3. REMOVE NULLS ──────────────────────────────────────────────────────────
print("\n========== STEP 3: REMOVE NULL VALUES ==========")
critical_cols = [
    "id", "symbol", "name",
    "current_price", "market_cap",
    "market_cap_rank", "total_volume"
]
df_no_null = df_dedup.dropna(subset=critical_cols)
null_removed = df_dedup.count() - df_no_null.count()
print(f"Null rows removed    : {null_removed}")
print(f"After null removal   : {df_no_null.count()}")

# ── 4. REMOVE INVALID VALUES ─────────────────────────────────────────────────
print("\n========== STEP 4: REMOVE INVALID VALUES ==========")
df_valid = df_no_null.filter(
    (col("current_price")  > 0)  &
    (col("market_cap")     > 0)  &
    (col("market_cap_rank")> 0)  &
    (col("total_volume")   >= 0) &
    (col("high_24h")       >= 0) &
    (col("low_24h")        >= 0) &
    (trim(col("id"))       != "") &
    (trim(col("symbol"))   != "") &
    (trim(col("name"))     != "")
)
invalid_removed = df_no_null.count() - df_valid.count()
print(f"Invalid rows removed : {invalid_removed}")
print(f"After invalid removal: {df_valid.count()}")

# ── 5. CAST CORRECT DATA TYPES ───────────────────────────────────────────────
print("\n========== STEP 5: CAST DATA TYPES ==========")
df_typed = df_valid \
    .withColumn("current_price",              col("current_price").cast(DoubleType())) \
    .withColumn("market_cap",                 col("market_cap").cast(DoubleType())) \
    .withColumn("market_cap_rank",            col("market_cap_rank").cast(LongType())) \
    .withColumn("total_volume",               col("total_volume").cast(DoubleType())) \
    .withColumn("high_24h",                   col("high_24h").cast(DoubleType())) \
    .withColumn("low_24h",                    col("low_24h").cast(DoubleType())) \
    .withColumn("price_change_24h",           col("price_change_24h").cast(DoubleType())) \
    .withColumn("price_change_percentage_24h",col("price_change_percentage_24h").cast(DoubleType())) \
    .withColumn("circulating_supply",         col("circulating_supply").cast(DoubleType())) \
    .withColumn("total_supply",               col("total_supply").cast(DoubleType())) \
    .withColumn("max_supply",                 col("max_supply").cast(DoubleType())) \
    .withColumn("ath",                        col("ath").cast(DoubleType())) \
    .withColumn("atl",                        col("atl").cast(DoubleType()))

print("Data types cast successfully")
df_typed.printSchema()

# ── 6. SUMMARY REPORT ────────────────────────────────────────────────────────
print("\n========== CLEANING SUMMARY ==========")
final_count = df_typed.count()
print(f"Raw records          : {raw_count}")
print(f"Duplicates removed   : {dup_removed}")
print(f"Null rows removed    : {null_removed}")
print(f"Invalid rows removed : {invalid_removed}")
print(f"Final clean records  : {final_count}")
print(f"Total removed        : {raw_count - final_count}")
print("\nSample cleaned data:")
df_typed.show(5, truncate=False)

# ── 7. SAVE TO BRONZE ────────────────────────────────────────────────────────
print("\n========== STEP 7: SAVE TO BRONZE ==========")
df_typed.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(OUTPUT_PATH)

print(f"Saved to: {OUTPUT_PATH}")
print("========== DONE ==========")

spark.stop()
