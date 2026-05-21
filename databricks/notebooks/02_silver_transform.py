# Databricks notebook — Silver Layer
# Cleans, types, and validates the bronze Yellow Taxi data.
# Filters outliers, coalesces nulls, enforces schema.
#
# Parameters:
#   storage_account, year, month

# COMMAND ----------

dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("year", "")
dbutils.widgets.text("month", "")

storage_account = dbutils.widgets.get("storage_account")
year = int(dbutils.widgets.get("year"))
month = int(dbutils.widgets.get("month"))

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType

tenant_id = dbutils.secrets.get(scope="adls", key="tenant-id")
client_id = dbutils.secrets.get(scope="adls", key="client-id")
client_secret = dbutils.secrets.get(scope="adls", key="client-secret")

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
               f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/delta/yellow_taxi/"
silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/delta/yellow_taxi/"

df = (
    spark.read.format("delta")
    .load(bronze_path)
    .filter(F.col("year") == year)
    .filter(F.col("month") == month)
)

print(f"Bronze rows: {df.count():,}")

# COMMAND ----------

# Type-cast and rename to snake_case
df_typed = (
    df
    .select(
        F.col("VendorID").cast(IntegerType()).alias("vendor_id"),
        F.col("tpep_pickup_datetime").cast(TimestampType()).alias("pickup_datetime"),
        F.col("tpep_dropoff_datetime").cast(TimestampType()).alias("dropoff_datetime"),
        F.col("passenger_count").cast(IntegerType()).alias("passenger_count"),
        F.col("trip_distance").cast(DoubleType()).alias("trip_distance_miles"),
        F.col("PULocationID").cast(IntegerType()).alias("pickup_location_id"),
        F.col("DOLocationID").cast(IntegerType()).alias("dropoff_location_id"),
        F.col("RatecodeID").cast(IntegerType()).alias("rate_code_id"),
        F.col("fare_amount").cast(DoubleType()).alias("fare_amount"),
        F.col("tip_amount").cast(DoubleType()).alias("tip_amount"),
        F.col("tolls_amount").cast(DoubleType()).alias("tolls_amount"),
        F.col("total_amount").cast(DoubleType()).alias("total_amount"),
        F.col("payment_type").cast(IntegerType()).alias("payment_type"),
    )
)

# COMMAND ----------

# Quality filters: drop obvious outliers and nulls
df_clean = (
    df_typed
    .dropna(subset=["pickup_datetime", "dropoff_datetime", "total_amount"])
    .filter(F.col("trip_distance_miles") > 0)
    .filter(F.col("trip_distance_miles") < 200)
    .filter(F.col("fare_amount") > 0)
    .filter(F.col("total_amount") > 0)
    .filter(F.col("total_amount") < 1000)
    .filter(F.col("pickup_datetime") < F.col("dropoff_datetime"))
    # Keep only trips within the expected month
    .filter(F.year("pickup_datetime") == year)
    .filter(F.month("pickup_datetime") == month)
    # Derived columns
    .withColumn(
        "trip_duration_minutes",
        (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60,
    )
    .filter(F.col("trip_duration_minutes") > 0)
    .filter(F.col("trip_duration_minutes") < 300)
    .withColumn("year", F.lit(year))
    .withColumn("month", F.lit(month))
)

rows_in = df.count()
rows_out = df_clean.count()
print(f"Silver rows: {rows_out:,} ({100 * rows_out / rows_in:.1f}% retained after quality filters)")

# COMMAND ----------

(
    df_clean
    .write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"year = {year} AND month = {month}")
    .partitionBy("year", "month")
    .save(silver_path)
)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS silver.yellow_taxi
    USING DELTA
    LOCATION '{silver_path}'
""")

print(f"Silver Delta written: year={year}, month={month}")
