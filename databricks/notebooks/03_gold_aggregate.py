# Databricks notebook — Gold Layer
# Produces two aggregated fact tables from silver Yellow Taxi data:
#   fct_trips_hourly  — trip volume, fares, and distances by pickup hour and zone
#   fct_revenue_daily — daily revenue and trip counts by pickup zone
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

silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/delta/yellow_taxi/"
gold_base = f"abfss://gold@{storage_account}.dfs.core.windows.net/delta"

silver = (
    spark.read.format("delta")
    .load(silver_path)
    .filter(F.col("year") == year)
    .filter(F.col("month") == month)
)

# COMMAND ----------
# fct_trips_hourly

fct_trips_hourly = (
    silver
    .withColumn("pickup_hour", F.date_trunc("hour", "pickup_datetime"))
    .groupBy("pickup_hour", "pickup_location_id", "year", "month")
    .agg(
        F.count("*").alias("trip_count"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("trip_distance_miles"), 2).alias("avg_distance_miles"),
        F.round(F.avg("trip_duration_minutes"), 1).alias("avg_duration_minutes"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.round(F.avg("tip_amount"), 2).alias("avg_tip"),
    )
)

(
    fct_trips_hourly
    .write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"year = {year} AND month = {month}")
    .partitionBy("year", "month")
    .save(f"{gold_base}/fct_trips_hourly/")
)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS gold.fct_trips_hourly
    USING DELTA
    LOCATION '{gold_base}/fct_trips_hourly/'
""")

print(f"fct_trips_hourly: {fct_trips_hourly.count():,} rows")

# COMMAND ----------
# fct_revenue_daily

fct_revenue_daily = (
    silver
    .withColumn("pickup_date", F.to_date("pickup_datetime"))
    .groupBy("pickup_date", "pickup_location_id", "year", "month")
    .agg(
        F.count("*").alias("trip_count"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.round(F.sum("fare_amount"), 2).alias("total_fare"),
        F.round(F.sum("tip_amount"), 2).alias("total_tips"),
        F.round(F.avg("trip_duration_minutes"), 1).alias("avg_duration_minutes"),
        F.countDistinct("pickup_location_id").alias("active_zones"),
    )
)

(
    fct_revenue_daily
    .write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"year = {year} AND month = {month}")
    .partitionBy("year", "month")
    .save(f"{gold_base}/fct_revenue_daily/")
)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS gold.fct_revenue_daily
    USING DELTA
    LOCATION '{gold_base}/fct_revenue_daily/'
""")

print(f"fct_revenue_daily: {fct_revenue_daily.count():,} rows")
print(f"Gold aggregation complete: year={year}, month={month}")
