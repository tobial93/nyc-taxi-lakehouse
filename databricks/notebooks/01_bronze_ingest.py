# Databricks notebook — Bronze Layer
# Registers raw Parquet files uploaded by Airflow as a Delta table.
#
# Parameters (passed by Airflow DatabricksRunNowOperator):
#   storage_account : str  — ADLS Gen2 account name
#   year            : str  — e.g. "2024"
#   month           : str  — e.g. "01"

# COMMAND ----------

dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("year", "")
dbutils.widgets.text("month", "")

storage_account = dbutils.widgets.get("storage_account")
year = dbutils.widgets.get("year")
month = dbutils.widgets.get("month")

# COMMAND ----------

# Configure ADLS Gen2 access via service principal stored in Databricks secrets
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

raw_path = (
    f"abfss://bronze@{storage_account}.dfs.core.windows.net"
    f"/raw/yellow_taxi/{year}/{month}/"
    f"yellow_tripdata_{year}-{month}.parquet"
)
bronze_delta_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/delta/yellow_taxi/"

df = spark.read.parquet(raw_path)
print(f"Loaded {df.count():,} rows from {raw_path}")

# COMMAND ----------

# Write as Delta with year/month partitioning
(
    df
    .withColumn("year", lit(int(year)).cast("integer"))
    .withColumn("month", lit(int(month)).cast("integer"))
    .write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"year = {year} AND month = {month}")
    .partitionBy("year", "month")
    .save(bronze_delta_path)
)

print(f"Bronze Delta written: year={year}, month={month}")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS bronze.yellow_taxi
    USING DELTA
    LOCATION '{bronze_delta_path}'
""")
spark.sql("MSCK REPAIR TABLE bronze.yellow_taxi")
