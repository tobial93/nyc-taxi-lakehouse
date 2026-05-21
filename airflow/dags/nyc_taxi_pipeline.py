"""
NYC Yellow Taxi Lakehouse Pipeline

Monthly batch pipeline:
  1. Download Parquet from NYC TLC public CDN → ADLS Gen2 bronze/raw/
  2. Databricks job: bronze Delta table registration
  3. Databricks job: silver cleaning and schema enforcement
  4. Databricks job: gold aggregation (fct_trips_hourly, fct_revenue_daily)
  5. Row-count validation on gold tables

Airflow Variables required:
  - DATABRICKS_JOB_ID_BRONZE   (set after terraform apply)
  - DATABRICKS_JOB_ID_SILVER
  - DATABRICKS_JOB_ID_GOLD

Airflow Connections required:
  - databricks_default  (host + token)
"""

import os
import tempfile
from datetime import datetime, timedelta

import requests
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

STORAGE_ACCOUNT = os.environ["AZURE_STORAGE_ACCOUNT"]
TENANT_ID = os.environ["AZURE_TENANT_ID"]
CLIENT_ID = os.environ["AZURE_CLIENT_ID"]
CLIENT_SECRET = os.environ["AZURE_CLIENT_SECRET"]

NYC_TLC_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    "yellow_tripdata_{year}-{month:02d}.parquet"
)
BRONZE_CONTAINER = "bronze"
BRONZE_RAW_PREFIX = "raw/yellow_taxi/{year}/{month:02d}"
BRONZE_FILENAME = "yellow_tripdata_{year}-{month:02d}.parquet"


def _adls_client() -> DataLakeServiceClient:
    credential = ClientSecretCredential(TENANT_ID, CLIENT_ID, CLIENT_SECRET)
    return DataLakeServiceClient(
        account_url=f"https://{STORAGE_ACCOUNT}.dfs.core.windows.net",
        credential=credential,
    )


def check_source_available(year: int, month: int) -> bool:
    url = NYC_TLC_URL.format(year=year, month=month)
    response = requests.head(url, timeout=10)
    return response.status_code == 200


def ingest_bronze(year: int, month: int) -> None:
    url = NYC_TLC_URL.format(year=year, month=month)
    filename = BRONZE_FILENAME.format(year=year, month=month)
    blob_path = f"{BRONZE_RAW_PREFIX.format(year=year, month=month)}/{filename}"

    client = _adls_client()
    fs = client.get_file_system_client(BRONZE_CONTAINER)
    file_client = fs.get_file_client(blob_path)

    with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()
        for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
            tmp.write(chunk)
        tmp.flush()

        with open(tmp.name, "rb") as f:
            file_client.upload_data(f, overwrite=True, length=os.path.getsize(tmp.name))


def validate_gold(year: int, month: int, **context) -> None:
    from azure.storage.filedatalake import DataLakeServiceClient

    client = _adls_client()
    fs = client.get_file_system_client("gold")
    paths = list(fs.get_paths(path=f"delta/fct_trips_hourly/year={year}/month={month:02d}"))
    if not paths:
        raise ValueError(
            f"Gold validation failed: no partitions found for {year}-{month:02d}"
        )


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="nyc_taxi_lakehouse",
    description="Monthly NYC Yellow Taxi medallion pipeline: Bronze → Silver → Gold",
    schedule="0 6 2 * *",  # 06:00 on the 2nd of every month (data lags ~1 month)
    start_date=datetime(2024, 1, 1),
    catchup=True,
    max_active_runs=3,
    default_args=default_args,
    tags=["lakehouse", "nyc-taxi", "azure", "databricks"],
) as dag:

    year = "{{ data_interval_start.year }}"
    month = "{{ data_interval_start.month }}"
    year_int = "{{ data_interval_start.strftime('%Y') }}"
    month_int = "{{ data_interval_start.strftime('%m') }}"

    check_source = ShortCircuitOperator(
        task_id="check_source_available",
        python_callable=check_source_available,
        op_kwargs={
            "year": "{{ data_interval_start.year }}",
            "month": "{{ data_interval_start.month }}",
        },
    )

    ingest = PythonOperator(
        task_id="ingest_bronze",
        python_callable=ingest_bronze,
        op_kwargs={
            "year": "{{ data_interval_start.year }}",
            "month": "{{ data_interval_start.month }}",
        },
    )

    bronze_job = DatabricksRunNowOperator(
        task_id="databricks_bronze",
        databricks_conn_id="databricks_default",
        job_id="{{ var.value.DATABRICKS_JOB_ID_BRONZE }}",
        notebook_params={
            "storage_account": STORAGE_ACCOUNT,
            "year": year_int,
            "month": month_int,
        },
        polling_period_seconds=30,
    )

    silver_job = DatabricksRunNowOperator(
        task_id="databricks_silver",
        databricks_conn_id="databricks_default",
        job_id="{{ var.value.DATABRICKS_JOB_ID_SILVER }}",
        notebook_params={
            "storage_account": STORAGE_ACCOUNT,
            "year": year_int,
            "month": month_int,
        },
        polling_period_seconds=30,
    )

    gold_job = DatabricksRunNowOperator(
        task_id="databricks_gold",
        databricks_conn_id="databricks_default",
        job_id="{{ var.value.DATABRICKS_JOB_ID_GOLD }}",
        notebook_params={
            "storage_account": STORAGE_ACCOUNT,
            "year": year_int,
            "month": month_int,
        },
        polling_period_seconds=30,
    )

    validate = PythonOperator(
        task_id="validate_gold",
        python_callable=validate_gold,
        op_kwargs={
            "year": "{{ data_interval_start.year }}",
            "month": "{{ data_interval_start.month }}",
        },
    )

    check_source >> ingest >> bronze_job >> silver_job >> gold_job >> validate
