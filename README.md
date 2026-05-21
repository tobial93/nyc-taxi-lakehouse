# NYC Taxi Analytics Lakehouse

Azure · Databricks · Airflow · Delta Lake · ADLS Gen2 · Terraform

## Overview

Monthly batch pipeline that ingests NYC Yellow Taxi trip data into a medallion lakehouse on Azure. Airflow orchestrates the three-stage pipeline; Databricks runs the PySpark transformations; Delta Lake provides ACID transactions and time travel across all layers.

```
Airflow (Docker Compose)
     │
     ▼ ShortCircuitOperator — skip if source not yet published
     │
     ▼ PythonOperator — download Parquet → ADLS Gen2 bronze/raw/
     │
     ▼ DatabricksRunNowOperator — bronze job
     │  Register raw Parquet as partitioned Delta table
     │
     ▼ DatabricksRunNowOperator — silver job
     │  Type-cast · clean outliers · derive trip_duration_minutes
     │
     ▼ DatabricksRunNowOperator — gold job
     │  fct_trips_hourly   — trip volume, fares, distances by hour + zone
     │  fct_revenue_daily  — daily revenue and trip counts by zone
     │
     ▼ PythonOperator — validate gold partition exists
```

## Repository Structure

```
nyc-taxi-lakehouse/
├── airflow/
│   ├── dags/
│   │   └── nyc_taxi_pipeline.py   # Monthly DAG, catchup-enabled
│   ├── docker-compose.yml         # Airflow 2.9 + LocalExecutor + Postgres
│   ├── requirements.txt           # Databricks + Azure SDK providers
│   └── .env.example               # Environment variables template
├── databricks/
│   └── notebooks/
│       ├── 01_bronze_ingest.py    # Raw Parquet → Delta
│       ├── 02_silver_transform.py # Clean, type-cast, filter outliers
│       └── 03_gold_aggregate.py   # fct_trips_hourly + fct_revenue_daily
├── terraform/
│   ├── main.tf                    # Providers, backend, resource group
│   ├── variables.tf
│   ├── adls.tf                    # Storage account + bronze/silver/gold containers
│   ├── databricks.tf              # Workspace, cluster, notebooks, jobs
│   ├── iam.tf                     # Service principal + secret scope
│   └── outputs.tf                 # Job IDs for Airflow Variables
└── tests/
    └── test_transformations.py    # PySpark unit tests for silver filters
```

## Prerequisites

- Azure subscription with billing enabled
- Azure CLI (`az login`)
- Terraform >= 1.7
- Docker + Docker Compose
- Python 3.11+

## Deploy

### 1. Provision infrastructure

```bash
cd terraform

terraform init \
  -backend-config="resource_group_name=<your-tfstate-rg>" \
  -backend-config="storage_account_name=<your-tfstate-sa>" \
  -backend-config="container_name=tfstate" \
  -backend-config="key=nyc-taxi-lakehouse/dev.tfstate"

terraform apply \
  -var="environment=dev" \
  -var="storage_account_name=<globally-unique-name>"
```

### 2. Wire Airflow to Databricks

After `terraform apply`, copy the job IDs from the output:

```bash
terraform output databricks_job_id_bronze
terraform output databricks_job_id_silver
terraform output databricks_job_id_gold
```

In the Airflow UI (http://localhost:8080), set these as Airflow Variables:
`DATABRICKS_JOB_ID_BRONZE`, `DATABRICKS_JOB_ID_SILVER`, `DATABRICKS_JOB_ID_GOLD`

Also create a Databricks connection (`Admin > Connections`):
- Conn ID: `databricks_default`
- Host: your workspace URL
- Token: personal access token

### 3. Start Airflow

```bash
cd airflow
cp .env.example .env  # fill in values
docker-compose up airflow-init
docker-compose up -d
```

### 4. Run tests

```bash
pip install pytest pyspark delta-spark
pytest tests/ -v
```

## Medallion Architecture

| Layer  | Storage path          | Format | Notes                                      |
|--------|-----------------------|--------|--------------------------------------------|
| Bronze | `bronze/raw/...`      | Parquet | Raw files from NYC TLC CDN                |
|        | `bronze/delta/...`    | Delta  | Partitioned by year/month                  |
| Silver | `silver/delta/...`    | Delta  | Cleaned, typed, outliers removed           |
| Gold   | `gold/delta/...`      | Delta  | Aggregated fact tables, BI-ready           |

### Silver quality filters

| Check | Rule |
|-------|------|
| Null drop | pickup_datetime, dropoff_datetime, total_amount |
| Distance | 0 < trip_distance_miles < 200 |
| Fare | 0 < fare_amount, 0 < total_amount < 1000 |
| Time order | pickup_datetime < dropoff_datetime |
| Duration | 0 < trip_duration_minutes < 300 |
| Month scope | pickup year/month matches pipeline run |

## Production Enhancements

- Replace personal access token with Azure Managed Identity for Databricks auth
- Add Delta table `OPTIMIZE` and `VACUUM` jobs on a weekly schedule
- Wire Great Expectations for automated data quality reporting on gold tables
- Enable Unity Catalog for fine-grained table-level access control
- Add a Databricks SQL warehouse for direct BI tool connectivity
