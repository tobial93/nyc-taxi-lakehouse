resource "azurerm_databricks_workspace" "main" {
  name                = "dbw-nyc-taxi-lakehouse${local.suffix}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  sku                 = var.databricks_sku
  tags                = local.tags
}

# Shared cluster for all three pipeline jobs (auto-terminates when idle)
resource "databricks_cluster" "transform" {
  cluster_name            = "nyc-taxi-transform${local.suffix}"
  spark_version           = var.spark_version
  node_type_id            = var.cluster_node_type
  autotermination_minutes = 20

  autoscale {
    min_workers = 1
    max_workers = 4
  }

  spark_conf = {
    "spark.databricks.delta.retentionDurationCheck.enabled" = "false"
    "spark.sql.extensions"                                  = "io.delta.sql.DeltaSparkSessionExtension"
    "spark.sql.catalog.spark_catalog"                       = "org.apache.spark.sql.delta.catalog.DeltaCatalog"
  }

  depends_on = [azurerm_databricks_workspace.main]
}

# Upload notebooks to workspace
resource "databricks_notebook" "bronze" {
  source = "${path.module}/../databricks/notebooks/01_bronze_ingest.py"
  path   = "/nyc-taxi-lakehouse/01_bronze_ingest"
}

resource "databricks_notebook" "silver" {
  source = "${path.module}/../databricks/notebooks/02_silver_transform.py"
  path   = "/nyc-taxi-lakehouse/02_silver_transform"
}

resource "databricks_notebook" "gold" {
  source = "${path.module}/../databricks/notebooks/03_gold_aggregate.py"
  path   = "/nyc-taxi-lakehouse/03_gold_aggregate"
}

# Jobs triggered by Airflow via DatabricksRunNowOperator
resource "databricks_job" "bronze" {
  name = "nyc-taxi-bronze${local.suffix}"

  task {
    task_key = "bronze_ingest"
    notebook_task {
      notebook_path = databricks_notebook.bronze.path
    }
    existing_cluster_id = databricks_cluster.transform.id
  }
}

resource "databricks_job" "silver" {
  name = "nyc-taxi-silver${local.suffix}"

  task {
    task_key = "silver_transform"
    notebook_task {
      notebook_path = databricks_notebook.silver.path
    }
    existing_cluster_id = databricks_cluster.transform.id
  }
}

resource "databricks_job" "gold" {
  name = "nyc-taxi-gold${local.suffix}"

  task {
    task_key = "gold_aggregate"
    notebook_task {
      notebook_path = databricks_notebook.gold.path
    }
    existing_cluster_id = databricks_cluster.transform.id
  }
}
