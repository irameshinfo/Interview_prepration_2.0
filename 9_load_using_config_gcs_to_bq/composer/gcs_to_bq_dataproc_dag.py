from airflow import DAG
from datetime import datetime
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator
)

# =========================
# CONFIG
# =========================
PROJECT_ID = "rameshgcplearning"
REGION = "us-east1"
CLUSTER_NAME = "ephemeral-cluster-{{ ds_nodash }}"
BUCKET = "dataproc_case_ram"

PYSPARK_URI = f"gs://{BUCKET}/scripts/dataproc_job.py"
CONFIG_URI = f"gs://{BUCKET}/config/config.json"

default_args = {
    "start_date": datetime(2024, 1, 1)
}

# =========================
# DAG
# =========================
with DAG(
    dag_id="gcs_to_bq_dataproc_pipeline",
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["gcp", "dataproc", "bigquery"]
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        cluster_config={

            "gce_cluster_config": {
                "zone_uri": "us-east1-b",
                "service_account_scopes": [
                    "https://www.googleapis.com/auth/cloud-platform"
                ]
            },

            "master_config": {
                "num_instances": 1,
                "machine_type_uri": "e2-standard-4",
                "disk_config": {
                    "boot_disk_size_gb": 50
                }
            },

            "worker_config": {
                "num_instances": 0
            },

            "software_config": {
                "image_version": "2.1-debian11",
                "properties": {
                    "dataproc:dataproc.allow.zero.workers": "true",  # ✅ FIXED
                    "spark:spark.driver.memory": "4g",
                    "spark:spark.executor.memory": "4g",
                    "spark:spark.executor.cores": "2"
                }
            },

            "lifecycle_config": {
                "auto_delete_ttl": {"seconds": 3600}
            }
        }
    )

    pyspark_job = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": PYSPARK_URI,
            "args": [CONFIG_URI]
        }
    }

    run_job = DataprocSubmitJobOperator(
        task_id="run_pyspark_job",
        job=pyspark_job,
        region=REGION,
        project_id=PROJECT_ID
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule="all_done"
    )

    create_cluster >> run_job >> delete_cluster