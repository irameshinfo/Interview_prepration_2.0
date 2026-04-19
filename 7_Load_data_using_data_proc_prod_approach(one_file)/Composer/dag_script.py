from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)

PROJECT_ID = "ranjanrishi-project"
REGION = "us-central1"
CLUSTER_NAME = "my-single-node-cluster"

PYSPARK_URI = "gs://dataproc_case/data_load_script.py"

CLUSTER_CONFIG = {
    "gce_cluster_config": {
        "zone_uri": "us-central1-f"
    },
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "e2-medium",
        "disk_config": {
            "boot_disk_size_gb": 30
        }
    },
    "worker_config": {
        "num_instances": 2,   # REQUIRED (your error says minimum 2)
        "machine_type_uri": "e2-medium",
        "disk_config": {
            "boot_disk_size_gb": 30
        }
    },
    "software_config": {
        "image_version": "2.2-debian12"
    }
}

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": PYSPARK_URI
    },
}

default_args = {
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dataproc_single_node_create_run_delete",
    default_args=default_args,
    schedule=None,   # ✅ updated for Airflow 2.10+
    catchup=False,
    tags=["dataproc", "single-node", "composer"],
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_single_node_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        cluster_config=CLUSTER_CONFIG,
    )

    run_job = DataprocSubmitJobOperator(
        task_id="run_pyspark_job",
        project_id=PROJECT_ID,
        region=REGION,
        job=PYSPARK_JOB,
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        trigger_rule="all_done",
    )

    create_cluster >> run_job >> delete_cluster