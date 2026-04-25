from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator
)

PROJECT_ID = "rameshgcplearning"
REGION = "us-central1"
CLUSTER_NAME = "ephemeral-postgres-bq-cluster"

PYSPARK_URI = "gs://postgres_to_bq/load_postgres_to_bq.py"

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1
}

# ✅ FIXED CLUSTER CONFIG (Single Node)
CLUSTER_CONFIG = {
    "gce_cluster_config": {
        "zone_uri": "us-central1-a",
        "subnetwork_uri": "default",
        "internal_ip_only": False
    },
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "e2-standard-2",
        "disk_config": {
        "boot_disk_type": "pd-standard",
        "boot_disk_size_gb": 50   # ✅ reduced from default
    }
    },
    # ❌ REMOVE worker_config completely
    "software_config": {
        "image_version": "2.1-debian11",
        "properties": {
            "dataproc:dataproc.allow.zero.workers": "true"
        }
    },
    "lifecycle_config": {
        "idle_delete_ttl": {"seconds": 1800}
    }
}

# ✅ JOB CONFIG
PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": PYSPARK_URI,
        "jar_file_uris": [
            "gs://spark-lib/bigquery/spark-bigquery-latest.jar"
        ],
        "properties": {           
            "spark.jars.packages": "org.postgresql:postgresql:42.6.0,com.google.cloud.sql:postgres-socket-factory:1.18.0"            
        }
    }
}

with DAG(
    dag_id="postgres_to_bq_single_node_final",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["dataproc", "postgres", "bq"]
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        cluster_config=CLUSTER_CONFIG
    )

    run_job = DataprocSubmitJobOperator(
        task_id="run_job",
        job=PYSPARK_JOB,
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
