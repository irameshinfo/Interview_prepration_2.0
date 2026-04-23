from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator

PROJECT_ID = "rameshgcplearning"
REGION = "us-central1"

BATCH_ID = f"pyspark-batch-{datetime.now().strftime('%Y%m%d%H%M%S')}"

PYSPARK_URI = "gs://dataproc_case_ram/data_load_script.py"

BATCH_CONFIG = {
    "pyspark_batch": {
        "main_python_file_uri": PYSPARK_URI,
    },
    "runtime_config": {
        "version": "2.1",
        "properties": {
            # ✅ Increased resources (IMPORTANT)
            "spark.executor.instances": "2",
            "spark.executor.cores": "2",
            "spark.driver.cores": "2",

            "spark.executor.memory": "4g",
            "spark.driver.memory": "4g",

            # ✅ Stability tuning
            "spark.sql.shuffle.partitions": "50",
            "spark.default.parallelism": "50"
        },
    },
}

default_args = {
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dataproc_serverless_optimized",
    default_args=default_args,
    schedule=None,
    catchup=False,
) as dag:

    run_job = DataprocCreateBatchOperator(
        task_id="run_pyspark_serverless",
        project_id=PROJECT_ID,
        region=REGION,
        batch=BATCH_CONFIG,
        batch_id=BATCH_ID,
    )