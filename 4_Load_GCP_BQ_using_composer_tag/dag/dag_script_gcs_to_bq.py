from airflow import DAG
from datetime import datetime
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

with DAG(
    dag_id="gcs_to_bq_emp_load",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["gcp", "bigquery", "csv-load"],  # ✅ Tags defined here
) as dag:

    load_csv_to_bq = GCSToBigQueryOperator(
        task_id="load_emp_csv",
        bucket="gcplearning",
        source_objects=["emp.csv"],
        destination_project_dataset_table="rameshgcplearning.gcpworkouts.emp_air_flow",
        source_format="CSV",
        skip_leading_rows=1,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id="google_cloud_default",
    )

    load_csv_to_bq