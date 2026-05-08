from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago

# =========================================================
# CONFIGURATION
# =========================================================

PROJECT_ID = "rameshgcplearning"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1)
}

# =========================================================
# DAG DEFINITION
# =========================================================

with DAG(
    dag_id="bq_curated_reporting_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["curated", "reporting"]
) as dag:

    # =====================================================
    # CALL CURATED LAYER MERGE PROCEDURE
    # =====================================================
   
    curated_layer_load = BigQueryInsertJobOperator(
    task_id="curated_layer_load",
    configuration={
        "query": {
            "query": "CALL `rameshgcplearning.curated.sp_load_all_tables_merge`()",
            "useLegacySql": False,
        }
    },
    location="us-central1",
    )
    
    

    # =====================================================
    # CALL REPORTING KPI PROCEDURE
    # =====================================================

    run_reporting_kpi_proc = BigQueryInsertJobOperator(
        task_id="run_reporting_kpi_proc",
    configuration={
        "query": {
            "query": "CALL `rameshgcplearning.reporting.sp_kpi_report_load`()",
            "useLegacySql": False,
        }
    },
    location="us-central1",
    )

    # =====================================================
    # DAG FLOW
    # =====================================================

    curated_layer_load >> run_reporting_kpi_proc