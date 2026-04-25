from airflow import DAG
from datetime import datetime
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator

# -----------------------------
# CONFIG
# -----------------------------
PROJECT_ID = "rameshgcplearning"
REGION = "us-central1"

SERVICE_ACCOUNT = "rameshgcplearning@rameshgcplearning.iam.gserviceaccount.com"

PY_FILE = "gs://dataflowtempbucket7/dataflow_trans.py"

TEMP_LOCATION = "gs://dataflowtempbucket7/temp"
STAGING_LOCATION = "gs://dataflowstaging7/staging"

# -----------------------------
# DAG
# -----------------------------
with DAG(
    dag_id="dataflow_streaming_job_v2",   # 👈 changed name to force refresh
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dataflow", "streaming"],
) as dag:

    run_dataflow = BeamRunPythonPipelineOperator(
        task_id="start_streaming_pipeline",
        py_file=PY_FILE,
        runner="DataflowRunner",
        pipeline_options={
            "project": PROJECT_ID,
            "region": REGION,

            # ✅ SAFE job name (no uppercase EVER)
            "job_name": "streaming-job-{{ ds_nodash }}",

            "runner": "DataflowRunner",
            "temp_location": TEMP_LOCATION,
            "staging_location": STAGING_LOCATION,

            "streaming": True,

            # ✅ CORRECT service account
            "service_account_email": SERVICE_ACCOUNT,

            # Optional tuning
            "autoscalingAlgorithm": "THROUGHPUT_BASED",
            "maxNumWorkers": 3,
        },
    )

    run_dataflow