from airflow import DAG
from datetime import datetime
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator

PROJECT_ID = "rameshgcplearning"
REGION = "us-central1"
SERVICE_ACCOUNT = "rameshgcplearning@rameshgcplearning.iam.gserviceaccount.com"

with DAG(
    dag_id="dataflow_streaming_job",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    run_dataflow = BeamRunPythonPipelineOperator(
        task_id="start_streaming_pipeline",
        py_file="gs://dataflowtempbucket7/dataflow_trans.py",
        runner="DataflowRunner",
        pipeline_options={
            "project": PROJECT_ID,
            "region": REGION,
            "job_name": "streaming-job-{{ ts_nodash }}",
            "runner": "DataflowRunner",
            "temp_location": "gs://dataflowtempbucket7/temp",
            "staging_location": "gs://dataflowstaging7/staging",
            "streaming": True,

            # ✅ ONLY THIS
            "service_account_email": SERVICE_ACCOUNT,
        },
    )