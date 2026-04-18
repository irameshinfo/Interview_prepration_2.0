from airflow import DAG
#from airflow.utils.dates import days_ago (not supported in new version of airflow)
from datetime import datetime, timedelta
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator

PROJECT_ID = "ranjanrishi-project"
REGION = "us-central1"

with DAG(
    dag_id="dataflow_streaming_job",
    #start_date=days_ago(1), (not supported in new version of airflow)
    start_date=datetime.now() - timedelta(days=1),
    schedule='@daily',   # ✅ updated
    catchup=False,
) as dag:

    run_dataflow = BeamRunPythonPipelineOperator(
        task_id="start_streaming_pipeline",
        py_file="gs://dataflowtempbucket7/dataflow_trans.py",
        runner="DataflowRunner",
        pipeline_options={
            "project": PROJECT_ID,
            "region": REGION,
            "temp_location": "gs://dataflowtempbucket7",
            "staging_location": "gs://dataflowstaging7",
            "streaming": True,
        },
    )

    run_dataflow