import pandas as pd
from google.cloud import storage, bigquery
from io import StringIO

def load_with_pandas(event, context):
    storage_client = storage.Client()
    bq_client = bigquery.Client()

    bucket = storage_client.bucket(event['bucket'])
    blob = bucket.blob(event['name'])

    data = blob.download_as_text()

    df = pd.read_csv(StringIO(data), delimiter=",")  # 👈 delimiter here

    # Load to BigQuery
    table_id = "ranjanrishi-project.cloud_function_load.emp_pands"
    job = bq_client.load_table_from_dataframe(df, table_id)
    job.result()

    print("Loaded using pandas")