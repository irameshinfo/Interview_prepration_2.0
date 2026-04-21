from google.cloud import bigquery

def load_csv_to_bq(event, context):
    client = bigquery.Client()

    bucket_name = event['bucket']
    file_name = event['name']

    # Optional: process only specific folder/file
    if not file_name.endswith(".csv"):
        print("Skipping non-CSV file")
        return

    uri = f"gs://{bucket_name}/{file_name}"

    table_id = "rameshgcplearning.gcpworkouts.emp_cloud_fn"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        field_delimiter=",",           # 👈 delimiter here
        skip_leading_rows=1,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE"  # or WRITE_APPEND
    )

    load_job = client.load_table_from_uri(
        uri,
        table_id,
        job_config=job_config
    )

    load_job.result()  # Wait for job to complete

    print(f"Loaded {file_name} into {table_id}")