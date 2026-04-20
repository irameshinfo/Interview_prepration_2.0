from google.cloud import bigquery
# Create client
client = bigquery.Client()

# Configure load job
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    autodetect=True,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
)

# GCS file path
uri = "gs://gcplearning/emp.csv"

# Target table
table_id = "rameshgcplearning.gcpworkouts.emp"

# Load data
job = client.load_table_from_uri(
    uri,
    table_id,
    job_config=job_config
)

job.result()  # Wait for completion

print("Loaded successfully")