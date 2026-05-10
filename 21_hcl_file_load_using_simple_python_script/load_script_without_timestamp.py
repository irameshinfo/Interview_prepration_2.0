from google.cloud import bigquery
client = bigquery.Client()

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    autodetect=True,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
)

# Table name -> GCS file mapping
files = {
    "accounts": "gs://hcl_load/input_files/Datafile_accounts.csv",
    "branches": "gs://hcl_load/input_files/Datafile_branches.csv",
    "customers": "gs://hcl_load/input_files/Datafile_customers.csv",
    "products": "gs://hcl_load/input_files/Datafile_products.csv",
    "transactions": "gs://hcl_load/input_files/Datafile_transactions.csv"
}

# Load all tables
jobs = []

for table, uri in files.items():

    table_id = f"rameshgcplearning.staging.{table}"

    job = client.load_table_from_uri(
        uri,
        table_id,
        job_config=job_config
    )

    jobs.append(job)

# Wait for all jobs to complete
for job in jobs:
    job.result()

print("All tables loaded successfully")