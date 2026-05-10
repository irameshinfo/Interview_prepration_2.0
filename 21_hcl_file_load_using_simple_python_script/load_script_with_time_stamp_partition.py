from google.cloud import bigquery
from datetime import datetime

client = bigquery.Client()

config = bigquery.LoadJobConfig(
    source_format="CSV",
    skip_leading_rows=1,
    autodetect=True,
    write_disposition="WRITE_TRUNCATE"
)

tables = ["accounts", "branches", "customers", "products", "transactions"]

for table in tables:

    uri = f"gs://hcl_load/input_files/Datafile_{table}.csv"

    staging_table = f"rameshgcplearning.staging.{table}"
    temp_table = f"rameshgcplearning.staging.temp_{table}"

    # Step 1: Load CSV into temp table
    load_job = client.load_table_from_uri(
        uri,
        temp_table,
        job_config=config
    )

    load_job.result()

    # Step 2: Add load timestamp into final table
    query = f"""
    CREATE OR REPLACE TABLE `{staging_table}` PARTITION BY DATE(load_timestamp) AS
    SELECT
        *,
        CURRENT_TIMESTAMP() AS load_timestamp
    FROM `{temp_table}`
    """

    query_job = client.query(query)

    query_job.result()

    print(f"{table} loaded successfully")