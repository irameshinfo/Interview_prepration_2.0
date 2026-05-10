from google.cloud import bigquery
from datetime import datetime

client = bigquery.Client()

PROJECT_ID = "rameshgcplearning"

AUDIT_TABLE = f"{PROJECT_ID}.audit.etl_audit"

config = bigquery.LoadJobConfig(
    source_format="CSV",
    skip_leading_rows=1,
    autodetect=True,
    write_disposition="WRITE_APPEND"
)

tables = ["accounts", "branches", "customers", "products", "transactions"]

for table in tables:

    source_file = f"Datafile_{table}.csv"

    uri = f"gs://hcl_load/input_files/{source_file}"

    temp_table = f"{PROJECT_ID}.staging.temp_{table}"

    final_table = f"{PROJECT_ID}.staging.{table}"

    load_start_time = datetime.utcnow()

    try:

        # =====================================================
        # LOAD INTO TEMP TABLE
        # =====================================================

        load_job = client.load_table_from_uri(
            uri,
            temp_table,
            job_config=config
        )

        load_job.result()

        # =====================================================
        # LOAD INTO FINAL PARTITION TABLE
        # =====================================================

        query = f"""
        CREATE OR REPLACE TABLE `{final_table}`
        PARTITION BY DATE(load_timestamp)
        AS
        SELECT
            *,
            CURRENT_TIMESTAMP() AS load_timestamp
        FROM `{temp_table}`
        """

        query_job = client.query(query)

        query_job.result()

        # =====================================================
        # GET ROW COUNT
        # =====================================================

        count_query = f"""
        SELECT COUNT(*) AS total_rows
        FROM `{final_table}`
        """

        count_result = client.query(count_query).result()

        rows_loaded = [row.total_rows for row in count_result][0]

        load_end_time = datetime.utcnow()

        # =====================================================
        # INSERT SUCCESS AUDIT RECORD
        # =====================================================

        audit_row = [
            {
                "table_name": table,
                "source_file": source_file,
                "load_start_time": load_start_time.isoformat(),
                "load_end_time": load_end_time.isoformat(),
                "rows_loaded": rows_loaded,
                "load_status": "SUCCESS",
                "error_message": None
            }
        ]

        errors = client.insert_rows_json(
            AUDIT_TABLE,
            audit_row
        )

        if errors:
            print(errors)

        print(f"{table} loaded successfully")

    except Exception as e:

        load_end_time = datetime.utcnow()

        # =====================================================
        # INSERT FAILURE AUDIT RECORD
        # =====================================================

        audit_row = [
            {
                "table_name": table,
                "source_file": source_file,
                "load_start_time": load_start_time.isoformat(),
                "load_end_time": load_end_time.isoformat(),
                "rows_loaded": 0,
                "load_status": "FAILED",
                "error_message": str(e)
            }
        ]

        client.insert_rows_json(
            AUDIT_TABLE,
            audit_row
        )

        print(f"Error loading {table}: {e}")