from google.cloud import bigquery
from datetime import datetime


def load_csv_to_bq(event, context):

    client = bigquery.Client()

    # -------------------------------------------------
    # FILE DETAILS
    # -------------------------------------------------

    bucket_name = event['bucket']
    file_name = event['name']

    print(f"Processing file: {file_name}")

    # -------------------------------------------------
    # PROCESS ONLY CSV
    # -------------------------------------------------

    if not file_name.endswith(".csv"):

        print("Skipping non CSV file")
        return

    # -------------------------------------------------
    # FILE PATH
    # -------------------------------------------------

    uri = f"gs://{bucket_name}/{file_name}"

    # -------------------------------------------------
    # FILE NAME
    # -------------------------------------------------

    base_file_name = file_name.split("/")[-1]

    # -------------------------------------------------
    # FILE → TABLE MAPPING
    # -------------------------------------------------

    table_mapping = {

        "Datafile_customers.csv": "customers_stg",

        "Datafile_accounts.csv": "accounts_stg",

        "Datafile_branches.csv": "branches_stg",

        "Datafile_products.csv": "products_stg",

        "Datafile_transactions.csv": "transactions_stg"
    }

    # -------------------------------------------------
    # GET TABLE NAME
    # -------------------------------------------------

    table_name = table_mapping.get(base_file_name)

    if not table_name:

        print(f"No matching table for {base_file_name}")
        return

    # -------------------------------------------------
    # TABLE ID
    # -------------------------------------------------

    table_id = (
        f"rameshgcplearning.staging.{table_name}"
    )

    print(f"Loading into table: {table_id}")

    # -------------------------------------------------
    # LOAD CONFIG
    # -------------------------------------------------

    job_config = bigquery.LoadJobConfig(

        source_format=bigquery.SourceFormat.CSV,

        field_delimiter=",",

        skip_leading_rows=1,

        autodetect=True,

        write_disposition="WRITE_TRUNCATE"
    )

    try:

        # -------------------------------------------------
        # LOAD DATA
        # -------------------------------------------------

        load_job = client.load_table_from_uri(

            uri,

            table_id,

            job_config=job_config
        )

        load_job.result()

        # -------------------------------------------------
        # GET ROW COUNT
        # -------------------------------------------------

        table = client.get_table(table_id)

        row_count = table.num_rows

        print(f"Rows loaded: {row_count}")

        # -------------------------------------------------
        # INSERT AUDIT LOG
        # -------------------------------------------------

        audit_table = (
            "rameshgcplearning.staging.audit_log"
        )

        rows_to_insert = [

            {

                "file_name": base_file_name,

                "table_name": table_name,

                "row_count": row_count,

                "status": "SUCCESS",

                "load_time": datetime.utcnow().isoformat()

            }

        ]

        errors = client.insert_rows_json(

            audit_table,

            rows_to_insert
        )

        if errors:

            print(f"Audit insert errors: {errors}")

        else:

            print("Audit log inserted successfully")

        print(
            f"Loaded {file_name} into {table_id}"
        )

    except Exception as e:

        print(f"Error: {str(e)}")

        # -------------------------------------------------
        # INSERT FAILURE LOG
        # -------------------------------------------------

        audit_table = (
            "rameshgcplearning.staging.audit_log"
        )

        rows_to_insert = [

            {

                "file_name": base_file_name,

                "table_name": table_name,

                "row_count": 0,

                "status": "FAILED",

                "load_time": datetime.utcnow().isoformat()

            }

        ]

        client.insert_rows_json(

            audit_table,

            rows_to_insert
        )