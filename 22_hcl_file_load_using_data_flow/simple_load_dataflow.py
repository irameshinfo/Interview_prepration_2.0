import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import csv


# =========================================================
# CONFIGURATION
# =========================================================

PROJECT_ID = "rameshgcplearning"
DATASET = "staging"
BUCKET = "hcl_load"

FILES = {
    "accounts": "gs://hcl_load/input_files/Datafile_accounts.csv",
    "branches": "gs://hcl_load/input_files/Datafile_branches.csv",
    "customers": "gs://hcl_load/input_files/Datafile_customers.csv",
    "products": "gs://hcl_load/input_files/Datafile_products.csv",
    "transactions": "gs://hcl_load/input_files/Datafile_transactions.csv"
}


# =========================================================
# BIGQUERY SCHEMAS
# =========================================================

SCHEMAS = {

    "accounts":
        "account_id:STRING,"
        "account_name:STRING,"
        "email:STRING,"
        "phone_number:STRING,"
        "address:STRING,"
        "created_date:STRING",

    "branches":
        "branch_id:STRING,"
        "branch_name:STRING,"
        "location:STRING,"
        "manager_name:STRING,"
        "opened_date:STRING,"
        "branch_type:STRING,"
        "contact_number:STRING",

    "customers":
        "customer_id:STRING,"
        "customer_name:STRING,"
        "email:STRING,"
        "phone:STRING,"
        "account_id:STRING,"
        "gender:STRING,"
        "dob:STRING,"
        "address:STRING,"
        "kyc_status:STRING,"
        "registration_date:STRING,"
        "transaction_id:STRING",

    "products":
        "product_id:STRING,"
        "product_name:STRING,"
        "product_type:STRING,"
        "price:INT64,"
        "launch_date:STRING,"
        "is_active:STRING,"
        "category:STRING,"
        "vendor_name:STRING",

    "transactions":
        "txn_id:STRING,"
        "account_id:STRING,"
        "product_id:STRING,"
        "branch_id:STRING,"
        "date:STRING,"
        "timestamp:STRING,"
        "amount:INT64,"
        "payment_method:STRING,"
        "status:STRING,"
        "currency:STRING,"
        "remarks:STRING,"
        "is_refund:STRING"
}

# =========================================================
# CSV PARSER
# =========================================================

def parse_csv(line, headers):

    values = list(csv.reader([line]))[0]

    return dict(zip(headers, values))


# =========================================================
# PIPELINE OPTIONS
# =========================================================

options = PipelineOptions(

    runner="DataflowRunner",

    project=PROJECT_ID,

    region="us-central1",

    temp_location="gs://hcl_load/temp",

    staging_location="gs://hcl_load/staging",

    job_name="csv-to-bq-load",

    save_main_session=True
)


# =========================================================
# PIPELINE
# =========================================================

with beam.Pipeline(options=options) as p:

    for table_name, file_path in FILES.items():

        # Read Header
        with beam.io.filesystems.FileSystems.open(file_path) as f:

            header_line = f.readline().decode("utf-8").strip()

        headers = header_line.split(",")

        (
            p

            | f"Read_{table_name}" >> beam.io.ReadFromText(
                file_path,
                skip_header_lines=1
            )

            | f"Parse_{table_name}" >> beam.Map(
                lambda line, h=headers: parse_csv(line, h)
            )

            | f"Write_{table_name}" >> WriteToBigQuery(

                table=f"{PROJECT_ID}:{DATASET}.{table_name}",

                schema=SCHEMAS[table_name],

                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,

                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,

                custom_gcs_temp_location="gs://hcl_load/bq_temp"
            )
        )

print("Pipeline submitted successfully")