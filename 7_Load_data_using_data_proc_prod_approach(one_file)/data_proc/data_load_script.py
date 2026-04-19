from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
import json
from google.cloud import storage

spark = SparkSession.builder \
    .appName("GCS to BQ with Config") \
    .getOrCreate()

# =========================
# READ CONFIG FROM GCS
# =========================
config_path = "gs://dataproc_case/config/config.json"

storage_client = storage.Client()

bucket_name = config_path.split("/")[2]
blob_path = "/".join(config_path.split("/")[3:])

bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(blob_path)

config = json.loads(blob.download_as_text())

# =========================
# READ CONFIG VALUES
# =========================
input_path = config["input_path"]
archive_bucket = config["archive_bucket"]
archive_path = config["archive_path"]
temp_bucket = config["temp_bucket"]

valid_table = config["bq"]["valid_table"]
invalid_table = config["bq"]["invalid_table"]
audit_table = config["bq"]["audit_table"]

phone_regex = config["validation"]["phone_regex"]
email_regex = config["validation"]["email_regex"]

# =========================
# READ DATA
# =========================
df = spark.read \
    .option("header", "true") \
    .option("delimiter", ",") \
    .option("inferSchema", "true") \
    .csv(input_path)

total_count = df.count()

# =========================
# VALIDATION
# =========================
df = df.withColumn("valid_phone", col("phone").rlike(phone_regex)) \
       .withColumn("valid_email", col("email").rlike(email_regex))

valid_df = df.filter(col("valid_phone") & col("valid_email"))

invalid_df = df.filter(~(col("valid_phone") & col("valid_email")))

valid_count = valid_df.count()
invalid_count = invalid_df.count()

# =========================
# ADD TIMESTAMP
# =========================
valid_df = valid_df.withColumn("load_time", current_timestamp())
invalid_df = invalid_df.withColumn("load_time", current_timestamp())

# =========================
# WRITE TO BIGQUERY
# =========================
valid_df.write.format("bigquery") \
    .option("table", valid_table) \
    .option("temporaryGcsBucket", "dataproc_temp_buk") \
    .mode("append") \
    .save()

invalid_df.write.format("bigquery") \
    .option("table", invalid_table) \
    .option("temporaryGcsBucket", "dataproc_temp_buk") \
    .mode("append") \
    .save()

# =========================
# AUDIT LOG
# =========================
audit_data = [(input_path, total_count, valid_count, invalid_count)]

audit_df = spark.createDataFrame(
    audit_data,
    ["file_name", "total_count", "valid_count", "invalid_count"]
).withColumn("load_time", current_timestamp())

audit_df.write.format("bigquery") \
    .option("table", audit_table) \
    .option("temporaryGcsBucket", "dataproc_temp_buk") \
    .mode("append") \
    .save()

# =========================
# ARCHIVE FILE
# =========================
source_blob = bucket.blob(input_path.split("/", 3)[-1])
destination_blob = bucket.blob(archive_path)

bucket.copy_blob(source_blob, bucket, destination_blob.name)
source_blob.delete()

print("Job completed successfully")