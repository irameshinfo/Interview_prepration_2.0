import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# =========================
# INIT
# =========================
spark = SparkSession.builder.appName("GCS to BQ Load").getOrCreate()

# =========================
# READ CONFIG
# =========================
config_path = sys.argv[1]

config = spark.read.text(config_path).collect()
config_json = json.loads("".join([row.value for row in config]))

input_path = config_json["input_path"]
valid_table = f'{config_json["project_id"]}.{config_json["dataset"]}.{config_json["valid_table"]}'
invalid_table = f'{config_json["project_id"]}.{config_json["dataset"]}.{config_json["invalid_table"]}'
temp_bucket = config_json["temp_bucket"]

# =========================
# READ CSV
# =========================
df = spark.read.option("header", True).csv(input_path)

# =========================
# VALIDATION (NO UDF 🚀)
# =========================
email_regex = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
phone_regex = r'^\+?[1-9]\d{9,14}$'

validated_df = df.withColumn(
    "valid_email",
    col("email").rlike(email_regex)
).withColumn(
    "valid_phone",
    col("phone").rlike(phone_regex)
)

# =========================
# SPLIT DATA
# =========================
valid_df = validated_df.filter(
    col("valid_email") & col("valid_phone")
)

invalid_df = validated_df.filter(
    ~(col("valid_email") & col("valid_phone"))
)

# =========================
# WRITE TO BIGQUERY
# =========================
valid_df.write.format("bigquery") \
    .option("table", valid_table) \
    .option("temporaryGcsBucket", temp_bucket) \
    .mode("overwrite") \
    .save()

invalid_df.write.format("bigquery") \
    .option("table", invalid_table) \
    .option("temporaryGcsBucket", temp_bucket) \
    .mode("overwrite") \
    .save()

# =========================
# END
# =========================
spark.stop()