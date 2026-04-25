from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Postgres_to_BQ") \
    .getOrCreate()

# -------- PostgreSQL Config --------
jdbc_url = "jdbc:postgresql://google/postgres"

properties = {
    "user": "postgres",
    "password": "YOUR_PASSWORD",
    "driver": "org.postgresql.Driver",
    "socketFactory": "com.google.cloud.sql.postgres.SocketFactory",
    "cloudSqlInstance": "rameshgcplearning:us-central1:rameshdb"
}

# -------- Read from PostgreSQL --------
df = spark.read.jdbc(
    url=jdbc_url,
    table="employee",
    properties=properties
)

df.show()

# -------- Write to BigQuery --------
df.write.format("bigquery") \
    .option("table", "rameshgcplearning.emp_dataset.employee") \
    .option("temporaryGcsBucket", "dataproc_temp_buk_ramesh") \
    .mode("append") \
    .save()