from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CleanAndWriteFailedTransactions") \
    .getOrCreate()

# Read all CSV files from GCS
df = spark.read.csv("gs://saadbucket/newproject/*.csv", header=True, inferSchema=True)

# Drop rows where any column is null
df_cleaned = df.na.drop()

# Drop rows with empty strings in important columns
df_cleaned = df_cleaned.filter(
    (col("transaction_id") != "") &
    (col("amount").isNotNull())
)

# Filter for failed transactions
df_failed = df_cleaned.filter(col("status") == "failed")

# MySQL connection properties
jdbc_url = "jdbc:mysql://34.93.159.74/saaddatabase"
properties = {
    "user": "saadsql",
    "password": "SaadSaad953323@",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Write failed transactions to MySQL
df_failed.write.jdbc(
    url=jdbc_url,
    table="failed_transactions",
    mode="overwrite",  # change to "append" if you don't want to overwrite
    properties=properties
)

# Stop Spark session
spark.stop()
