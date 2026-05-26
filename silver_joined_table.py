# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# Ensure silver schema exists
spark.sql("CREATE SCHEMA IF NOT EXISTS claude_catalog.silver")

# COMMAND ----------

# Load raw tables
bookings   = spark.table("claude_catalog.raw.bookings")
passengers = spark.table("claude_catalog.raw.passengers")
airports   = spark.table("claude_catalog.raw.airports")

# COMMAND ----------

# Join bookings -> passengers -> airports into one wide table
joined = (
    bookings
    .join(passengers, on="passenger_id", how="left")
    .join(airports,   on="airport_id",   how="left")
    .select(
        "booking_id",
        "passenger_id",
        "name",
        "gender",
        "nationality",
        "flight_id",
        "airport_id",
        "airport_name",
        "city",
        "country",
        "amount",
        "booking_date",
    )
)

# COMMAND ----------

# Write as Delta table in silver schema
target_table = "claude_catalog.silver.bookings_enriched"

(joined.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table))

count = spark.table(target_table).count()
print(f"Table '{target_table}' created successfully — {count} rows")
