# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# Ensure gold schema exists
spark.sql("CREATE SCHEMA IF NOT EXISTS claude_catalog.gold")

# COMMAND ----------

# Create aggregate view: bookings count per city
spark.sql("""
    CREATE OR REPLACE VIEW claude_catalog.gold.bookings_per_city AS
    SELECT
        city,
        country,
        COUNT(booking_id) AS total_bookings
    FROM claude_catalog.silver.bookings_enriched
    GROUP BY city, country
    ORDER BY total_bookings DESC
""")

print("View 'claude_catalog.gold.bookings_per_city' created successfully.")

# COMMAND ----------

# Preview the view
result = spark.sql("SELECT * FROM claude_catalog.gold.bookings_per_city")
result.show(truncate=False)
