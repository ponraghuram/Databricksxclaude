import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Ensure catalog and schema exist
spark.sql("CREATE CATALOG IF NOT EXISTS claude_catalog")
spark.sql("CREATE SCHEMA IF NOT EXISTS claude_catalog.raw")

urls = {
    "airports":   "https://raw.githubusercontent.com/anshlambagit/Claude_X_Dtabricks/refs/heads/main/airports.csv",
    "bookings":   "https://raw.githubusercontent.com/anshlambagit/Claude_X_Dtabricks/refs/heads/main/bookings.csv",
    "passengers": "https://raw.githubusercontent.com/anshlambagit/Claude_X_Dtabricks/refs/heads/main/passengers.csv",
}

for table_name, url in urls.items():
    print(f"\n--- Processing {table_name} ---")

    # Fetch with pandas
    pdf = pd.read_csv(url)
    print(f"Rows: {len(pdf)}, Columns: {list(pdf.columns)}")

    # Convert to Spark DataFrame
    sdf = spark.createDataFrame(pdf)

    # Write as Delta table (overwrite so re-runs are idempotent)
    full_table = f"claude_catalog.raw.{table_name}"
    (sdf.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_table))

    count = spark.table(full_table).count()
    print(f"Table '{full_table}' created — {count} rows")

print("\nAll 3 Delta tables created successfully in claude_catalog.raw!")
