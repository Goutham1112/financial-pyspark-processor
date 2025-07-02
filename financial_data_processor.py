from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from delta import configure_spark_with_delta_pip
import os

# --- 1. Configure Spark Session with Delta Lake ---
print("Initializing Spark Session...")
builder = SparkSession.builder \
    .appName("FinancialDataProcessor") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.driver.memory", "4g") # Give Spark more memory if you face issues

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR") # Reduce noisy Spark logs

print("Spark Session initialized.")

# --- 2. Define Input and Output Paths ---
# Using relative paths for simplicity
csv_input_path = "mock_transactions.csv"
delta_output_path = "processed_financial_data_delta" # This will be a folder

# --- 3. Ingest Data ---
print(f"Reading data from {csv_input_path}...")
try:
    # We infer schema for simplicity, but in real-world, you'd define it strictly.
    df = spark.read \
              .option("header", "true") \
              .option("inferSchema", "true") \
              .csv(csv_input_path)
    print("Data ingested successfully.")
    df.printSchema()
    df.show(5)
except Exception as e:
    print(f"Error reading CSV: {e}")
    spark.stop()
    exit() # Exit if we can't read the input file

# --- 4. Basic Cleaning and Transformation ---
print("Applying basic cleaning and transformations...")
# Drop rows where 'transaction_id' or 'amount' is null (critical fields)
cleaned_df = df.dropna(subset=['transaction_id', 'amount'])

# Cast 'amount' to Double and 'date' to Date type
# If date inference fails, try specifying format: F.to_date(col("date"), "yyyy-MM-dd")
transformed_df = cleaned_df.withColumn("amount", col("amount").cast("double")) \
                           .withColumn("date", col("date").cast("date")) \
                           .withColumn("transaction_id", col("transaction_id").cast("int"))

# Fill empty 'currency' with 'UNKNOWN'
transformed_df = transformed_df.fillna({"currency": "UNKNOWN"})

# Deduplicate based on transaction_id (assuming transaction_id should be unique)
# This keeps the first occurrence if duplicates exist
final_df = transformed_df.dropDuplicates(['transaction_id'])

print("Cleaning and transformation complete.")
final_df.printSchema()
final_df.show(5)

# --- 5. Write to Delta Lake ---
print(f"Writing processed data to Delta Lake at {delta_output_path}...")
try:
    # Use 'overwrite' mode for simplicity in this example.
    # For incremental loads, you'd use 'append' or 'merge'.
    final_df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(delta_output_path)
    print("Data successfully written to Delta Lake.")
except Exception as e:
    print(f"Error writing to Delta Lake: {e}")

# --- Verify Delta Table (Optional) ---
print(f"\nVerifying data from Delta Lake: {delta_output_path}...")
try:
    read_delta_df = spark.read.format("delta").load(delta_output_path)
    read_delta_df.show()
    print("Delta Lake read verification successful.")
except Exception as e:
    print(f"Error reading from Delta Lake for verification: {e}")


# --- 6. Stop Spark Session ---
print("Stopping Spark Session.")
spark.stop()
print("Processing complete.")