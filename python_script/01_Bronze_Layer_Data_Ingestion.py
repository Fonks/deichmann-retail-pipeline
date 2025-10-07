# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Raw Data Ingestion
# MAGIC 
# MAGIC **Project:** Deichmann Retail Analytics Pipeline  
# MAGIC **Layer:** Bronze (Raw Data)  
# MAGIC **Purpose:** Ingest CSV files and create Delta Lake tables
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## What this notebook does:
# MAGIC 1. Uploads CSV files to DBFS (Databricks File System)
# MAGIC 2. Reads CSV data with PySpark
# MAGIC 3. Validates data quality
# MAGIC 4. Writes data as Delta Tables (Bronze Layer)
# MAGIC 5. Displays summary statistics

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Setup and Configuration

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# Initialize Spark Session (already available in Databricks)
spark = SparkSession.builder.getOrCreate()

# Set configurations
spark.conf.set("spark.sql.shuffle.partitions", "8")
spark.conf.set("spark.sql.adaptive.enabled", "true")

# Define paths
bronze_path = "/mnt/bronze/"
print("✅ Configuration complete!")
print(f"📁 Bronze layer path: {bronze_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Upload CSV Files to DBFS
# MAGIC 
# MAGIC **MANUAL STEP:** You need to upload your CSV files first!
# MAGIC 
# MAGIC ### How to upload:
# MAGIC 1. In Databricks, click on "Data" in the left menu
# MAGIC 2. Click "Create Table"
# MAGIC 3. Click "Upload File"
# MAGIC 4. Upload all 4 CSV files:
# MAGIC    - stores.csv
# MAGIC    - products.csv
# MAGIC    - customers.csv
# MAGIC    - sales.csv
# MAGIC 5. Note the path (usually: `/FileStore/tables/filename.csv`)
# MAGIC 
# MAGIC **OR use the file upload widget below:**

# COMMAND ----------

# File paths - UPDATE THESE after uploading!
# After you upload via Data → Upload, you'll get paths like:
# /FileStore/tables/stores.csv

stores_csv_path = "dbfs:/FileStore/tables/stores.csv"
products_csv_path = "dbfs:/FileStore/tables/products.csv"
customers_csv_path = "dbfs:/FileStore/tables/customers.csv"
sales_csv_path = "dbfs:/FileStore/tables/sales.csv"

print("📂 File paths configured")
print("⚠️  Make sure to upload the CSV files first!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Define Schemas (Best Practice!)
# MAGIC 
# MAGIC Instead of letting Spark infer schemas, we define them explicitly for better control and data quality.

# COMMAND ----------

# Schema for Stores
stores_schema = StructType([
    StructField("store_id", StringType(), False),
    StructField("store_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("store_type", StringType(), True),
    StructField("size_category", StringType(), True),
    StructField("size_sqm", IntegerType(), True),
    StructField("opening_date", DateType(), True),
    StructField("is_active", BooleanType(), True)
])

# Schema for Products
products_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("category", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("season", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("cost", DoubleType(), True),
    StructField("size_range", StringType(), True),
    StructField("color_options", IntegerType(), True),
    StructField("launch_date", DateType(), True),
    StructField("is_active", BooleanType(), True)
])

# Schema for Customers
customers_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("city", StringType(), True),
    StructField("registration_date", DateType(), True),
    StructField("customer_segment", StringType(), True),
    StructField("loyalty_member", BooleanType(), True),
    StructField("is_active", BooleanType(), True)
])

# Schema for Sales
sales_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("transaction_date", DateType(), True),
    StructField("transaction_time", StringType(), True),
    StructField("store_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("discount_percent", IntegerType(), True),
    StructField("discount_amount", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("payment_method", StringType(), True),
    StructField("channel", StringType(), True)
])

print("✅ Schemas defined for all tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Read CSV Files

# COMMAND ----------

# Read Stores
print("📖 Reading stores.csv...")
df_stores_raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .schema(stores_schema) \
    .csv(stores_csv_path)

print(f"✅ Stores loaded: {df_stores_raw.count()} rows")
df_stores_raw.show(5)

# COMMAND ----------

# Read Products
print("📖 Reading products.csv...")
df_products_raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .schema(products_schema) \
    .csv(products_csv_path)

print(f"✅ Products loaded: {df_products_raw.count()} rows")
df_products_raw.show(5)

# COMMAND ----------

# Read Customers
print("📖 Reading customers.csv...")
df_customers_raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .schema(customers_schema) \
    .csv(customers_csv_path)

print(f"✅ Customers loaded: {df_customers_raw.count()} rows")
df_customers_raw.show(5)

# COMMAND ----------

# Read Sales
print("📖 Reading sales.csv...")
df_sales_raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .schema(sales_schema) \
    .csv(sales_csv_path)

print(f"✅ Sales loaded: {df_sales_raw.count()} rows")
df_sales_raw.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Data Quality Checks (Bronze Layer)
# MAGIC 
# MAGIC Basic validation before writing to Delta

# COMMAND ----------

from pyspark.sql.functions import col, count, when, isnan

def data_quality_check(df, table_name):
    """
    Performs basic data quality checks
    """
    print(f"\n{'='*60}")
    print(f"📊 DATA QUALITY REPORT: {table_name}")
    print(f"{'='*60}")
    
    # Total rows
    total_rows = df.count()
    print(f"\n📈 Total Rows: {total_rows:,}")
    
    # Null counts per column
    print(f"\n🔍 Null Values per Column:")
    null_counts = df.select([
        count(when(col(c).isNull(), c)).alias(c) 
        for c in df.columns
    ])
    null_df = null_counts.toPandas().T
    null_df.columns = ['Null Count']
    null_df['Null %'] = (null_df['Null Count'] / total_rows * 100).round(2)
    print(null_df[null_df['Null Count'] > 0])
    
    # Distinct counts for key columns
    if 'store_id' in df.columns or 'product_id' in df.columns or 'customer_id' in df.columns:
        print(f"\n🔑 Distinct Values in Key Columns:")
        for col_name in df.columns:
            if 'id' in col_name.lower():
                distinct_count = df.select(col_name).distinct().count()
                print(f"  - {col_name}: {distinct_count:,} distinct values")
    
    print(f"\n{'='*60}\n")
    
    return True

# Run quality checks
data_quality_check(df_stores_raw, "Stores")
data_quality_check(df_products_raw, "Products")
data_quality_check(df_customers_raw, "Customers")
data_quality_check(df_sales_raw, "Sales")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Add Metadata Columns
# MAGIC 
# MAGIC Add ingestion timestamp and source file info (best practice for data lineage)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# Add metadata to each dataframe
df_stores = df_stores_raw \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("stores.csv"))

df_products = df_products_raw \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("products.csv"))

df_customers = df_customers_raw \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("customers.csv"))

df_sales = df_sales_raw \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("sales.csv"))

print("✅ Metadata columns added to all dataframes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Write to Delta Lake (Bronze Layer)
# MAGIC 
# MAGIC Delta Lake provides:
# MAGIC - ACID transactions
# MAGIC - Time travel
# MAGIC - Schema evolution
# MAGIC - Better performance than Parquet

# COMMAND ----------

# Write Stores to Delta
print("💾 Writing Stores to Delta Lake...")
df_stores.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("bronze_stores")

print("✅ Stores table created in Bronze layer")

# COMMAND ----------

# Write Products to Delta
print("💾 Writing Products to Delta Lake...")
df_products.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("bronze_products")
print("✅ Products table created in Bronze layer")

# COMMAND ----------

# Write Customers to Delta
print("💾 Writing Customers to Delta Lake...")
df_customers.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("bronze_customers")

print("✅ Customers table created in Bronze layer")

# COMMAND ----------

# Write Sales to Delta
print("💾 Writing Sales to Delta Lake...")
df_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("transaction_date") \
    .saveAsTable("bronze_sales")

print("✅ Sales table created in Bronze layer (partitioned by date)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Verify Delta Tables

# COMMAND ----------

# List all tables in Bronze layer
print("📋 Bronze Layer Tables:")
bronze_tables = spark.sql("SHOW TABLES").filter("tableName LIKE 'bronze_%'")
display(bronze_tables)

# COMMAND ----------

# Quick query to verify data
print("\n🔍 Sample data from each Bronze table:\n")

print("--- STORES ---")
spark.sql("SELECT * FROM bronze_stores LIMIT 5").show()

print("--- PRODUCTS ---")
spark.sql("SELECT * FROM bronze_products LIMIT 5").show()

print("--- CUSTOMERS ---")
spark.sql("SELECT * FROM bronze_customers LIMIT 5").show()

print("--- SALES ---")
spark.sql("SELECT * FROM bronze_sales LIMIT 5").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Summary Statistics

# COMMAND ----------

print("📊 BRONZE LAYER SUMMARY")
print("="*60)

# Get counts from Delta tables
stores_count = spark.sql("SELECT COUNT(*) as cnt FROM bronze_stores").collect()[0]['cnt']
products_count = spark.sql("SELECT COUNT(*) as cnt FROM bronze_products").collect()[0]['cnt']
customers_count = spark.sql("SELECT COUNT(*) as cnt FROM bronze_customers").collect()[0]['cnt']
sales_count = spark.sql("SELECT COUNT(*) as cnt FROM bronze_sales").collect()[0]['cnt']

print(f"\n📈 Record Counts:")
print(f"  - Stores:    {stores_count:>8,}")
print(f"  - Products:  {products_count:>8,}")
print(f"  - Customers: {customers_count:>8,}")
print(f"  - Sales:     {sales_count:>8,}")

# Sales summary
sales_summary = spark.sql("""
    SELECT 
        MIN(transaction_date) as first_transaction,
        MAX(transaction_date) as last_transaction,
        COUNT(DISTINCT store_id) as unique_stores,
        COUNT(DISTINCT product_id) as unique_products,
        COUNT(DISTINCT customer_id) as unique_customers,
        SUM(total_amount) as total_revenue,
        AVG(total_amount) as avg_order_value
    FROM bronze_sales
""")

print(f"\n💰 Sales Insights:")
display(sales_summary)

print("\n" + "="*60)
print("✅ BRONZE LAYER INGESTION COMPLETE!")
print("="*60)
print("\n📝 Next Steps:")
print("  1. Review the data quality reports above")
print("  2. Run the Silver Layer notebook for data cleaning")
print("  3. Check Delta Lake time travel capabilities")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎉 Congratulations!
# MAGIC 
# MAGIC You have successfully:
# MAGIC - ✅ Uploaded CSV files to Databricks
# MAGIC - ✅ Created structured schemas
# MAGIC - ✅ Performed data quality checks
# MAGIC - ✅ Written data to Delta Lake format
# MAGIC - ✅ Created Bronze layer tables
# MAGIC 
# MAGIC **For the interview, be ready to explain:**
# MAGIC - Why we use Delta Lake instead of regular Parquet
# MAGIC - The importance of schema definition
# MAGIC - Data quality checks at ingestion
# MAGIC - Partitioning strategy (sales by date)

# COMMAND ----------

