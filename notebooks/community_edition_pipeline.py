# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 🛒 E-Commerce Data Engineering Platform (Community Edition)
# MAGIC **Author:** Jagdeep (jagdeepa1047@gmail.com)  
# MAGIC **GitHub:** https://github.com/jagdeep222422/ecommerce-data-platform
# MAGIC 
# MAGIC > **100% Standalone Notebook:** Runs directly on Databricks Community Edition using internal **DBFS (Databricks File System)**. 
# MAGIC > **No Azure, AWS, or cloud credentials required!**

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1️⃣ Install & Import Dependencies

# COMMAND ----------
import requests
import json
from datetime import datetime
from pyspark.sql.functions import (
    col, current_timestamp, round as spark_round,
    avg, count, sum as spark_sum, when
)
from pyspark.sql.types import DoubleType

# Define DBFS storage paths (Internal to Databricks)
DBFS_BASE = "dbfs:/FileStore/ecommerce_platform"
BRONZE_PRODUCTS_PATH = f"{DBFS_BASE}/bronze/products"
BRONZE_ORDERS_PATH   = f"{DBFS_BASE}/bronze/orders"
SILVER_PRODUCTS_PATH = f"{DBFS_BASE}/silver/products"
GOLD_CATEGORY_PATH   = f"{DBFS_BASE}/gold/sales_by_category"

print("✅ Setup complete! Storage paths pointing to internal DBFS.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2️⃣ Ingestion (REST API → Bronze Layer DBFS)

# COMMAND ----------
# Fetch real data from FakeStore REST API
response = requests.get("https://fakestoreapi.com/products", timeout=15)
products_data = response.json()

# Save raw JSON into DBFS
dbutils.fs.mkdirs(BRONZE_PRODUCTS_PATH)
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
raw_file_path = f"{BRONZE_PRODUCTS_PATH}/products_{timestamp}.json"

dbutils.fs.put(raw_file_path, json.dumps(products_data), overwrite=True)
print(f"✅ Ingested {len(products_data)} products into Bronze DBFS: {raw_file_path}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3️⃣ Silver Layer (Clean, Validate & Store as Delta Lake)

# COMMAND ----------
# Read raw JSON from Bronze
df_raw = spark.read.json(raw_file_path)

# Transformation & Quality Cleansing
df_silver = (
    df_raw
    .filter(col("id").isNotNull())
    .filter(col("price") > 0)
    .withColumn("price_usd", col("price").cast(DoubleType()))
    .withColumn("ingested_at", current_timestamp())
    .dropDuplicates(["id"])
)

# Write to Silver Delta Lake Table
df_silver.write.format("delta").mode("overwrite").save(SILVER_PRODUCTS_PATH)

# Register as SQL Table
spark.sql(f"CREATE TABLE IF NOT EXISTS silver_products USING DELTA LOCATION '{SILVER_PRODUCTS_PATH}'")

print("✅ Silver Delta table processed & registered successfully!")
display(df_silver.limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4️⃣ Gold Layer (Business Aggregations & Analytics)

# COMMAND ----------
# Read Silver Delta Table
df_silver_read = spark.read.format("delta").load(SILVER_PRODUCTS_PATH)

# Aggregate metrics for business reporting
df_gold = (
    df_silver_read
    .groupBy("category")
    .agg(
        count("id").alias("product_count"),
        spark_round(avg("price_usd"), 2).alias("avg_price_usd"),
        spark_round(spark_sum("price_usd"), 2).alias("total_inventory_value")
    )
    .orderBy(col("total_inventory_value").desc())
)

# Write to Gold Delta Table
df_gold.write.format("delta").mode("overwrite").save(GOLD_CATEGORY_PATH)
spark.sql(f"CREATE TABLE IF NOT EXISTS gold_sales_by_category USING DELTA LOCATION '{GOLD_CATEGORY_PATH}'")

print("✅ Gold Delta table created!")
display(df_gold)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5️⃣ Databricks SQL Queries & Dashboards

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT 
# MAGIC     category,
# MAGIC     product_count,
# MAGIC     avg_price_usd,
# MAGIC     total_inventory_value,
# MAGIC     RANK() OVER (ORDER BY total_inventory_value DESC) as rank
# MAGIC FROM gold_sales_by_category;
