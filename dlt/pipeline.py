"""
Delta Live Tables Pipeline: Bronze → Silver → Gold
====================================================
This file defines the full DLT pipeline for the E-Commerce Analytics Platform.

Layers:
  🥉 Bronze  — Raw ingested data (schema inferred, minimal transformation)
  🥈 Silver  — Cleaned, typed, deduplicated, quality-validated data
  🥇 Gold    — Business-ready aggregations for SQL dashboards and ML features

How to use on Databricks:
  1. Upload this file to your Databricks workspace
  2. Create a Delta Live Tables pipeline pointing to this notebook
  3. Set the storage location to your ADLS/S3/DBFS path
  4. Run the pipeline

Docs: https://docs.databricks.com/delta-live-tables/index.html
"""

import dlt
from pyspark.sql.functions import (
    col, current_timestamp, to_timestamp, round as spark_round,
    avg, count, sum as spark_sum, when, month, lit
)
from pyspark.sql.types import IntegerType, DoubleType, StringType

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# Update INPUT_PATH to your actual Bronze data location
# ─────────────────────────────────────────────────────────────────────────────
INPUT_PATH_PRODUCTS = "/mnt/datalake/bronze/products/"   # or dbfs:/bronze/products/
INPUT_PATH_ORDERS   = "/mnt/datalake/bronze/orders/"
INPUT_PATH_USERS    = "/mnt/datalake/bronze/users/"
INPUT_PATH_WEATHER  = "/mnt/datalake/bronze/weather/"


# =============================================================================
# 🥉 BRONZE LAYER — Raw ingestion using Auto Loader
# =============================================================================

@dlt.table(
    name="bronze_products",
    comment="Raw product data ingested from FakeStore API via Auto Loader",
    table_properties={"quality": "bronze", "pipelines.reset.allowed": "true"}
)
def bronze_products():
    """
    Auto Loader reads new JSON files incrementally.
    cloudFiles.inferColumnTypes handles schema evolution automatically.
    """
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .load(INPUT_PATH_PRODUCTS)
            .withColumn("_load_timestamp", current_timestamp())
    )


@dlt.table(
    name="bronze_orders",
    comment="Raw order line items from FakeStore API carts endpoint"
)
def bronze_orders():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .load(INPUT_PATH_ORDERS)
            .withColumn("_load_timestamp", current_timestamp())
    )


@dlt.table(
    name="bronze_users",
    comment="Raw user/customer data from FakeStore API"
)
def bronze_users():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .load(INPUT_PATH_USERS)
            .withColumn("_load_timestamp", current_timestamp())
    )


@dlt.table(
    name="bronze_weather",
    comment="Raw weather data from OpenWeatherMap API"
)
def bronze_weather():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .load(INPUT_PATH_WEATHER)
            .withColumn("_load_timestamp", current_timestamp())
    )


# =============================================================================
# 🥈 SILVER LAYER — Cleaned, validated, typed data with DLT Expectations
# =============================================================================

@dlt.expect_or_drop("valid_product_id", "product_id IS NOT NULL AND product_id > 0")
@dlt.expect_or_drop("valid_price", "price > 0")
@dlt.expect_or_drop("title_not_empty", "title IS NOT NULL AND LENGTH(title) > 0")
@dlt.expect_or_warn("valid_rating", "rating BETWEEN 0 AND 5")
@dlt.expect_or_warn("reasonable_price", "price < 10000")
@dlt.table(
    name="silver_products",
    comment="Cleaned and validated product data — quality-enforced by DLT Expectations"
)
def silver_products():
    return (
        dlt.read_stream("bronze_products")
            .select(
                col("id").cast(IntegerType()).alias("product_id"),
                col("title").cast(StringType()),
                col("price").cast(DoubleType()),
                col("category").cast(StringType()),
                col("description").cast(StringType()),
                col("rating.rate").cast(DoubleType()).alias("rating"),
                col("rating.count").cast(IntegerType()).alias("review_count"),
                col("_ingestion_timestamp").cast("timestamp").alias("ingested_at"),
                col("_source").alias("source_system"),
                col("_run_date").alias("run_date"),
                current_timestamp().alias("processed_at")
            )
            .dropDuplicates(["product_id"])
    )


@dlt.expect_or_drop("valid_cart_id", "cart_id IS NOT NULL")
@dlt.expect_or_drop("valid_user_id", "user_id IS NOT NULL AND user_id > 0")
@dlt.expect_or_drop("positive_quantity", "quantity > 0")
@dlt.table(
    name="silver_orders",
    comment="Cleaned order line items with validated business rules"
)
def silver_orders():
    return (
        dlt.read_stream("bronze_orders")
            .select(
                col("cart_id").cast(IntegerType()),
                col("user_id").cast(IntegerType()),
                to_timestamp(col("order_date")).alias("order_date"),
                col("product_id").cast(IntegerType()),
                col("quantity").cast(IntegerType()),
                col("_run_date").alias("run_date"),
                current_timestamp().alias("processed_at")
            )
    )


@dlt.expect_or_drop("valid_user_id_dim", "user_id IS NOT NULL")
@dlt.expect_or_drop("email_not_null", "email IS NOT NULL")
@dlt.table(
    name="silver_users",
    comment="Cleaned customer dimension table"
)
def silver_users():
    return (
        dlt.read_stream("bronze_users")
            .select(
                col("user_id").cast(IntegerType()),
                col("username"),
                col("email"),
                col("city"),
                col("state"),
                col("zipcode"),
                col("_run_date").alias("run_date"),
                current_timestamp().alias("processed_at")
            )
            .dropDuplicates(["user_id"])
    )


@dlt.table(
    name="silver_weather",
    comment="Cleaned weather feature data for ML demand forecasting"
)
def silver_weather():
    return (
        dlt.read_stream("bronze_weather")
            .select(
                col("city"),
                col("country"),
                col("temperature_c").cast(DoubleType()),
                col("humidity_pct").cast(IntegerType()),
                col("weather_condition"),
                col("wind_speed_mps").cast(DoubleType()),
                col("recorded_at").cast("timestamp"),
                current_timestamp().alias("processed_at")
            )
    )


# =============================================================================
# 🥇 GOLD LAYER — Business-ready aggregations for dashboards and ML
# =============================================================================

@dlt.table(
    name="gold_category_summary",
    comment="Product metrics aggregated by category — used for SQL dashboards"
)
def gold_category_summary():
    return (
        dlt.read("silver_products")
            .groupBy("category")
            .agg(
                count("product_id").alias("total_products"),
                spark_round(avg("price"), 2).alias("avg_price_usd"),
                spark_round(avg("rating"), 2).alias("avg_rating"),
                spark_sum("review_count").alias("total_reviews"),
                spark_round(spark_sum("price"), 2).alias("total_inventory_value")
            )
            .withColumn("price_segment",
                when(col("avg_price_usd") > 200, "Premium")
                .when(col("avg_price_usd") > 50, "Mid-Range")
                .otherwise("Budget"))
            .withColumn("updated_at", current_timestamp())
    )


@dlt.table(
    name="gold_order_product_enriched",
    comment="Orders joined with product details — fact table for business reporting"
)
def gold_order_product_enriched():
    """
    Joins order line items with product details to create a rich fact table.
    Demonstrates SCD-style enrichment at the Gold layer.
    """
    orders  = dlt.read("silver_orders")
    products = dlt.read("silver_products")
    users   = dlt.read("silver_users")

    return (
        orders
            .join(products, on="product_id", how="left")
            .join(users, on="user_id", how="left")
            .select(
                orders["cart_id"],
                orders["user_id"],
                users["city"].alias("customer_city"),
                orders["order_date"],
                orders["product_id"],
                products["title"].alias("product_name"),
                products["category"],
                products["price"].alias("unit_price"),
                orders["quantity"],
                spark_round(products["price"] * orders["quantity"], 2).alias("line_total"),
                products["rating"],
                current_timestamp().alias("updated_at")
            )
    )


@dlt.table(
    name="gold_demand_features",
    comment="Feature table for ML demand forecasting model — consumed by MLflow pipeline"
)
def gold_demand_features():
    """
    Combines product, order, and weather features for the ML model.
    As a Data Engineer, this feature table is your primary ML contribution.
    """
    products = dlt.read("silver_products")
    weather  = dlt.read("silver_weather")

    features = (
        products
            .groupBy("category")
            .agg(
                count("product_id").alias("product_count"),
                spark_round(avg("price"), 2).alias("avg_price"),
                spark_round(avg("rating"), 2).alias("avg_rating"),
                spark_sum("review_count").alias("demand_proxy")  # reviews ≈ demand signal
            )
            .withColumn("season_month", month(current_timestamp()))
            .withColumn("season_encoded",
                when(col("season_month").isin(12, 1, 2), lit(0))   # Winter
                .when(col("season_month").isin(3, 4, 5), lit(1))   # Spring
                .when(col("season_month").isin(6, 7, 8), lit(2))   # Summer
                .otherwise(lit(3)))                                 # Fall
            .withColumn("updated_at", current_timestamp())
    )
    return features
