"""
Module: feature_engineering.py
Description: Builds the ML feature table from Silver/Gold layer data.
This is the Data Engineer's primary contribution to the ML pipeline.
Covers: PySpark feature engineering, Delta feature tables, MLflow data tracking.
"""

import json
import logging
import pandas as pd
import numpy as np
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def build_features_pandas(products_df: pd.DataFrame, weather_df: pd.DataFrame = None) -> pd.DataFrame:
    """
    Build ML feature table from Silver layer data using pandas.
    On Databricks, replace with PySpark transformations on Delta tables.

    Feature Engineering steps:
    1. Category-level aggregations (product count, avg price, avg rating)
    2. Season encoding (cyclical feature for time of year)
    3. Price bucket encoding
    4. Weather enrichment (if available)

    Args:
        products_df: Silver products DataFrame
        weather_df:  Silver weather DataFrame (optional)
    Returns:
        Feature DataFrame ready for ML training
    """
    logger.info("Building ML feature table...")

    # ── Step 1: Category-level aggregations ─────────────────────────────────
    features = (
        products_df
            .groupby("category")
            .agg(
                product_count=("product_id", "count"),
                avg_price=("price", "mean"),
                max_price=("price", "max"),
                min_price=("price", "min"),
                price_std=("price", "std"),
                avg_rating=("rating", "mean"),
                total_reviews=("review_count", "sum"),
                demand_proxy=("review_count", "sum"),  # reviews ~ demand signal
            )
            .reset_index()
    )

    # ── Step 2: Season encoding ──────────────────────────────────────────────
    current_month = datetime.now().month
    season_map = {12: 0, 1: 0, 2: 0,   # Winter = 0
                  3: 1, 4: 1, 5: 1,     # Spring = 1
                  6: 2, 7: 2, 8: 2,     # Summer = 2
                  9: 3, 10: 3, 11: 3}   # Fall   = 3
    features["season_encoded"] = season_map.get(current_month, 0)
    features["season_name"] = ["Winter", "Spring", "Summer", "Fall"][features["season_encoded"].iloc[0]]

    # ── Step 3: Price bucket encoding ───────────────────────────────────────
    features["price_bucket"] = pd.cut(
        features["avg_price"],
        bins=[0, 20, 50, 100, 500, float("inf")],
        labels=[0, 1, 2, 3, 4],  # Ordinal encoding for ML
        right=True
    ).astype(int)

    # ── Step 4: Demand score normalization (target variable proxy) ───────────
    max_reviews = features["total_reviews"].max()
    features["demand_score_normalized"] = features["demand_proxy"] / max_reviews

    # ── Step 5: Weather enrichment (if available) ────────────────────────────
    if weather_df is not None and not weather_df.empty:
        # Use average temperature across cities as a global weather signal
        avg_temp = weather_df["temperature_c"].mean()
        avg_humidity = weather_df["humidity_pct"].mean()
        features["avg_temperature_c"] = round(avg_temp, 1)
        features["avg_humidity_pct"] = round(avg_humidity, 1)
        logger.info(f"Weather features added: temp={avg_temp:.1f}°C, humidity={avg_humidity:.1f}%")
    else:
        # Default values when weather data unavailable
        features["avg_temperature_c"] = 20.0
        features["avg_humidity_pct"] = 65.0

    # ── Step 6: Add metadata ─────────────────────────────────────────────────
    features["feature_created_at"] = datetime.now().isoformat()
    features["feature_version"] = "v1.0"

    # Round numeric columns for cleanliness
    numeric_cols = ["avg_price", "max_price", "min_price", "price_std", "avg_rating"]
    features[numeric_cols] = features[numeric_cols].round(2)

    logger.info(f"✅ Feature table built: {len(features)} rows, {len(features.columns)} features")
    logger.info(f"   Categories: {list(features['category'])}")
    logger.info(f"   Features: {list(features.columns)}")

    return features


def get_feature_columns() -> list:
    """
    Returns the list of feature columns used for ML training.
    Keeping this centralized ensures training and scoring use the same features.
    """
    return [
        "product_count",
        "avg_price",
        "max_price",
        "min_price",
        "avg_rating",
        "price_bucket",
        "season_encoded",
        "avg_temperature_c",
        "avg_humidity_pct",
    ]


def get_target_column() -> str:
    return "demand_score_normalized"


def save_features(features_df: pd.DataFrame, output_path: str = "./data/features/demand_features.csv"):
    """
    Save feature table to CSV for local development.
    On Databricks: features_df.write.format("delta").saveAsTable("features.demand_features")
    """
    import os
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    features_df.to_csv(output_path, index=False)
    logger.info(f"✅ Features saved to: {output_path}")
    return output_path


if __name__ == "__main__":
    import sys
    sys.path.insert(0, ".")

    # ── Load sample data (replace with Delta tables on Databricks) ──────────
    sample_products = pd.DataFrame({
        "product_id":    [1, 2, 3, 4, 5, 6, 7, 8],
        "title":         ["Laptop", "Ring", "T-Shirt", "Jacket", "Headphones", "Necklace", "Jeans", "Dress"],
        "price":         [999.99, 250.0, 19.99, 89.99, 149.99, 75.0, 49.99, 65.0],
        "category":      ["electronics", "jewelery", "men's clothing", "men's clothing",
                          "electronics", "jewelery", "men's clothing", "women's clothing"],
        "rating":        [4.5, 3.9, 4.1, 4.3, 4.6, 3.7, 4.0, 4.4],
        "review_count":  [320, 45, 210, 155, 487, 62, 189, 243],
    })

    sample_weather = pd.DataFrame({
        "city":            ["New York", "London", "Mumbai"],
        "temperature_c":   [18.5, 12.0, 31.0],
        "humidity_pct":    [65, 78, 82],
        "weather_condition": ["Clear", "Clouds", "Clear"],
    })

    features = build_features_pandas(sample_products, sample_weather)
    output_path = save_features(features)

    print("\n📊 Feature Table Preview:")
    print(features.to_string(index=False))
    print(f"\n🎯 Feature columns for ML: {get_feature_columns()}")
    print(f"🎯 Target column: {get_target_column()}")
