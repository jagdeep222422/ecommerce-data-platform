"""
Module: fetch_products.py
Description: Ingests product data from FakeStore API and writes raw JSON to storage.
Covers: REST API ingestion, error handling, retry logic, Bronze layer landing.
"""

import requests
import json
import os
import time
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://fakestoreapi.com"
OUTPUT_DIR = "./data/bronze/products"  # Replace with /mnt/datalake/bronze/products on Databricks


def fetch_with_retry(url: str, max_retries: int = 3, timeout: int = 10) -> dict:
    """
    Fetch data from a URL with exponential backoff retry logic.
    """
    for attempt in range(max_retries):
        try:
            logger.info(f"Fetching: {url} (attempt {attempt + 1}/{max_retries})")
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()  # Raises HTTPError for 4xx/5xx responses
            return response.json()
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error: {e}")
        except requests.exceptions.ConnectionError:
            logger.warning("Connection error. Retrying...")
        except requests.exceptions.Timeout:
            logger.warning("Request timed out. Retrying...")
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")

        wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
        logger.info(f"Waiting {wait_time}s before retry...")
        time.sleep(wait_time)

    raise Exception(f"Failed to fetch {url} after {max_retries} attempts")


def ingest_products() -> dict:
    """
    Fetch all products from FakeStore API and save to Bronze layer.
    Returns summary metadata.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_date = datetime.now().strftime("%Y-%m-%d")

    # ── Step 1: Fetch all products ──────────────────────────────────────────
    products = fetch_with_retry(f"{BASE_URL}/products")
    logger.info(f"Fetched {len(products)} products")

    # ── Step 2: Enrich with metadata ────────────────────────────────────────
    for product in products:
        product["_ingestion_timestamp"] = datetime.now().isoformat()
        product["_source"] = "fakestoreapi.com"
        product["_run_date"] = run_date

    # ── Step 3: Save to Bronze layer ────────────────────────────────────────
    os.makedirs(f"{OUTPUT_DIR}/{run_date}", exist_ok=True)
    output_path = f"{OUTPUT_DIR}/{run_date}/products_{timestamp}.json"

    with open(output_path, "w") as f:
        json.dump(products, f, indent=2)

    logger.info(f"✅ Saved {len(products)} products to: {output_path}")

    return {
        "status": "success",
        "records_ingested": len(products),
        "output_path": output_path,
        "timestamp": timestamp,
    }


def ingest_products_by_category() -> dict:
    """
    Fetch products per category — demonstrates pagination-style multi-call ingestion.
    """
    categories = fetch_with_retry(f"{BASE_URL}/products/categories")
    logger.info(f"Found {len(categories)} categories: {categories}")

    all_data = {}
    for category in categories:
        encoded = category.replace("'", "%27").replace(" ", "%20")
        products = fetch_with_retry(f"{BASE_URL}/products/category/{encoded}")
        all_data[category] = products
        logger.info(f"  → {category}: {len(products)} products")
        time.sleep(0.5)  # Respect rate limits

    # Save enriched category data
    run_date = datetime.now().strftime("%Y-%m-%d")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    os.makedirs(f"{OUTPUT_DIR}/{run_date}", exist_ok=True)
    output_path = f"{OUTPUT_DIR}/{run_date}/products_by_category_{timestamp}.json"

    with open(output_path, "w") as f:
        json.dump(all_data, f, indent=2)

    logger.info(f"✅ Saved category breakdown to: {output_path}")
    return {"status": "success", "categories": len(categories), "output_path": output_path}


if __name__ == "__main__":
    result = ingest_products()
    print(json.dumps(result, indent=2))

    result2 = ingest_products_by_category()
    print(json.dumps(result2, indent=2))
