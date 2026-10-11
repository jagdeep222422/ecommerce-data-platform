"""
Module: fetch_orders.py
Description: Ingests cart/order data from FakeStore API and writes to Bronze layer.
Covers: Paginated-style multi-user data ingestion, data flattening, Bronze landing.
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
OUTPUT_DIR = "./data/bronze/orders"


def fetch_with_retry(url: str, max_retries: int = 3) -> dict:
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.warning(f"Attempt {attempt + 1} failed: {e}")
            time.sleep(2 ** attempt)
    raise Exception(f"Failed to fetch {url} after {max_retries} retries")


def ingest_carts() -> dict:
    """
    Fetch all shopping carts (orders) and flatten product line items.
    Demonstrates data flattening / normalization before Bronze landing.
    """
    run_date = datetime.now().strftime("%Y-%m-%d")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # ── Step 1: Fetch all carts ──────────────────────────────────────────────
    carts = fetch_with_retry(f"{BASE_URL}/carts")
    logger.info(f"Fetched {len(carts)} carts")

    # ── Step 2: Flatten — one row per product in the cart ───────────────────
    order_line_items = []
    for cart in carts:
        for product in cart.get("products", []):
            order_line_items.append({
                "cart_id": cart["id"],
                "user_id": cart["userId"],
                "order_date": cart["date"],
                "product_id": product["productId"],
                "quantity": product["quantity"],
                "_ingestion_timestamp": datetime.now().isoformat(),
                "_source": "fakestoreapi.com/carts",
                "_run_date": run_date,
            })

    logger.info(f"Flattened into {len(order_line_items)} line items")

    # ── Step 3: Save to Bronze ───────────────────────────────────────────────
    os.makedirs(f"{OUTPUT_DIR}/{run_date}", exist_ok=True)
    output_path = f"{OUTPUT_DIR}/{run_date}/orders_{timestamp}.json"

    with open(output_path, "w") as f:
        json.dump(order_line_items, f, indent=2)

    logger.info(f"✅ Saved {len(order_line_items)} order line items to: {output_path}")
    return {
        "status": "success",
        "carts": len(carts),
        "line_items": len(order_line_items),
        "output_path": output_path,
    }


def ingest_users() -> dict:
    """
    Fetch all users — used as a dimension table for enriching orders in Silver.
    """
    run_date = datetime.now().strftime("%Y-%m-%d")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    users = fetch_with_retry(f"{BASE_URL}/users")

    # Remove sensitive/unnecessary fields, keep only what's needed
    clean_users = [
        {
            "user_id": u["id"],
            "username": u["username"],
            "email": u["email"],
            "city": u["address"]["city"],
            "state": u["address"].get("state", "unknown"),
            "zipcode": u["address"]["zipcode"],
            "_ingestion_timestamp": datetime.now().isoformat(),
            "_run_date": run_date,
        }
        for u in users
    ]

    os.makedirs(f"./data/bronze/users/{run_date}", exist_ok=True)
    output_path = f"./data/bronze/users/{run_date}/users_{timestamp}.json"

    with open(output_path, "w") as f:
        json.dump(clean_users, f, indent=2)

    logger.info(f"✅ Saved {len(clean_users)} users to: {output_path}")
    return {"status": "success", "users": len(clean_users), "output_path": output_path}


if __name__ == "__main__":
    print(json.dumps(ingest_carts(), indent=2))
    print(json.dumps(ingest_users(), indent=2))
