"""
Module: dq_checks.py
Description: Batch data quality validation using Great Expectations.
Runs AFTER the DLT pipeline to validate Silver layer tables.
Results are logged to a Delta monitoring table for trend tracking.

Install: pip install great-expectations
"""

import json
import logging
from datetime import date, datetime
from typing import Dict, Any

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def run_product_quality_checks(df) -> Dict[str, Any]:
    """
    Run Great Expectations checks on silver_products.
    
    Args:
        df: pandas DataFrame of silver_products table
    Returns:
        dict with quality metrics and results
    """
    try:
        import great_expectations as gx

        context = gx.get_context()

        # Build validator from pandas DataFrame
        validator = context.sources.pandas_default.read_dataframe(
            df,
            asset_name="silver_products"
        )

        # ── Completeness checks ──────────────────────────────────────
        validator.expect_column_to_exist("product_id")
        validator.expect_column_to_exist("title")
        validator.expect_column_to_exist("price")
        validator.expect_column_to_exist("category")
        validator.expect_column_to_exist("rating")

        # ── Null checks ──────────────────────────────────────────────
        validator.expect_column_values_to_not_be_null("product_id")
        validator.expect_column_values_to_not_be_null("title")
        validator.expect_column_values_to_not_be_null("price")
        validator.expect_column_values_to_not_be_null("category")

        # ── Uniqueness checks ────────────────────────────────────────
        validator.expect_column_values_to_be_unique("product_id")

        # ── Range / value checks ─────────────────────────────────────
        validator.expect_column_values_to_be_between("price", min_value=0.01, max_value=10000)
        validator.expect_column_values_to_be_between("rating", min_value=0, max_value=5)
        validator.expect_column_values_to_be_between("review_count", min_value=0, max_value=100000)

        # ── Categorical checks ───────────────────────────────────────
        validator.expect_column_values_to_be_in_set(
            "category",
            ["electronics", "jewelery", "men's clothing", "women's clothing"]
        )

        # ── Type checks ──────────────────────────────────────────────
        validator.expect_column_values_to_be_of_type("price", "float")
        validator.expect_column_values_to_be_of_type("product_id", "int")

        # ── Run all checks ───────────────────────────────────────────
        results = validator.validate()

        success_pct = results.statistics["success_percent"]
        passed = results.statistics["successful_expectations"]
        failed = results.statistics["unsuccessful_expectations"]
        total  = results.statistics["evaluated_expectations"]

        logger.info(f"✅ Data Quality: {success_pct:.1f}% ({passed}/{total} checks passed)")
        if failed > 0:
            logger.warning(f"⚠️  {failed} check(s) FAILED — review data quality report")

        return {
            "table": "silver_products",
            "run_date": str(date.today()),
            "run_timestamp": datetime.now().isoformat(),
            "total_checks": total,
            "passed_checks": passed,
            "failed_checks": failed,
            "success_percent": round(success_pct, 2),
            "status": "PASS" if success_pct >= 95 else "WARN" if success_pct >= 80 else "FAIL",
        }

    except ImportError:
        logger.warning("great_expectations not installed. Running basic checks instead.")
        return run_basic_checks(df, "silver_products")


def run_basic_checks(df, table_name: str) -> Dict[str, Any]:
    """
    Fallback basic data quality checks without Great Expectations.
    Works in any Python environment (no extra installs).
    """
    checks = []
    
    # Null checks
    for col in ["product_id", "title", "price", "category"]:
        if col in df.columns:
            null_count = df[col].isnull().sum()
            checks.append({
                "check": f"no_nulls_{col}",
                "passed": bool(null_count == 0),
                "detail": f"{null_count} nulls found"
            })

    # Uniqueness
    if "product_id" in df.columns:
        dup_count = df["product_id"].duplicated().sum()
        checks.append({
            "check": "unique_product_id",
            "passed": bool(dup_count == 0),
            "detail": f"{dup_count} duplicates found"
        })

    # Range checks
    if "price" in df.columns:
        invalid_price = (df["price"] <= 0).sum()
        checks.append({
            "check": "positive_price",
            "passed": bool(invalid_price == 0),
            "detail": f"{invalid_price} rows with price <= 0"
        })

    if "rating" in df.columns:
        invalid_rating = ((df["rating"] < 0) | (df["rating"] > 5)).sum()
        checks.append({
            "check": "valid_rating_range",
            "passed": bool(invalid_rating == 0),
            "detail": f"{invalid_rating} rows with rating out of 0-5 range"
        })

    passed = sum(1 for c in checks if c["passed"])
    total  = len(checks)
    success_pct = (passed / total * 100) if total > 0 else 0

    for check in checks:
        icon = "✅" if check["passed"] else "❌"
        logger.info(f"  {icon} {check['check']}: {check['detail']}")

    logger.info(f"\n📊 Quality Score: {success_pct:.1f}% ({passed}/{total} passed)")

    return {
        "table": table_name,
        "run_date": str(date.today()),
        "run_timestamp": datetime.now().isoformat(),
        "total_checks": total,
        "passed_checks": passed,
        "failed_checks": total - passed,
        "success_percent": round(success_pct, 2),
        "status": "PASS" if success_pct >= 95 else "WARN" if success_pct >= 80 else "FAIL",
        "check_details": checks,
    }


def save_dq_results(results: Dict[str, Any], output_path: str = "./data/dq_log.json"):
    """
    Save DQ results to a JSON log file.
    On Databricks, this would write to a Delta table for trend analysis.
    """
    import os
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Append to existing log
    try:
        with open(output_path, "r") as f:
            log = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        log = []

    log.append(results)

    with open(output_path, "w") as f:
        json.dump(log, f, indent=2)

    logger.info(f"📝 DQ results saved to: {output_path}")


if __name__ == "__main__":
    import pandas as pd

    # ── Load sample data (replace with spark.table() on Databricks) ─────────
    try:
        from ingestion.fetch_products import ingest_products
        import glob

        ingest_products()
        files = glob.glob("./data/bronze/products/**/*.json", recursive=True)
        if files:
            with open(files[0]) as f:
                raw = json.load(f)
            df = pd.DataFrame(raw)
            # Normalize nested 'rating' field
            df["rating"] = df["rating"].apply(lambda x: x["rate"] if isinstance(x, dict) else x)
            df["review_count"] = df.get("rating", {})
            df.rename(columns={"id": "product_id"}, inplace=True)
        else:
            raise FileNotFoundError("No data found")
    except Exception:
        # Demo with sample data
        df = pd.DataFrame({
            "product_id": [1, 2, 3, 4, 5],
            "title": ["Product A", "Product B", "Product C", "Product D", None],
            "price": [29.99, 0.0, 149.99, 9.99, 49.99],
            "category": ["electronics", "jewelery", "men's clothing", "unknown", "women's clothing"],
            "rating": [4.5, 3.2, 5.1, 2.0, 4.8],
            "review_count": [120, 45, 200, 10, 88],
        })
        logger.info("Using sample demo data")

    # Run checks
    results = run_basic_checks(df, "silver_products")
    save_dq_results(results)
    print("\n" + "="*50)
    print(json.dumps(results, indent=2))
