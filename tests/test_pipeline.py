"""
Tests for API ingestion modules.
Covers: Unit testing with pytest, mocking HTTP calls, validating data structure.
"""

import pytest
import json
from unittest.mock import patch, MagicMock
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ── Sample fixture data ────────────────────────────────────────────────────────
SAMPLE_PRODUCTS = [
    {
        "id": 1,
        "title": "Test Laptop",
        "price": 999.99,
        "category": "electronics",
        "description": "A great laptop",
        "rating": {"rate": 4.5, "count": 120},
    },
    {
        "id": 2,
        "title": "Gold Ring",
        "price": 250.0,
        "category": "jewelery",
        "description": "Beautiful ring",
        "rating": {"rate": 3.9, "count": 45},
    },
]

SAMPLE_CARTS = [
    {
        "id": 1,
        "userId": 1,
        "date": "2020-03-02T00:00:02.000Z",
        "products": [
            {"productId": 1, "quantity": 4},
            {"productId": 2, "quantity": 1},
        ],
    },
    {
        "id": 2,
        "userId": 2,
        "date": "2020-01-02T00:00:02.000Z",
        "products": [
            {"productId": 3, "quantity": 2},
        ],
    },
]


class TestFetchProducts:
    """Tests for fetch_products.py ingestion module."""

    @patch("requests.get")
    def test_ingest_products_success(self, mock_get, tmp_path):
        """Test successful product ingestion returns correct structure."""
        # Arrange: mock API response
        mock_response = MagicMock()
        mock_response.json.return_value = SAMPLE_PRODUCTS
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        # Temporarily point output to tmp_path
        import ingestion.fetch_products as fp
        original_dir = fp.OUTPUT_DIR
        fp.OUTPUT_DIR = str(tmp_path / "bronze" / "products")

        # Act
        result = fp.ingest_products()

        # Assert
        assert result["status"] == "success"
        assert result["records_ingested"] == 2
        assert "output_path" in result

        fp.OUTPUT_DIR = original_dir  # restore

    @patch("requests.get")
    def test_retry_on_failure(self, mock_get):
        """Test that fetch_with_retry retries on connection error."""
        from ingestion.fetch_products import fetch_with_retry

        # First call fails, second succeeds
        mock_get.side_effect = [
            __import__("requests").exceptions.ConnectionError("Connection refused"),
            MagicMock(json=lambda: SAMPLE_PRODUCTS, raise_for_status=lambda: None),
        ]

        result = fetch_with_retry("http://test.example.com/products", max_retries=3)
        assert result == SAMPLE_PRODUCTS
        assert mock_get.call_count == 2

    @patch("requests.get")
    def test_all_retries_fail_raises_exception(self, mock_get):
        """Test that exception is raised after all retries are exhausted."""
        import requests
        from ingestion.fetch_products import fetch_with_retry

        mock_get.side_effect = requests.exceptions.ConnectionError("Always fails")

        with pytest.raises(Exception, match="Failed to fetch"):
            fetch_with_retry("http://test.example.com", max_retries=2)

    @patch("requests.get")
    def test_metadata_added_to_products(self, mock_get, tmp_path):
        """Test that ingestion metadata is added to each product record."""
        mock_response = MagicMock()
        mock_response.json.return_value = [SAMPLE_PRODUCTS[0]]
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        import ingestion.fetch_products as fp
        fp.OUTPUT_DIR = str(tmp_path / "bronze" / "products")

        fp.ingest_products()

        # Read back the saved file
        import glob
        files = glob.glob(f"{fp.OUTPUT_DIR}/**/*.json", recursive=True)
        assert len(files) == 1

        with open(files[0]) as f:
            saved_data = json.load(f)

        assert "_ingestion_timestamp" in saved_data[0]
        assert saved_data[0]["_source"] == "fakestoreapi.com"
        assert "_run_date" in saved_data[0]


class TestFetchOrders:
    """Tests for fetch_orders.py ingestion module."""

    @patch("requests.get")
    def test_carts_flattened_to_line_items(self, mock_get, tmp_path):
        """Test that cart/product nesting is correctly flattened."""
        mock_response = MagicMock()
        mock_response.json.return_value = SAMPLE_CARTS
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        import ingestion.fetch_orders as fo
        fo.OUTPUT_DIR = str(tmp_path / "bronze" / "orders")

        result = fo.ingest_carts()

        # Cart 1 has 2 products, Cart 2 has 1 product → 3 line items total
        assert result["status"] == "success"
        assert result["carts"] == 2
        assert result["line_items"] == 3

    @patch("requests.get")
    def test_line_item_structure(self, mock_get, tmp_path):
        """Test that each line item has the required fields."""
        mock_response = MagicMock()
        mock_response.json.return_value = [SAMPLE_CARTS[0]]
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        import ingestion.fetch_orders as fo
        fo.OUTPUT_DIR = str(tmp_path / "bronze" / "orders")

        fo.ingest_carts()

        import glob
        files = glob.glob(f"{fo.OUTPUT_DIR}/**/*.json", recursive=True)
        with open(files[0]) as f:
            line_items = json.load(f)

        required_fields = {"cart_id", "user_id", "product_id", "quantity", "_ingestion_timestamp"}
        for item in line_items:
            assert required_fields.issubset(set(item.keys())), f"Missing fields in: {item}"
            assert item["quantity"] > 0


class TestDataQuality:
    """Tests for data quality check functions."""

    def test_all_valid_data_passes(self):
        """Test that clean data results in 100% quality score."""
        import pandas as pd
        from quality.dq_checks import run_basic_checks

        clean_df = pd.DataFrame({
            "product_id":   [1, 2, 3],
            "title":        ["A", "B", "C"],
            "price":        [10.0, 25.0, 99.99],
            "category":     ["electronics", "jewelery", "men's clothing"],
            "rating":       [4.5, 3.2, 4.8],
            "review_count": [100, 50, 200],
        })

        result = run_basic_checks(clean_df, "test_table")
        assert result["failed_checks"] == 0
        assert result["success_percent"] == 100.0
        assert result["status"] == "PASS"

    def test_null_values_detected(self):
        """Test that null values in required fields are flagged."""
        import pandas as pd
        from quality.dq_checks import run_basic_checks

        dirty_df = pd.DataFrame({
            "product_id":   [1, 2, None],         # null product_id
            "title":        ["A", None, "C"],      # null title
            "price":        [10.0, 25.0, -5.0],   # negative price
            "category":     ["electronics", "jewelery", "men's clothing"],
            "rating":       [4.5, 3.2, 4.8],
            "review_count": [100, 50, 200],
        })

        result = run_basic_checks(dirty_df, "test_table")
        assert result["failed_checks"] > 0
        assert result["status"] != "PASS"

    def test_duplicate_product_ids_detected(self):
        """Test that duplicate product_ids are caught by uniqueness check."""
        import pandas as pd
        from quality.dq_checks import run_basic_checks

        dup_df = pd.DataFrame({
            "product_id":   [1, 1, 3],   # duplicate id=1
            "title":        ["A", "A-copy", "C"],
            "price":        [10.0, 10.0, 99.99],
            "category":     ["electronics", "electronics", "men's clothing"],
            "rating":       [4.5, 4.5, 4.8],
            "review_count": [100, 100, 200],
        })

        result = run_basic_checks(dup_df, "test_table")
        # Find the uniqueness check result
        dup_check = next(c for c in result["check_details"] if "unique" in c["check"])
        assert not dup_check["passed"]


class TestFeatureEngineering:
    """Tests for ML feature engineering module."""

    def test_feature_table_has_correct_columns(self):
        """Test that all required feature columns are present."""
        import pandas as pd
        from ml.feature_engineering import build_features_pandas, get_feature_columns

        sample = pd.DataFrame({
            "product_id":   [1, 2, 3],
            "title":        ["A", "B", "C"],
            "price":        [10.0, 200.0, 50.0],
            "category":     ["electronics", "electronics", "jewelery"],
            "rating":       [4.5, 3.2, 4.8],
            "review_count": [100, 50, 200],
        })

        features = build_features_pandas(sample)
        required_cols = get_feature_columns()

        for col in required_cols:
            assert col in features.columns, f"Missing feature column: {col}"

    def test_demand_score_between_0_and_1(self):
        """Test that demand score is normalized to [0, 1] range."""
        import pandas as pd
        from ml.feature_engineering import build_features_pandas

        sample = pd.DataFrame({
            "product_id":   [1, 2, 3, 4],
            "title":        ["A", "B", "C", "D"],
            "price":        [10.0, 200.0, 50.0, 75.0],
            "category":     ["electronics", "electronics", "jewelery", "jewelery"],
            "rating":       [4.5, 3.2, 4.8, 4.0],
            "review_count": [100, 50, 200, 80],
        })

        features = build_features_pandas(sample)
        assert (features["demand_score_normalized"] >= 0).all()
        assert (features["demand_score_normalized"] <= 1).all()

    def test_season_encoding_valid_range(self):
        """Test that season_encoded is always 0-3."""
        import pandas as pd
        from ml.feature_engineering import build_features_pandas

        sample = pd.DataFrame({
            "product_id":   [1, 2],
            "title":        ["A", "B"],
            "price":        [10.0, 20.0],
            "category":     ["electronics", "jewelery"],
            "rating":       [4.5, 3.2],
            "review_count": [100, 50],
        })

        features = build_features_pandas(sample)
        assert features["season_encoded"].between(0, 3).all()
