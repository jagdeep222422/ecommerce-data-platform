"""
Module: demand_forecast.py
Description: Trains a demand forecasting model with full MLflow experiment tracking.
Covers: MLflow Tracking, Model Registry, feature logging, model versioning.

On Databricks:
  - MLflow is pre-installed and connected to your workspace
  - Replace pandas DataFrames with spark.table("gold_demand_features")
  - Models are registered in Unity Catalog Model Registry
"""

import mlflow
import mlflow.sklearn
import pandas as pd
import numpy as np
import json
import logging
import os
from datetime import datetime
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.linear_model import Ridge
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ── MLflow Configuration ─────────────────────────────────────────────────────
EXPERIMENT_NAME = "ecommerce-demand-forecast"
MODEL_NAME = "EcommerceDemandForecastModel"
# SQLite backend (local dev). On Databricks, MLflow is auto-configured.
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "sqlite:///mlflow.db")

mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
os.environ["MLFLOW_ALLOW_FILE_STORE"] = "false"  # ensure DB mode
mlflow.set_experiment(EXPERIMENT_NAME)


def load_features(feature_path: str = "./data/features/demand_features.csv") -> pd.DataFrame:
    """Load feature table. On Databricks, replace with spark.table()"""
    if os.path.exists(feature_path):
        df = pd.read_csv(feature_path)
        logger.info(f"Loaded features from: {feature_path} ({len(df)} rows)")
        return df
    else:
        # Fallback: generate sample features for demo
        logger.warning("Feature file not found. Generating sample data for demo.")
        return _generate_sample_features()


def _generate_sample_features() -> pd.DataFrame:
    """Generate synthetic feature data for demonstration."""
    np.random.seed(42)
    categories = ["electronics", "jewelery", "men's clothing", "women's clothing"]
    data = []
    for _ in range(200):  # 200 sample rows (simulate multiple runs/time periods)
        cat = np.random.choice(categories)
        price = np.random.uniform(10, 500)
        rating = np.random.uniform(2.5, 5.0)
        season = np.random.randint(0, 4)
        temp = np.random.uniform(5, 35)
        humidity = np.random.uniform(40, 90)
        # Simulate demand: higher rating + moderate price + summer = higher demand
        demand = (rating * 0.4) + (1 / (price + 1) * 100) + (0.1 if season == 2 else 0) + np.random.normal(0, 0.05)
        data.append({
            "category": cat,
            "product_count": np.random.randint(2, 10),
            "avg_price": round(price, 2),
            "max_price": round(price * 1.5, 2),
            "min_price": round(price * 0.5, 2),
            "avg_rating": round(rating, 2),
            "price_bucket": min(int(price / 100), 4),
            "season_encoded": season,
            "avg_temperature_c": round(temp, 1),
            "avg_humidity_pct": round(humidity, 1),
            "demand_score_normalized": round(min(max(demand, 0), 1), 4),
        })
    return pd.DataFrame(data)


def evaluate_model(model, X_test: pd.DataFrame, y_test: pd.Series) -> dict:
    """Calculate and return key regression metrics."""
    predictions = model.predict(X_test)
    return {
        "rmse":  round(np.sqrt(mean_squared_error(y_test, predictions)), 4),
        "mae":   round(mean_absolute_error(y_test, predictions), 4),
        "r2":    round(r2_score(y_test, predictions), 4),
        "mape":  round(np.mean(np.abs((y_test - predictions) / (y_test + 1e-9))) * 100, 2),
    }


def train_and_log(
    features_df: pd.DataFrame,
    model_type: str = "random_forest",
    run_name: str = None,
) -> str:
    """
    Train a model and log everything to MLflow.

    Args:
        features_df: Feature DataFrame from feature engineering
        model_type:  One of 'random_forest', 'gradient_boosting', 'ridge'
        run_name:    Optional custom run name
    Returns:
        MLflow run_id of the logged model
    """
    from ml.feature_engineering import get_feature_columns, get_target_column

    feature_cols = get_feature_columns()
    target_col = get_target_column()

    # ── Prepare data ─────────────────────────────────────────────────────────
    available_features = [c for c in feature_cols if c in features_df.columns]
    X = features_df[available_features]
    y = features_df[target_col]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # ── Select model ─────────────────────────────────────────────────────────
    models = {
        "random_forest": RandomForestRegressor(n_estimators=100, max_depth=6, random_state=42),
        "gradient_boosting": GradientBoostingRegressor(n_estimators=100, learning_rate=0.1, random_state=42),
        "ridge": Pipeline([("scaler", StandardScaler()), ("model", Ridge(alpha=1.0))]),
    }
    model = models.get(model_type, models["random_forest"])

    # ── MLflow Run ────────────────────────────────────────────────────────────
    run_name = run_name or f"{model_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    with mlflow.start_run(run_name=run_name):
        run_id = mlflow.active_run().info.run_id
        logger.info(f"🚀 MLflow Run started: {run_id}")

        # ── Log parameters ────────────────────────────────────────────────────
        mlflow.log_param("model_type", model_type)
        mlflow.log_param("train_size", len(X_train))
        mlflow.log_param("test_size", len(X_test))
        mlflow.log_param("features_used", json.dumps(available_features))
        mlflow.log_param("feature_count", len(available_features))
        mlflow.log_param("target_column", target_col)

        if model_type == "random_forest":
            mlflow.log_param("n_estimators", 100)
            mlflow.log_param("max_depth", 6)
        elif model_type == "gradient_boosting":
            mlflow.log_param("n_estimators", 100)
            mlflow.log_param("learning_rate", 0.1)
        elif model_type == "ridge":
            mlflow.log_param("alpha", 1.0)

        # ── Train model ───────────────────────────────────────────────────────
        logger.info(f"Training {model_type} model on {len(X_train)} samples...")
        model.fit(X_train, y_train)

        # ── Evaluate ──────────────────────────────────────────────────────────
        metrics = evaluate_model(model, X_test, y_test)
        for metric_name, value in metrics.items():
            mlflow.log_metric(metric_name, value)

        # Cross-validation score (min 2 folds, max 5)
        n_splits = max(2, min(5, len(X)))
        cv_scores = cross_val_score(model, X, y, cv=n_splits, scoring="r2")
        mlflow.log_metric("cv_r2_mean", round(float(cv_scores.mean()), 4))
        mlflow.log_metric("cv_r2_std", round(float(cv_scores.std()), 4))

        logger.info(f"📊 Metrics: RMSE={metrics['rmse']}, MAE={metrics['mae']}, R²={metrics['r2']}")

        # ── Log model ─────────────────────────────────────────────────────────
        # skops_trusted_types needed for tree-based sklearn models (MLflow 3.x security)
        TRUSTED_SKLEARN_TYPES = [
            "sklearn.tree._tree.Tree",
            "sklearn.ensemble._forest.RandomForestRegressor",
            "sklearn.ensemble._gb.GradientBoostingRegressor",
            "sklearn.linear_model._ridge.Ridge",
            "sklearn.preprocessing._data.StandardScaler",
            "sklearn.pipeline.Pipeline",
            "numpy.dtype",
            "numpy.ndarray",
            "builtins.float",
            "builtins.int",
            "builtins.bool",
        ]
        mlflow.sklearn.log_model(
            sk_model=model,
            artifact_path="demand_model",
            registered_model_name=MODEL_NAME,  # Auto-registers in Model Registry
            input_example=X_test.head(5),
            skops_trusted_types=TRUSTED_SKLEARN_TYPES,
        )

        # ── Log feature importance (for tree-based models) ────────────────────
        if hasattr(model, "feature_importances_"):
            importance_df = pd.DataFrame({
                "feature": available_features,
                "importance": model.feature_importances_,
            }).sort_values("importance", ascending=False)

            importance_path = "/tmp/feature_importance.csv"
            importance_df.to_csv(importance_path, index=False)
            mlflow.log_artifact(importance_path, "feature_analysis")
            logger.info(f"🔍 Top feature: {importance_df.iloc[0]['feature']} "
                       f"(importance={importance_df.iloc[0]['importance']:.3f})")

        logger.info(f"✅ Model logged to MLflow. Run ID: {run_id}")
        return run_id


def run_all_experiments(features_df: pd.DataFrame):
    """
    Train multiple model types and compare in MLflow UI.
    Demonstrates model comparison — a key MLflow workflow.
    """
    logger.info("🧪 Running all model experiments...")
    results = {}

    for model_type in ["random_forest", "gradient_boosting", "ridge"]:
        logger.info(f"\n--- Training: {model_type} ---")
        run_id = train_and_log(features_df, model_type=model_type)
        results[model_type] = run_id

    logger.info(f"\n✅ All experiments complete!")
    logger.info(f"   View results: mlflow ui --port 5000")
    return results


if __name__ == "__main__":
    import sys
    sys.path.insert(0, ".")

    # Load features
    features_df = load_features()
    logger.info(f"Feature table shape: {features_df.shape}")
    logger.info(f"Columns: {list(features_df.columns)}")

    # Run experiments
    results = run_all_experiments(features_df)

    print("\n" + "=" * 60)
    print("📊 MLflow Experiment Summary")
    print("=" * 60)
    for model, run_id in results.items():
        print(f"  {model}: run_id={run_id}")
    print(f"\n🔗 View UI: mlflow ui")
    print(f"   Experiment: {EXPERIMENT_NAME}")
    print(f"   Model Registry: {MODEL_NAME}")
