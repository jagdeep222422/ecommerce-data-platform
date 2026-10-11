-- ============================================================
-- Databricks SQL Dashboard Queries
-- E-Commerce Analytics & Demand Forecasting Platform
-- ============================================================
-- Run these queries in Databricks SQL Editor
-- Connect to SQL Warehouse, then create dashboard visualizations
-- ============================================================


-- ────────────────────────────────────────────────────────────
-- DASHBOARD 1: Category Performance Overview
-- Chart type: Bar chart (category vs estimated_revenue)
-- ────────────────────────────────────────────────────────────
SELECT
    category,
    total_products,
    avg_price_usd,
    avg_rating,
    total_reviews,
    total_inventory_value,
    price_segment,
    RANK() OVER (ORDER BY total_inventory_value DESC) AS revenue_rank
FROM gold_category_summary
ORDER BY total_inventory_value DESC;


-- ────────────────────────────────────────────────────────────
-- DASHBOARD 2: Top 10 Highest Rated Products
-- Chart type: Table with conditional formatting
-- ────────────────────────────────────────────────────────────
SELECT
    product_id,
    title             AS product_name,
    category,
    ROUND(price, 2)   AS price_usd,
    rating,
    review_count,
    CASE
        WHEN rating >= 4.5 THEN '⭐ Excellent'
        WHEN rating >= 4.0 THEN '✅ Good'
        WHEN rating >= 3.0 THEN '🔶 Average'
        ELSE '❌ Poor'
    END               AS rating_label
FROM silver_products
WHERE rating IS NOT NULL
ORDER BY rating DESC, review_count DESC
LIMIT 10;


-- ────────────────────────────────────────────────────────────
-- DASHBOARD 3: Price Distribution by Category
-- Chart type: Box plot / histogram
-- ────────────────────────────────────────────────────────────
SELECT
    category,
    COUNT(product_id)              AS product_count,
    ROUND(MIN(price), 2)           AS min_price,
    ROUND(AVG(price), 2)           AS avg_price,
    ROUND(MAX(price), 2)           AS max_price,
    ROUND(STDDEV(price), 2)        AS price_std,
    ROUND(PERCENTILE(price, 0.25), 2) AS p25_price,
    ROUND(PERCENTILE(price, 0.50), 2) AS median_price,
    ROUND(PERCENTILE(price, 0.75), 2) AS p75_price
FROM silver_products
GROUP BY category
ORDER BY avg_price DESC;


-- ────────────────────────────────────────────────────────────
-- DASHBOARD 4: Order Activity by User (Top Customers)
-- Chart type: Bar chart
-- ────────────────────────────────────────────────────────────
SELECT
    o.user_id,
    u.city                                    AS customer_city,
    COUNT(DISTINCT o.cart_id)                 AS total_orders,
    COUNT(o.product_id)                       AS total_items_ordered,
    SUM(o.quantity)                           AS total_quantity,
    ROUND(SUM(p.price * o.quantity), 2)       AS total_spend_usd
FROM silver_orders o
LEFT JOIN silver_users   u ON o.user_id    = u.user_id
LEFT JOIN silver_products p ON o.product_id = p.product_id
GROUP BY o.user_id, u.city
ORDER BY total_spend_usd DESC
LIMIT 20;


-- ────────────────────────────────────────────────────────────
-- DASHBOARD 5: Category Demand Score (ML feature preview)
-- Chart type: Horizontal bar chart
-- ────────────────────────────────────────────────────────────
SELECT
    category,
    product_count,
    avg_price,
    avg_rating,
    demand_proxy                                                          AS total_reviews,
    ROUND(demand_proxy / MAX(demand_proxy) OVER (), 4)                    AS demand_score,
    CASE
        WHEN demand_proxy / MAX(demand_proxy) OVER () >= 0.7 THEN '🔥 High Demand'
        WHEN demand_proxy / MAX(demand_proxy) OVER () >= 0.4 THEN '📈 Medium Demand'
        ELSE '📉 Low Demand'
    END                                                                   AS demand_tier
FROM gold_demand_features
ORDER BY demand_score DESC;


-- ────────────────────────────────────────────────────────────
-- DASHBOARD 6: Data Quality Monitoring (Pipeline Health)
-- Chart type: Line chart (success_percent over time)
-- ────────────────────────────────────────────────────────────
-- Note: dq_monitoring_log is written by quality/dq_checks.py
SELECT
    run_date,
    table_name,
    total_checks,
    passed_checks,
    failed_checks,
    success_percent,
    CASE
        WHEN success_percent >= 95 THEN '🟢 Healthy'
        WHEN success_percent >= 80 THEN '🟡 Warning'
        ELSE '🔴 Critical'
    END AS pipeline_health
FROM dq_monitoring_log
ORDER BY run_date DESC;


-- ────────────────────────────────────────────────────────────
-- DASHBOARD 7: Products Never Ordered (Inventory Alert)
-- Chart type: Table (business insight)
-- ────────────────────────────────────────────────────────────
SELECT
    p.product_id,
    p.title    AS product_name,
    p.category,
    p.price    AS price_usd,
    p.rating,
    'Never Ordered' AS order_status
FROM silver_products p
LEFT JOIN silver_orders o ON p.product_id = o.product_id
WHERE o.product_id IS NULL
ORDER BY p.price DESC;


-- ────────────────────────────────────────────────────────────
-- DASHBOARD 8: Delta Table Optimization Check
-- Run this in Databricks SQL to optimize Gold tables for performance
-- ────────────────────────────────────────────────────────────
-- OPTIMIZE gold_category_summary ZORDER BY (category);
-- OPTIMIZE gold_order_product_enriched ZORDER BY (category, order_date);
-- VACUUM silver_products RETAIN 168 HOURS;   -- Keep 7 days of history

-- Check Delta table history (Time Travel)
-- DESCRIBE HISTORY silver_products;
-- SELECT * FROM silver_products VERSION AS OF 1;
-- SELECT * FROM silver_products TIMESTAMP AS OF '2024-01-01';
