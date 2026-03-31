# Gold Layer - Business-Level Aggregations and Metrics
# Analytics-ready data for reporting and dashboards

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    count, countDistinct, sum as _sum, avg, min as _min, max as _max,
    col, round as _round, current_timestamp, when
)
# Product Summary by Price Category
@dp.materialized_view(
    comment="Gold layer: Product counts and price statistics by price category"
)
def gold_products_by_category():
    """
    Aggregated product metrics by price category:
    - Product counts
    - Average, min, max prices
    - Supplier counts
    """
    products = spark.read.table("silver_products").alias("p")
    suppliers = spark.read.table("silver_product_suppliers").alias("s")
    
    return (
        products
        .join(suppliers, col("p.product_id") == col("s.product_id"), "left")
        .groupBy(col("p.price_category"))
        .agg(
            count(col("p.product_id")).alias("product_count"),
            countDistinct(col("p.product_id")).alias("distinct_product_count"),
            _round(avg(col("p.price")), 2).alias("avg_price"),
            _round(_min(col("p.price")), 2).alias("min_price"),
            _round(_max(col("p.price")), 2).alias("max_price"),
            countDistinct(col("s.supplier_name")).alias("unique_suppliers"),
            current_timestamp().alias("calculated_at")
        )
        .orderBy(col("product_count").desc())
    )

# Supplier Summary
@dp.materialized_view(
    comment="Gold layer: Supplier metrics and product counts"
)
def gold_supplier_metrics():
    """
    Aggregated supplier metrics:
    - Product counts per supplier
    - Price statistics for supplier's products
    """
    products = spark.read.table("silver_products").alias("p")
    suppliers = spark.read.table("silver_product_suppliers").alias("s")
    
    return (
        suppliers
        .join(products, col("s.product_id") == col("p.product_id"), "inner")
        .groupBy(col("s.supplier_name_standardized"))
        .agg(
            count(col("s.product_id")).alias("product_count"),
            countDistinct(col("s.product_id")).alias("distinct_product_count"),
            _round(avg(col("p.price")), 2).alias("avg_product_price"),
            _round(_min(col("p.price")), 2).alias("min_product_price"),
            _round(_max(col("p.price")), 2).alias("max_product_price"),
            current_timestamp().alias("calculated_at")
        )
        .orderBy(col("product_count").desc())
    )

# Overall Product Summary
@dp.materialized_view(
    comment="Gold layer: Overall product portfolio summary"
)
def gold_product_summary():
    """
    High-level product portfolio metrics:
    - Total products
    - Total suppliers
    - Overall price statistics
    - Category distribution
    """
    products = spark.read.table("silver_products")
    suppliers = spark.read.table("silver_product_suppliers")
    
    # Overall metrics
    overall = products.agg(
        count(col("product_id")).alias("total_products"),
        _round(avg(col("price")), 2).alias("avg_price"),
        _round(_min(col("price")), 2).alias("min_price"),
        _round(_max(col("price")), 2).alias("max_price")
    )
    
    # Add supplier count
    supplier_count = suppliers.agg(
        countDistinct(col("supplier_name")).alias("total_suppliers")
    )
    
    return (
        overall
        .crossJoin(supplier_count)
        .withColumn("calculated_at", current_timestamp())
    )

# Product Change History Summary
@dp.materialized_view(
    comment="Gold layer: Product change frequency and history metrics"
)
def gold_product_change_summary():
    """
    Product change tracking metrics:
    - Products with most changes
    - Change frequency
    - Current vs historical versions
    """
    history = spark.read.table("silver_products_history")
    
    return (
        history
        .groupBy(col("product_id"))
        .agg(
            count("*").alias("version_count"),
            _min(col("valid_from")).alias("first_seen"),
            _max(col("valid_from")).alias("last_changed"),
            _sum(when(col("is_current"), 1).otherwise(0)).alias("is_active"),
            current_timestamp().alias("calculated_at")
        )
        .orderBy(col("version_count").desc())
    )
