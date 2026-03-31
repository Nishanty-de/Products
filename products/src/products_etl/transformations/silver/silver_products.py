# Silver Layer - Cleaned and Conformed Product Data
# Flattens nested structures and applies data quality rules

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, explode, trim, upper, when, coalesce, 
    current_timestamp, regexp_replace
)

# Silver Products Table
# Reads from bronze using BATCH semantics (spark.read) for materialized view
@dp.materialized_view(
    comment="Silver layer: Cleaned and conformed product data (current state only)",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true"
    }
)
@dp.expect_or_drop("valid_product_id", "product_id IS NOT NULL AND product_id > 0")
@dp.expect_or_drop("valid_product_name", "product_name IS NOT NULL AND LENGTH(TRIM(product_name)) > 0")
@dp.expect("valid_price", "price IS NULL OR price >= 0")
def silver_products():
    """
    Cleaned product data with:
    - Data quality checks (non-null IDs, valid names, non-negative prices)
    - Standardized text (trimmed, uppercase names)
    - Derived fields (price categories)
    """
    return (
        spark.read.table("bronze_products")
        .filter(col("__END_AT").isNull())  # Get only current/active records from SCD Type 2
        .select(
            col("_id").alias("product_id"),
            trim(col("name")).alias("product_name"),
            upper(trim(col("name"))).alias("product_name_standardized"),
            col("price"),
            when(col("price") < 10, "Budget")
                .when(col("price") < 50, "Mid-Range")
                .when(col("price") < 200, "Premium")
                .otherwise("Luxury").alias("price_category"),
            trim(col("details")).alias("product_details"),
            col("suppliers"),
            current_timestamp().alias("processed_at")
        )
    )

# Silver Product History Table
# Full historical view of all product changes
@dp.materialized_view(
    comment="Silver layer: Complete product history with all changes tracked",
    table_properties={"delta.enableChangeDataFeed": "true"}
)
@dp.expect_or_drop("valid_product_id_history", "product_id IS NOT NULL")
def silver_products_history():
    """
    Complete product history including all changes over time.
    Includes __START_AT and __END_AT for temporal queries.
    """
    return (
        spark.read.table("bronze_products")
        .select(
            col("_id").alias("product_id"),
            trim(col("name")).alias("product_name"),
            col("price"),
            trim(col("details")).alias("product_details"),
            col("__START_AT").alias("valid_from"),
            col("__END_AT").alias("valid_to"),
            when(col("__END_AT").isNull(), True).otherwise(False).alias("is_current")
        )
    )

# Silver Product Suppliers Table
# Flattened supplier data from nested array
@dp.materialized_view(
    comment="Silver layer: Flattened product-supplier relationships",
    table_properties={"delta.enableChangeDataFeed": "true"}
)
@dp.expect_or_drop("valid_supplier_product_id", "product_id IS NOT NULL")
@dp.expect_or_drop("valid_supplier_name", "supplier_name IS NOT NULL AND LENGTH(TRIM(supplier_name)) > 0")
def silver_product_suppliers():
    """
    Flattened product-supplier relationships.
    One row per product-supplier combination.
    """
    return (
        spark.read.table("bronze_products")
        .filter(col("__END_AT").isNull())  # Get only current/active records
        .select(
            col("_id").alias("product_id"),
            explode(col("suppliers.supplier._name")).alias("supplier_name_raw")
        )
        .select(
            col("product_id"),
            trim(col("supplier_name_raw")).alias("supplier_name"),
            upper(trim(col("supplier_name_raw"))).alias("supplier_name_standardized"),
            current_timestamp().alias("processed_at")
        )
    )
