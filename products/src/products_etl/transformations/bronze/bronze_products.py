# Bronze Layer - Raw Product Data Ingestion with Auto CDC
# Handles full snapshots up to 500 GB with change tracking

from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, LongType, ArrayType, TimestampType
from typing import Optional, Tuple
from pyspark.sql import DataFrame

# Define the inner supplier structure (attribute 'name')
supplier_schema = StructType([
    StructField("_name", StringType(), False)  # Attributes are prefixed with _ by default
])

# Define the product structure
product_schema = StructType([
    StructField("_id", LongType(), False),
    StructField("name", StringType(), True),
    StructField("price", DecimalType(10, 2), True),
    StructField("details", StringType(), True),
    StructField("suppliers", StructType([
        StructField("supplier", ArrayType(supplier_schema), True)
    ]), True)
])

# Create target streaming table for CDC
# This will store the raw product data with full history (SCD Type 2)
dp.create_streaming_table(
    name="bronze_products",
    comment="Bronze layer: Raw product data with full history tracking (SCD Type 2)",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",  # Optimize writes for large data
        "delta.autoOptimize.autoCompact": "true"  # Auto-compact small files
    }
)

# Define function to load product snapshots
# This handles periodic full snapshots (daily/weekly dumps)
def load_product_snapshot(latest_snapshot_version: Optional[int]) -> Optional[Tuple[DataFrame, int]]:
    """
    Load product snapshots incrementally.
    
    Args:
        latest_snapshot_version: Version of the last processed snapshot (None for first run)
    
    Returns:
        Tuple of (DataFrame, version) or None when no more snapshots exist
    """
    if latest_snapshot_version is None:
        # First run - load initial snapshot
        # Assuming snapshots are stored as Parquet files (more efficient than XML for 500 GB)
        df = spark.read.format("xml").option("rowTag", "product").schema(product_schema).load("/Volumes/workspace/default/raw_data/products/snapshot_v1/")
        return (df, 1)
    else:
        # Load next snapshot version
        next_version = latest_snapshot_version + 1
        snapshot_path = f"/Volumes/workspace/default/raw_data/products/snapshot_v{next_version}/"
        
        # Check if next snapshot exists
        try:
            df = spark.read.format("xml").option("rowTag", "product").schema(product_schema).load(snapshot_path)
            return (df, next_version)
        except:
            # No more snapshots to process
            return None

# Apply Auto CDC from snapshots
# This automatically computes inserts, updates, and deletes between consecutive snapshots
dp.create_auto_cdc_from_snapshot_flow(
    target="bronze_products",
    source=load_product_snapshot,
    keys=["_id"],  # Primary key for identifying products
    stored_as_scd_type=2  # Track full history with __START_AT and __END_AT columns
)
