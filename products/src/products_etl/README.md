# Product Data Pipeline - Medallion Architecture with Auto CDC

## Overview
This pipeline implements a complete medallion architecture (Bronze → Silver → Gold) for ingesting and processing large product snapshots (up to 500 GB) using Spark Declarative Pipelines with Auto CDC capabilities.

## Architecture

### 🥉 Bronze Layer
**Location**: `transformations/bronze/bronze_products.py`

**Purpose**: Raw data ingestion with change tracking

**Features**:
- Auto CDC from snapshot (handles periodic full dumps)
- SCD Type 2 history tracking (maintains full change history)
- Optimized for large datasets (500 GB+)
- Auto-compaction and write optimization enabled

**Output Tables**:
- `bronze_products`: Raw product data with `__START_AT` and `__END_AT` columns

**Data Source**:
- Expects XML snapshots in: `/Volumes/workspace/default/raw_data/products/snapshot_v{N}/`
- Version increments automatically (v1, v2, v3, etc.)

### 🥈 Silver Layer
**Location**: `transformations/silver/silver_products.py`

**Purpose**: Cleaned, conformed, and flattened data

**Features**:
- Data quality checks (non-null IDs, valid names, non-negative prices)
- Standardized text (trimmed, uppercase)
- Derived fields (price categories)
- Flattened nested structures
- Current and historical views

**Output Tables**:
1. **`silver_products`**: Current product state with cleaned data
   - Columns: `product_id`, `product_name`, `product_name_standardized`, `price`, `price_category`, `product_details`, `processed_at`
   
2. **`silver_products_history`**: Complete change history
   - Columns: `product_id`, `product_name`, `price`, `product_details`, `valid_from`, `valid_to`, `is_current`
   
3. **`silver_product_suppliers`**: Flattened product-supplier relationships
   - Columns: `product_id`, `supplier_name`, `supplier_name_standardized`, `processed_at`

**Data Quality Rules**:
- ✅ Product IDs must be non-null and positive
- ✅ Product names must be non-null and non-empty
- ⚠️ Prices must be non-negative (warns but doesn't drop)

### 🥇 Gold Layer
**Location**: `transformations/gold/gold_product_metrics.py`

**Purpose**: Business-level aggregations and analytics

**Features**:
- Pre-computed metrics for dashboards
- Aggregated KPIs
- Business intelligence ready

**Output Tables**:
1. **`gold_products_by_category`**: Product metrics by price category
   - Product counts, average/min/max prices, unique suppliers per category
   
2. **`gold_supplier_metrics`**: Supplier performance metrics
   - Product counts per supplier, price statistics
   
3. **`gold_product_summary`**: Overall portfolio summary
   - Total products, total suppliers, overall price statistics
   
4. **`gold_product_change_summary`**: Change tracking metrics
   - Version counts per product, first seen, last changed dates

## Data Flow

```
Snapshot Files (XML)
         ↓
    Bronze Layer (Auto CDC - SCD Type 2)
         ↓
    Silver Layer (Clean, Flatten, Quality Checks)
         ↓
    Gold Layer (Aggregations, Business Metrics)
```

## Setup Instructions

### 1. Prepare Data Sources
Place your product snapshot files in the expected location:
```
/Volumes/workspace/default/raw_data/products/snapshot_v1/
/Volumes/workspace/default/raw_data/products/snapshot_v2/
/Volumes/workspace/default/raw_data/products/snapshot_v3/
...
```

**Expected Schema** (XML format recommended):
```
_id: Long (required)
name: String
price: Decimal(10,2)
details: String
suppliers:
  supplier: Array[
    _name: String
  ]
```

### 2. Configure Pipeline
- Ensure all files in `transformations/` are included in pipeline settings
- For 500 GB datasets, use **serverless** compute for optimal performance
- Consider enabling auto-scaling

### 3. Run Pipeline
```
# Initial run (processes first snapshot)
Start Pipeline Update

# Subsequent runs (processes new snapshots)
Start Pipeline Update (will auto-detect new versions)
```

## Key Features

### Auto CDC from Snapshot
- Automatically detects inserts, updates, and deletes between snapshots
- No need to manually compute diffs
- Efficient processing even for 500 GB datasets

### SCD Type 2 History
- Every change to a product creates a new version
- `__START_AT`: When this version became active
- `__END_AT`: When this version was superseded (NULL for current)
- Query historical state at any point in time

### Scalability Optimizations
- **Delta Auto-Optimize**: Automatic file compaction and optimization
- **Parquet Format**: Columnar storage for efficient large-scale processing
- **Materialized Views**: Pre-computed transformations for fast querying
- **Partition-friendly**: Ready for partitioning by date or category if needed

## Query Examples

### Get Current Products
```sql
SELECT * FROM silver_products
```

### Get Product History for Specific Product
```sql
SELECT * FROM silver_products_history
WHERE product_id = 12345
ORDER BY valid_from DESC
```

### Get Products as of Specific Date
```sql
SELECT * FROM silver_products_history
WHERE valid_from <= '2024-03-15'
  AND (valid_to > '2024-03-15' OR valid_to IS NULL)
```

### Product Metrics by Category
```sql
SELECT * FROM gold_products_by_category
ORDER BY product_count DESC
```

## Monitoring

Check pipeline status:
- Pipeline Update Status: Shows current processing state
- Dataset Status: View each table's row counts and processing times
- Data Quality Metrics: Track expectation violations

## Troubleshooting

### Issue: Snapshot not loading
- Verify snapshot path matches: `/Volumes/workspace/default/raw_data/products/snapshot_v{N}/`
- Check file format is XML (or update code for Parquet/CSV)
- Ensure schema matches expected structure

### Issue: Large snapshot performance
- Use **serverless** compute tier
- Enable auto-scaling
- Consider partitioning source data by category or date
