"""Bronze Layer Verification Script

This script validates that all bronze ingestion jobs completed successfully
by querying the bronze tables and checking data quality metrics.

Usage:
    - Run this script after bronze pipeline completes
    - Verifies table existence, record counts, and data freshness
    - Reports any data quality issues found
"""

from pyspark.sql import functions as F
from datetime import datetime, timedelta

# ==============================================================================
# Configuration
# ==============================================================================

# Catalog where bronze tables are stored
CATALOG = "0-ai-trust"
SCHEMA = "bronze"

# Bronze tables to verify (created by the bronze pipeline)
BRONZE_TABLES = [
    "cdc_changes",      # CDC events from operational databases
    "kafka_events",     # Kafka event stream data
    "file_arrivals"     # Binary file uploads
]

# Data freshness threshold (hours) - alert if no new data in this period
FRESHNESS_THRESHOLD_HOURS = 24


# ==============================================================================
# Helper Functions
# ==============================================================================

def fully_qualified_table(table_name: str) -> str:
    """Return the fully qualified table name.
    
    Args:
        table_name: Short table name (e.g., 'cdc_changes')
    
    Returns:
        Fully qualified name (e.g., '0-ai-trust.bronze.cdc_changes')
    """
    return f"{CATALOG}.{SCHEMA}.{table_name}"


def table_exists(table_name: str) -> bool:
    """Check if a table exists in Unity Catalog.
    
    Args:
        table_name: Fully qualified table name
    
    Returns:
        True if table exists, False otherwise
    """
    try:
        # Move the action (.collect()) inside try block to catch errors properly
        # Spark Connect defers execution until action fires
        spark.table(table_name).limit(1).collect()
        return True
    except Exception:
        return False


def get_table_stats(table_name: str) -> dict:
    """Get statistics about a bronze table.
    
    Args:
        table_name: Fully qualified table name
    
    Returns:
        Dictionary with count, latest_ingestion, and data_age_hours
    """
    df = spark.table(table_name)
    
    # Get record count
    count = df.count()
    
    # Get latest ingestion timestamp (all bronze tables have _ingested_at)
    if count > 0:
        latest = df.agg(F.max("_ingested_at").alias("latest")).collect()[0]["latest"]
        
        # Calculate data age in hours
        if latest:
            data_age = (datetime.now() - latest).total_seconds() / 3600
        else:
            data_age = None
    else:
        latest = None
        data_age = None
    
    return {
        "count": count,
        "latest_ingestion": latest,
        "data_age_hours": data_age
    }


def check_null_values(table_name: str, critical_columns: list) -> dict:
    """Check for null values in critical columns.
    
    Args:
        table_name: Fully qualified table name
        critical_columns: List of column names that shouldn't be null
    
    Returns:
        Dictionary mapping column names to null counts
    """
    df = spark.table(table_name)
    null_counts = {}
    
    # Cache df.columns before the loop to avoid repeated Analyze RPCs
    # On Spark Connect, accessing df.columns in a loop triggers RPC for each iteration
    available_columns = set(df.columns)
    
    for col in critical_columns:
        if col in available_columns:
            null_count = df.filter(F.col(col).isNull()).count()
            null_counts[col] = null_count
    
    return null_counts


def show_sample_records(table_name: str, n: int = 5):
    """Display sample records from the table.
    
    Args:
        table_name: Fully qualified table name
        n: Number of sample records to show
    """
    df = spark.table(table_name)
    print(f"\n  Sample records (showing {n}):")
    df.limit(n).show(n, truncate=50, vertical=False)


# ==============================================================================
# Main Verification Logic
# ==============================================================================

def verify_bronze_layer():
    """Main function to verify all bronze tables.
    
    Checks:
        1. Table existence
        2. Record counts
        3. Data freshness
        4. Null values in critical columns
        5. Sample data preview
    
    Returns:
        True if all checks pass, False otherwise
    """
    print("="*80)
    print("BRONZE LAYER VERIFICATION REPORT")
    print("="*80)
    print(f"Catalog: {CATALOG}")
    print(f"Schema: {SCHEMA}")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    all_checks_passed = True
    
    # Verify each bronze table
    for table in BRONZE_TABLES:
        fq_table = fully_qualified_table(table)
        print(f"\n{'='*80}")
        print(f"Table: {fq_table}")
        print(f"{'='*80}")
        
        # Check 1: Table existence
        if not table_exists(fq_table):
            print(f"❌ FAILED: Table does not exist!")
            all_checks_passed = False
            continue
        else:
            print(f"✅ Table exists")
        
        # Check 2: Get table statistics
        stats = get_table_stats(fq_table)
        print(f"\n📊 Statistics:")
        print(f"  - Record count: {stats['count']:,}")
        
        if stats['count'] == 0:
            print(f"  ⚠️  WARNING: Table is empty!")
            all_checks_passed = False
            continue
        
        # Check 3: Data freshness
        print(f"  - Latest ingestion: {stats['latest_ingestion']}")
        if stats['data_age_hours'] is not None:
            print(f"  - Data age: {stats['data_age_hours']:.1f} hours")
            
            if stats['data_age_hours'] > FRESHNESS_THRESHOLD_HOURS:
                print(f"  ⚠️  WARNING: Data is stale (older than {FRESHNESS_THRESHOLD_HOURS} hours)")
                all_checks_passed = False
            else:
                print(f"  ✅ Data is fresh")
        
        # Check 4: Null value checks for critical columns
        critical_cols_map = {
            "cdc_changes": ["source_dataset", "operation", "source_lsn"],
            "kafka_events": ["event_id", "event_type", "topic"],
            "file_arrivals": ["source_dataset", "source_file"]
        }
        
        if table in critical_cols_map:
            print(f"\n🔍 Data Quality Checks:")
            null_counts = check_null_values(fq_table, critical_cols_map[table])
            
            has_nulls = False
            for col, null_count in null_counts.items():
                if null_count > 0:
                    print(f"  ⚠️  WARNING: Column '{col}' has {null_count:,} null values")
                    has_nulls = True
                else:
                    print(f"  ✅ Column '{col}' has no nulls")
            
            if has_nulls:
                all_checks_passed = False
        
        # Check 5: Show sample records
        try:
            show_sample_records(fq_table, n=3)
        except Exception as e:
            print(f"  ⚠️  Could not display sample records: {str(e)}")
    
    # Final summary
    print(f"\n{'='*80}")
    print("VERIFICATION SUMMARY")
    print(f"{'='*80}")
    
    if all_checks_passed:
        print("✅ All bronze layer checks PASSED")
        print("   Bronze data is ready for silver layer processing.")
    else:
        print("❌ Some checks FAILED or have WARNINGS")
        print("   Review the issues above before proceeding to silver layer.")
    
    print(f"{'='*80}")
    
    return all_checks_passed


# ==============================================================================
# Execute Verification
# ==============================================================================

if __name__ == "__main__":
    # Run the verification
    success = verify_bronze_layer()
    
    # Exit with appropriate code (useful for automation/CI)
    if not success:
        raise Exception("Bronze layer verification failed. Check the report above.")
