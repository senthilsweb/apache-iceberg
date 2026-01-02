#!/usr/bin/env python3
r"""
File Name: bot_iceberg_loader.py
Author: Senthilnathan Karuppaiah
Date: 04-DEC-2025 (Updated: 02-JAN-2026)
Description: 
CSV to Iceberg Table Loader - Creates Apache Iceberg tables on S3-compatible storage.
Supports loading from:
- Remote HTTP/HTTPS URLs (downloads to temp, then loads)
- Local file paths
- Directory with glob patterns (multiple files)

Python Environment Setup:
    python3 -m venv env
    source env/bin/activate
    pip install -r requirements.txt

Environment Variables:
    Source Configuration:
    - ICE_SOURCE_PATH     : URL, file path, or directory (required)
    - ICE_GLOB_PATTERN    : Glob pattern for directories (e.g., "*.csv")
    - ICE_TABLE_NAME      : Table name (optional - derived from filename if empty)
    - ICE_NAMESPACE       : Iceberg namespace (default: 'default')
    
    Table Naming:
    - ICE_PLURALIZE_TABLE : 'true' to pluralize table names (default: 'false')
    - ICE_GLOB_MERGE_TABLE: 'true' to merge glob files into single table (default: 'false')
    
    Catalog Configuration:
    - ICE_CATALOG_TYPE    : 'rest' (Nessie) or 'sql' (SQLite) (default: 'rest')
    - ICE_NESSIE_URI      : Nessie REST catalog URI
    - ICE_WAREHOUSE_PATH  : S3 warehouse path (e.g., 's3://bucket/warehouse')
    - ICE_CATALOG_DB_PATH : SQLite catalog path for local dev (default: './iceberg_catalog.db')
    
    S3-Compatible Storage (Cloudflare R2, AWS S3, MinIO):
    - R2_ENDPOINT         : S3-compatible endpoint URL
    - R2_ACCESS_KEY       : Access key ID
    - R2_SECRET_KEY       : Secret access key
    - R2_REGION           : Region (default: 'auto')

Usage Examples:
    # Remote URL (table name derived from filename: users.csv → users)
    ICE_SOURCE_PATH="https://raw.githubusercontent.com/user/repo/main/data/users.csv" \
    ICE_NAMESPACE="mydb" \
    ICE_CATALOG_TYPE="rest" \
    ICE_NESSIE_URI="https://nessie.example.com/iceberg/" \
    ICE_WAREHOUSE_PATH="s3://bucket/warehouse" \
    R2_ENDPOINT="https://account.r2.cloudflarestorage.com" \
    R2_ACCESS_KEY="your-key" \
    R2_SECRET_KEY="your-secret" \
    python bot_iceberg_loader.py
    
    # Local file with explicit table name
    ICE_SOURCE_PATH="./data/sales.csv" \
    ICE_TABLE_NAME="sales_data" \
    ICE_NAMESPACE="analytics" \
    python bot_iceberg_loader.py
    
    # Directory with glob pattern (creates one table per file)
    ICE_SOURCE_PATH="./data/" \
    ICE_GLOB_PATTERN="*.csv" \
    ICE_NAMESPACE="imports" \
    python bot_iceberg_loader.py
    
    # Directory with glob, merge all into single table
    ICE_SOURCE_PATH="./data/" \
    ICE_GLOB_PATTERN="sales_*.csv" \
    ICE_TABLE_NAME="all_sales" \
    ICE_GLOB_MERGE_TABLE="true" \
    python bot_iceberg_loader.py

Output:
    - Creates Iceberg table(s) in the specified namespace
    - Writes Parquet data files to S3 warehouse
    - Logs execution details to ./logs/
"""

# Import necessary libraries
import pyarrow.csv as csv
import pyarrow as pa
import os
import sys
from datetime import datetime
from dotenv import load_dotenv
import structlog
import logging

# Import utility functions
from utils import (
    is_remote_url,
    detect_source_type,
    get_files_to_process,
    resolve_table_name,
    cleanup_temp_files
)

# Load environment variables
load_dotenv()

# Try to import pyiceberg
try:
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.catalog.rest import RestCatalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import (
        StringType, LongType, NestedField, TimestampType, IntegerType,
        DoubleType, FloatType, BooleanType, DateType
    )
except ImportError:
    print("❌ PyIceberg is not installed. Please install it:")
    print("   pip install 'pyiceberg[s3fs,pyarrow]' pyarrow")
    sys.exit(1)

# =============================
# LOGGING CONFIGURATION
# =============================

script_name = os.path.splitext(os.path.basename(__file__))[0]
os.makedirs("./logs", exist_ok=True)
log_filename = f"./logs/{script_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(filename=log_filename, level=logging.INFO, format="%(message)s")
structlog.configure(
    logger_factory=structlog.stdlib.LoggerFactory(),
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ]
)
log = structlog.get_logger()

# =============================
# CONFIGURATION FROM .env
# =============================

# Source Configuration
SOURCE_PATH = os.getenv('ICE_SOURCE_PATH', '')
GLOB_PATTERN = os.getenv('ICE_GLOB_PATTERN', '')
TABLE_NAME = os.getenv('ICE_TABLE_NAME', '')  # Optional - derived from filename if empty
NAMESPACE = os.getenv('ICE_NAMESPACE', 'default')

# Table naming options
PLURALIZE_TABLE = os.getenv('ICE_PLURALIZE_TABLE', 'false').lower() == 'true'
GLOB_MERGE_TABLE = os.getenv('ICE_GLOB_MERGE_TABLE', 'false').lower() == 'true'

# S3/R2 Credentials
S3_ACCESS_KEY = os.getenv('R2_ACCESS_KEY') or os.getenv('ICE_S3_ACCESS_KEY', '')
S3_SECRET_KEY = os.getenv('R2_SECRET_KEY') or os.getenv('ICE_S3_SECRET_KEY', '')
S3_REGION = os.getenv('R2_REGION', 'auto')
S3_ENDPOINT = os.getenv('R2_ENDPOINT') or os.getenv('ICE_S3_ENDPOINT', '')

# Iceberg Configuration
WAREHOUSE_PATH = os.getenv('ICE_WAREHOUSE_PATH', 's3://iceberg-demo/warehouse')
CATALOG_DB_PATH = os.getenv('ICE_CATALOG_DB_PATH', './iceberg_catalog.db')

# Catalog Type Configuration
CATALOG_TYPE = os.getenv('ICE_CATALOG_TYPE', 'rest').lower()
NESSIE_URI = os.getenv('ICE_NESSIE_URI', 'http://localhost:19120/iceberg/')


# =============================
# HELPER FUNCTIONS
# =============================

def read_csv_to_arrow(csv_path: str):
    """Read CSV file and return PyArrow table."""
    try:
        log.info("Reading CSV file", file_path=csv_path)
        
        if not os.path.exists(csv_path):
            log.error("CSV file not found", file_path=csv_path)
            return None
        
        read_opts = csv.ReadOptions(use_threads=True)
        convert_opts = csv.ConvertOptions(auto_dict_encode=False)
        parse_opts = csv.ParseOptions(newlines_in_values=True)

        table = csv.read_csv(
            csv_path,
            read_options=read_opts,
            convert_options=convert_opts,
            parse_options=parse_opts
        )
        
        log.info("CSV file loaded successfully", 
                rows=table.num_rows, 
                columns=table.num_columns,
                column_names=[f.name for f in table.schema])
        
        return table
        
    except Exception as e:
        log.error("Error reading CSV file", error=str(e), file_path=csv_path)
        return None


def arrow_to_iceberg_schema(arrow_schema):
    """Convert PyArrow schema to Iceberg schema."""
    log.info("Converting Arrow schema to Iceberg schema")
    fields = []
    
    for i, field in enumerate(arrow_schema):
        field_id = i + 1
        arrow_type = field.type
        
        # Map Arrow types to Iceberg types
        if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
            iceberg_type = StringType()
        elif pa.types.is_float64(arrow_type):
            iceberg_type = DoubleType()
        elif pa.types.is_float32(arrow_type) or pa.types.is_float16(arrow_type):
            iceberg_type = FloatType()
        elif pa.types.is_int64(arrow_type):
            iceberg_type = LongType()
        elif pa.types.is_int32(arrow_type) or pa.types.is_int16(arrow_type) or pa.types.is_int8(arrow_type):
            iceberg_type = IntegerType()
        elif pa.types.is_timestamp(arrow_type):
            iceberg_type = TimestampType()
        elif pa.types.is_date(arrow_type):
            iceberg_type = DateType()
        elif pa.types.is_boolean(arrow_type):
            iceberg_type = BooleanType()
        else:
            iceberg_type = StringType()
            log.warning("Unsupported Arrow type, defaulting to string", 
                       field_name=field.name, arrow_type=str(arrow_type))
        
        fields.append(
            NestedField(
                field_id=field_id,
                name=field.name,
                field_type=iceberg_type,
                required=not field.nullable
            )
        )
    
    log.info("Schema conversion completed", field_count=len(fields))
    return Schema(*fields)


def configure_catalog():
    """Configure and return the PyIceberg catalog."""
    if CATALOG_TYPE == 'rest':
        log.info("Configuring REST catalog (Nessie)", 
                nessie_uri=NESSIE_URI, warehouse=WAREHOUSE_PATH)
        
        catalog = RestCatalog(
            "nessie",
            **{
                "uri": NESSIE_URI,
                "warehouse": WAREHOUSE_PATH,
                "s3.endpoint": S3_ENDPOINT,
                "s3.access-key-id": S3_ACCESS_KEY,
                "s3.secret-access-key": S3_SECRET_KEY,
                "s3.region": S3_REGION,
                "s3.path-style-access": "true",
            }
        )
        return catalog
    else:
        # SQLite catalog
        log.info("Configuring SQL catalog (SQLite)", 
                catalog_db=CATALOG_DB_PATH, warehouse=WAREHOUSE_PATH)
        
        catalog = SqlCatalog(
            "local_catalog",
            **{
                "uri": f"sqlite:///{CATALOG_DB_PATH}",
                "warehouse": WAREHOUSE_PATH,
                "s3.endpoint": S3_ENDPOINT,
                "s3.access-key-id": S3_ACCESS_KEY,
                "s3.secret-access-key": S3_SECRET_KEY,
                "s3.region": S3_REGION,
                "s3.path-style-access": "true",
            }
        )
        return catalog


def create_or_replace_table(catalog, table_identifier: str, iceberg_schema, arrow_table, table_name: str) -> bool:
    """Create or replace an Iceberg table and load data."""
    try:
        # Drop existing table if exists
        try:
            catalog.drop_table(table_identifier)
            log.info("Dropped existing table", table=table_identifier)
        except Exception:
            pass
        
        table_location = f"{WAREHOUSE_PATH}/{table_name}"
        log.info("Creating Iceberg table", table=table_identifier, location=table_location)
        
        table = catalog.create_table(
            identifier=table_identifier,
            schema=iceberg_schema,
            location=table_location
        )
        
        log.info("Table created successfully", location=table.location())
        
        # Append data
        log.info("Writing data to Iceberg table", rows=arrow_table.num_rows)
        table.append(arrow_table)
        
        log.info("Data written successfully", 
                table=table_identifier, rows=arrow_table.num_rows)
        
        return True
        
    except Exception as e:
        log.error("Failed to create or load table", table=table_identifier, error=str(e))
        return False


def process_single_file(catalog, file_path: str, filename: str, explicit_table_name: str = None) -> bool:
    """
    Process a single CSV file into an Iceberg table.
    
    Args:
        catalog: PyIceberg catalog
        file_path: Path to CSV file (local)
        filename: Original filename (for table name derivation)
        explicit_table_name: Explicit table name (overrides derivation)
        
    Returns:
        Boolean indicating success
    """
    # Determine table name
    if explicit_table_name:
        table_name = explicit_table_name
    else:
        table_name = resolve_table_name(filename, pluralize=PLURALIZE_TABLE)
    
    table_identifier = f"{NAMESPACE}.{table_name}"
    
    print(f"\n📄 Processing: {filename}")
    print(f"   Table: {table_identifier}")
    
    # Read CSV
    arrow_table = read_csv_to_arrow(file_path)
    if arrow_table is None:
        print(f"   ❌ Failed to read CSV file")
        return False
    
    print(f"   ✅ Loaded {arrow_table.num_rows} rows, {arrow_table.num_columns} columns")
    
    # Convert schema
    iceberg_schema = arrow_to_iceberg_schema(arrow_table.schema)
    
    # Create and load table
    success = create_or_replace_table(catalog, table_identifier, iceberg_schema, arrow_table, table_name)
    
    if success:
        table = catalog.load_table(table_identifier)
        print(f"   ✅ Table created at: {table.location()}")
        print(f"   📤 Wrote {arrow_table.num_rows} rows")
    else:
        print(f"   ❌ Failed to create table")
    
    return success


# =============================
# MAIN PIPELINE
# =============================

def csv_to_iceberg_pipeline() -> bool:
    """Main pipeline function."""
    
    if not SOURCE_PATH:
        print("❌ ICE_SOURCE_PATH is required")
        print("   Set it to a URL, file path, or directory")
        return False
    
    source_type = detect_source_type(SOURCE_PATH, GLOB_PATTERN)
    log.info("Starting pipeline", 
            source_path=SOURCE_PATH,
            source_type=source_type,
            glob_pattern=GLOB_PATTERN,
            namespace=NAMESPACE,
            catalog_type=CATALOG_TYPE)
    
    print(f"\n🔍 Source: {SOURCE_PATH}")
    print(f"   Type: {source_type}")
    if GLOB_PATTERN:
        print(f"   Pattern: {GLOB_PATTERN}")
    
    # Get files to process
    try:
        files_to_process = get_files_to_process(SOURCE_PATH, GLOB_PATTERN or None)
    except Exception as e:
        log.error("Failed to get files", error=str(e))
        print(f"❌ Failed to get files: {e}")
        return False
    
    if not files_to_process:
        print("❌ No files found to process")
        return False
    
    print(f"   Files: {len(files_to_process)}")
    
    # Configure catalog
    print(f"\n🔧 Configuring catalog (type: {CATALOG_TYPE})...")
    try:
        catalog = configure_catalog()
        if CATALOG_TYPE == 'rest':
            print(f"   ✅ Nessie at {NESSIE_URI}")
        else:
            print(f"   ✅ SQLite at {CATALOG_DB_PATH}")
    except Exception as e:
        log.error("Failed to configure catalog", error=str(e))
        print(f"   ❌ Failed: {e}")
        return False
    
    # Create namespace
    try:
        catalog.create_namespace(NAMESPACE)
        print(f"   ✅ Namespace '{NAMESPACE}' created")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"   ℹ️  Namespace '{NAMESPACE}' exists")
        else:
            log.warning("Namespace warning", error=str(e))
    
    # Process files
    success_count = 0
    failure_count = 0
    temp_files = []
    
    for file_path, filename in files_to_process:
        # Track temp files for cleanup
        if is_remote_url(SOURCE_PATH):
            temp_files.append(file_path)
        
        # Use explicit table name only for single file
        explicit_name = TABLE_NAME if len(files_to_process) == 1 and TABLE_NAME else None
        
        try:
            success = process_single_file(catalog, file_path, filename, explicit_name)
            if success:
                success_count += 1
            else:
                failure_count += 1
        except Exception as e:
            log.error("Error processing file", file=filename, error=str(e))
            print(f"   ❌ Error: {e}")
            failure_count += 1
    
    # Cleanup temp files
    cleanup_temp_files(temp_files)
    
    # Summary
    print(f"\n{'='*50}")
    print(f"📊 Summary")
    print(f"   ✅ Success: {success_count}")
    print(f"   ❌ Failed: {failure_count}")
    print(f"   📁 Log: {log_filename}")
    
    return failure_count == 0


# =============================
# MAIN EXECUTION
# =============================

if __name__ == "__main__":
    print("🚀 CSV → Iceberg Table Loader")
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    success = csv_to_iceberg_pipeline()
    
    if not success:
        sys.exit(1)
