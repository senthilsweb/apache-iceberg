#!/usr/bin/env python3
r"""
File Name: bot_iceberg_loader.py
Author: Senthilnathan Karuppaiah
Date: 04-DEC-2025
Description: 
CSV to Iceberg Table Loader - Creates Apache Iceberg tables on S3-compatible storage.
This script reads a CSV file and creates a true Apache Iceberg table on Cloudflare R2
(or any S3-compatible storage) using PyIceberg with a SQLite catalog.

Python Environment Setup (replace `env` with your desired environment name):
1. Create a virtual environment:
   - For Unix/Linux/Mac: `python3 -m venv env`
   - For Windows: `py -m venv env`
2. Activate the virtual environment:
   - For Unix/Linux/Mac: `source env/bin/activate`
   - For Windows: `.\env\Scripts\activate`
3. Install dependencies:
   - `pip install "pyiceberg[s3fs,pyarrow,sql-sqlite]" python-dotenv structlog`
4. To freeze the installed packages for sharing or deployment:
   - `pip freeze > requirements.txt`

Requirements:
- pyiceberg[s3fs,pyarrow]
- pyarrow
- python-dotenv
- structlog

Environment Variables (.env file):
- ICE_CSV_FILE_PATH: Path to CSV file to load
- ICE_TABLE_NAME: Target Iceberg table name
- ICE_NAMESPACE: Iceberg namespace (default: 'default')
- ICE_S3_ACCESS_KEY: S3/R2 access key
- ICE_S3_SECRET_KEY: S3/R2 secret key
- ICE_S3_REGION: S3 region (default: 'auto' for R2)
- ICE_S3_ENDPOINT: S3 endpoint URL
- ICE_WAREHOUSE_PATH: S3 path for Iceberg warehouse
- ICE_CATALOG_DB_PATH: Local SQLite catalog path

Usage:
    python bot_iceberg_loader.py
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

# Load environment variables for configuration and secrets
load_dotenv()

# Try to import pyiceberg, provide helpful error if not installed
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

# Get the current script name (without .py extension)
script_name = os.path.splitext(os.path.basename(__file__))[0]

# Create logs directory if it doesn't exist
os.makedirs("./logs", exist_ok=True)

# Generate a log filename with the current timestamp
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

# CSV/Data Configuration
CSV_FILE = os.getenv('ICE_CSV_FILE_PATH', './data/sample.csv')
TABLE_NAME = os.getenv('ICE_TABLE_NAME', 'my_iceberg_table')
NAMESPACE = os.getenv('ICE_NAMESPACE', 'default')

# S3/R2 Credentials (use R2_ prefix, fallback to ICE_S3_ for backward compatibility)
S3_ACCESS_KEY = os.getenv('R2_ACCESS_KEY') or os.getenv('ICE_S3_ACCESS_KEY', '')
S3_SECRET_KEY = os.getenv('R2_SECRET_KEY') or os.getenv('ICE_S3_SECRET_KEY', '')
S3_REGION = os.getenv('R2_REGION', 'auto')
S3_ENDPOINT = os.getenv('R2_ENDPOINT') or os.getenv('ICE_S3_ENDPOINT', '')

# Iceberg Configuration
WAREHOUSE_PATH = os.getenv('ICE_WAREHOUSE_PATH', 's3://iceberg-demo/warehouse')
CATALOG_DB_PATH = os.getenv('ICE_CATALOG_DB_PATH', './iceberg_catalog.db')

# Catalog Type Configuration (sqlite or nessie)
CATALOG_TYPE = os.getenv('ICE_CATALOG_TYPE', 'nessie').lower()
NESSIE_URI = os.getenv('ICE_NESSIE_URI', 'http://localhost:19120/iceberg/')


# =============================
# HELPER FUNCTIONS
# =============================

def read_csv_to_arrow(csv_path: str):
    """
    Read CSV file and return PyArrow table.
    
    Args:
        csv_path: Path to CSV file
        
    Returns:
        PyArrow table or None if error
    """
    try:
        log.info("Reading CSV file", file_path=csv_path)
        
        # Check if file exists
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
    """
    Convert PyArrow schema to Iceberg schema.
    
    Args:
        arrow_schema: PyArrow schema
        
    Returns:
        Iceberg Schema object
    """
    log.info("Converting Arrow schema to Iceberg schema")
    fields = []
    
    for i, field in enumerate(arrow_schema):
        field_id = i + 1
        arrow_type = field.type
        
        # Map Arrow types to Iceberg types
        if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
            iceberg_type = StringType()
            type_name = "string"
        elif pa.types.is_float64(arrow_type):
            iceberg_type = DoubleType()
            type_name = "double"
        elif pa.types.is_float32(arrow_type) or pa.types.is_float16(arrow_type):
            iceberg_type = FloatType()
            type_name = "float"
        elif pa.types.is_int64(arrow_type):
            iceberg_type = LongType()
            type_name = "long"
        elif pa.types.is_int32(arrow_type) or pa.types.is_int16(arrow_type) or pa.types.is_int8(arrow_type):
            iceberg_type = IntegerType()
            type_name = "integer"
        elif pa.types.is_timestamp(arrow_type):
            iceberg_type = TimestampType()
            type_name = "timestamp"
        elif pa.types.is_date(arrow_type):
            iceberg_type = DateType()
            type_name = "date"
        elif pa.types.is_boolean(arrow_type):
            iceberg_type = BooleanType()
            type_name = "boolean"
        else:
            # Default to string for unsupported types
            iceberg_type = StringType()
            type_name = "string (default)"
            log.warning("Unsupported Arrow type, defaulting to string", 
                       field_name=field.name, 
                       arrow_type=str(arrow_type))
        
        log.debug("Field type mapping", 
                 field_name=field.name, 
                 arrow_type=str(arrow_type), 
                 iceberg_type=type_name)
        
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
    """
    Configure and return the PyIceberg catalog.
    Supports both SQLite (local) and Nessie (REST) catalogs.
    
    Returns:
        Catalog object (SqlCatalog or RestCatalog)
    """
    if CATALOG_TYPE == 'nessie':
        log.info("Configuring Nessie catalog", 
                nessie_uri=NESSIE_URI,
                warehouse=WAREHOUSE_PATH,
                endpoint=S3_ENDPOINT)
        
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
        
        log.info("Nessie catalog configured successfully")
        return catalog
    else:
        # Default: SQLite catalog
        log.info("Configuring SQLite catalog", 
                catalog_db=CATALOG_DB_PATH,
                warehouse=WAREHOUSE_PATH,
                endpoint=S3_ENDPOINT)
        
        catalog = SqlCatalog(
            "r2_catalog",
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
        
        log.info("SQLite catalog configured successfully")
        return catalog


def create_or_replace_table(catalog, table_identifier: str, iceberg_schema, arrow_table) -> bool:
    """
    Create or replace an Iceberg table and load data.
    
    Args:
        catalog: PyIceberg catalog
        table_identifier: Full table name (namespace.table)
        iceberg_schema: Iceberg schema
        arrow_table: PyArrow table with data
        
    Returns:
        Boolean indicating success
    """
    try:
        # Try to drop existing table first
        try:
            catalog.drop_table(table_identifier)
            log.info("Dropped existing table", table=table_identifier)
        except Exception:
            pass  # Table doesn't exist, that's fine
        
        log.info("Creating Iceberg table", 
                table=table_identifier,
                location=f"{WAREHOUSE_PATH}/{TABLE_NAME}")
        
        table = catalog.create_table(
            identifier=table_identifier,
            schema=iceberg_schema,
            location=f"{WAREHOUSE_PATH}/{TABLE_NAME}"
        )
        
        log.info("Table created successfully", location=table.location())
        
        # Append data to the table
        log.info("Writing data to Iceberg table", rows=arrow_table.num_rows)
        table.append(arrow_table)
        
        log.info("Data written successfully", 
                table=table_identifier,
                rows=arrow_table.num_rows,
                metadata_location=f"{table.location()}/metadata/",
                data_location=f"{table.location()}/data/")
        
        return True
        
    except Exception as e:
        log.error("Failed to create or load table", 
                 table=table_identifier, 
                 error=str(e))
        return False


# =============================
# MAIN PIPELINE FUNCTION
# =============================

def csv_to_iceberg_pipeline() -> bool:
    """
    Main pipeline function that orchestrates the entire process.
    
    Returns:
        Boolean indicating success
    """
    log.info("Starting CSV to Iceberg pipeline", 
            csv_file=CSV_FILE,
            table_name=TABLE_NAME,
            namespace=NAMESPACE,
            warehouse=WAREHOUSE_PATH)
    
    try:
        # Step 1: Read CSV file
        arrow_table = read_csv_to_arrow(CSV_FILE)
        if arrow_table is None:
            log.error("Failed to read CSV file, aborting pipeline")
            return False
        
        print(f"👍 CSV loaded: {arrow_table.num_rows} rows, {arrow_table.num_columns} columns")
        print("📋 Arrow Schema:")
        print(arrow_table.schema)
        print("")
        
        # Step 2: Configure catalog
        print(f"🔧 Configuring PyIceberg catalog (type: {CATALOG_TYPE})...")
        catalog = configure_catalog()
        if CATALOG_TYPE == 'nessie':
            print(f"✅ Catalog created: Nessie at {NESSIE_URI}")
        else:
            print(f"✅ Catalog created: SQLite at {CATALOG_DB_PATH}")
        print(f"✅ Warehouse: {WAREHOUSE_PATH}")
        
        # Step 3: Create namespace if it doesn't exist
        try:
            catalog.create_namespace(NAMESPACE)
            log.info("Namespace created", namespace=NAMESPACE)
            print(f"✅ Namespace '{NAMESPACE}' created")
        except Exception as e:
            if "already exists" in str(e).lower():
                log.info("Namespace already exists", namespace=NAMESPACE)
                print(f"ℹ️  Namespace '{NAMESPACE}' already exists")
            else:
                log.warning("Namespace warning", namespace=NAMESPACE, error=str(e))
                print(f"⚠️  Namespace warning: {e}")
        
        # Step 4: Convert Arrow schema to Iceberg schema
        iceberg_schema = arrow_to_iceberg_schema(arrow_table.schema)
        print("")
        print("📋 Iceberg Schema:")
        print(iceberg_schema)
        print("")
        
        # Step 5: Create table and load data
        table_identifier = f"{NAMESPACE}.{TABLE_NAME}"
        print(f"🧊 Creating Iceberg table: {table_identifier}")
        
        success = create_or_replace_table(catalog, table_identifier, iceberg_schema, arrow_table)
        
        if success:
            # Reload table to get location
            table = catalog.load_table(table_identifier)
            
            log.info("Pipeline completed successfully", 
                    records_processed=arrow_table.num_rows,
                    table=table_identifier,
                    location=table.location())
            
            print(f"✅ Table created at: {table.location()}")
            print(f"📤 Wrote {arrow_table.num_rows} rows to Iceberg table")
            print("")
            print("🎉 SUCCESS! Iceberg table created with metadata!")
            print("")
            print("📁 Iceberg Structure:")
            if CATALOG_TYPE == 'nessie':
                print(f"   Catalog: Nessie at {NESSIE_URI}")
            else:
                print(f"   Catalog DB: {CATALOG_DB_PATH}")
            print(f"   Table Location: {table.location()}")
            print(f"   Metadata: {table.location()}/metadata/")
            print(f"   Data: {table.location()}/data/")
            
            return True
        else:
            log.error("Pipeline failed")
            return False
            
    except Exception as e:
        log.error("Pipeline execution failed", error=str(e))
        return False


# =============================
# MAIN EXECUTION
# =============================

if __name__ == "__main__":
    print("🚀 CSV → Iceberg Table Loader")
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("")
    
    success = csv_to_iceberg_pipeline()
    
    if success:
        print("")
        print("📖 To read this table:")
        print("")
        if CATALOG_TYPE == 'nessie':
            print("   # Python with PyIceberg + Nessie:")
            print(f"   from pyiceberg.catalog.rest import RestCatalog")
            print(f"   catalog = RestCatalog('nessie', uri='{NESSIE_URI}', ...)")
        else:
            print("   # Python with PyIceberg:")
            print(f"   from pyiceberg.catalog.sql import SqlCatalog")
            print(f"   catalog = SqlCatalog('r2_catalog', uri='sqlite:///{CATALOG_DB_PATH}', ...)")
        print(f"   table = catalog.load_table('{NAMESPACE}.{TABLE_NAME}')")
        print(f"   df = table.scan().to_pandas()")
        print("")
        print(f"📁 Log file: {log_filename}")
    else:
        print(f"❌ Pipeline failed. Check log file: {log_filename}")
        sys.exit(1)
