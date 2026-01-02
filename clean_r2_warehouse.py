#!/usr/bin/env python3
"""
File Name: clean_r2_warehouse.py
Author: Senthilnathan Karuppaiah
Date: 04-DEC-2025
Description: 
Cleans (deletes) all objects in the Iceberg warehouse folder on Cloudflare R2.
This is useful when migrating from one catalog (SQLite) to another (Nessie)
and you need to recreate all tables fresh.

WARNING: This will DELETE ALL DATA in the warehouse folder!

Usage:
    python clean_r2_warehouse.py

Requirements:
    pip install s3fs python-dotenv
"""

import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

try:
    import s3fs
except ImportError:
    print("❌ s3fs is not installed. Please install it:")
    print("   pip install s3fs")
    sys.exit(1)

# Configuration from .env
S3_ACCESS_KEY = os.getenv('ICE_S3_ACCESS_KEY', '')
S3_SECRET_KEY = os.getenv('ICE_S3_SECRET_KEY', '')
S3_ENDPOINT = os.getenv('ICE_S3_ENDPOINT', '')
S3_REGION = os.getenv('ICE_S3_REGION', 'auto')
WAREHOUSE_PATH = os.getenv('ICE_WAREHOUSE_PATH', 's3://iceberg-demo/warehouse')

def clean_warehouse():
    """Delete all objects in the warehouse folder on R2."""
    
    print("🧹 R2 Warehouse Cleaner")
    print("=" * 60)
    print(f"📁 Warehouse Path: {WAREHOUSE_PATH}")
    print(f"🌐 Endpoint: {S3_ENDPOINT}")
    print("=" * 60)
    print("")
    
    # Confirm with user
    print("⚠️  WARNING: This will DELETE ALL DATA in the warehouse!")
    print("")
    confirm = input("Type 'DELETE' to confirm: ")
    
    if confirm != 'DELETE':
        print("❌ Aborted. No changes made.")
        return False
    
    print("")
    print("🔌 Connecting to R2...")
    
    # Create S3 filesystem
    fs = s3fs.S3FileSystem(
        key=S3_ACCESS_KEY,
        secret=S3_SECRET_KEY,
        endpoint_url=S3_ENDPOINT,
        client_kwargs={'region_name': S3_REGION}
    )
    
    # Convert s3:// path to bucket/prefix format
    warehouse_path = WAREHOUSE_PATH.replace('s3://', '')
    
    print(f"📋 Listing objects in: {warehouse_path}")
    
    try:
        # List all files in warehouse
        files = fs.ls(warehouse_path, detail=True)
        
        if not files:
            print("ℹ️  Warehouse is already empty.")
            return True
        
        # Count total objects (recursively)
        all_files = list(fs.find(warehouse_path))
        print(f"📊 Found {len(all_files)} objects to delete")
        print("")
        
        # Show first few files as preview
        print("📁 Preview of objects to delete:")
        for f in all_files[:10]:
            print(f"   - {f}")
        if len(all_files) > 10:
            print(f"   ... and {len(all_files) - 10} more")
        print("")
        
        # Final confirmation
        confirm2 = input("Type 'YES' to proceed with deletion: ")
        if confirm2 != 'YES':
            print("❌ Aborted. No changes made.")
            return False
        
        print("")
        print("🗑️  Deleting objects...")
        
        # Delete all files recursively
        fs.rm(warehouse_path, recursive=True)
        
        print("✅ All objects deleted successfully!")
        print("")
        print("📋 Next steps:")
        print("   1. Run 'docker-compose up -d' in nessie-catalog/ to start Nessie")
        print("   2. Ensure ICE_CATALOG_TYPE=nessie in .env")
        print("   3. Run 'python bot_iceberg_loader.py' to recreate tables")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


if __name__ == "__main__":
    success = clean_warehouse()
    sys.exit(0 if success else 1)
