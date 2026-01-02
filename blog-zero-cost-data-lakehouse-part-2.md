---
title: "Part 2 - Loading CSV Data into Apache Iceberg Tables with PyIceberg and PyArrow"
description: "A practical guide to creating Iceberg tables and loading CSV data using PyIceberg with pluggable catalog and storage backends. Supports remote URLs, local files, and glob patterns."
slug: zero-cost-data-lakehouse-part-2-loading-data
authors:
  - name: Senthilnathan Karuppaiah
    title: Data Engineer
    url: https://github.com/senthilsweb
tags: [apache-iceberg, pyiceberg, data-lakehouse, csv-to-iceberg, data-engineering]
image: /coverimages/ci_part-2-loading-csv-data-into-apache-iceberg-tables-with-pyiceberg-and-pyarrow.png
hide_table_of_contents: false
date: 2026-01-02
linkedin_post: |
  Part 2 of my $0 Data Lakehouse series is live!

  This time: Loading CSV data into Apache Iceberg tables.

  New in this version:
  → Load from remote URLs (GitHub, S3, any HTTP)
  → Load from local files or directories with glob patterns
  → Auto table naming from filename (optional pluralization)
  → Streaming download for large remote files

  Full article with code walkthrough 👇

  #ApacheIceberg #DataLakehouse #DataEngineering #PyIceberg #OpenSource
---

# Part 2 - Loading CSV Data into Apache Iceberg Tables with PyIceberg and PyArrow

In [Part 1 - Building a $0 Data Lakehouse with Apache Iceberg, Nessie, Neon, and Cloudflare R2](https://www.linkedin.com/pulse/building-0-data-lakehouse-apache-iceberg-nessie-neon-r2-karuppaiah-lgeqe/), we set up the infrastructure — a Nessie catalog on Fly.io, Neon PostgreSQL for metadata, and Cloudflare R2 for object storage. All on free tiers.

Now comes the interesting part: **actually loading data into Iceberg tables**.

In this article, I'll walk you through a Python-based approach that:

- Reads CSV files from **remote URLs**, **local files**, or **directories with glob patterns**
- Supports multiple catalog backends (REST catalog like Nessie, or simple file-based SQLite)
- Works with any S3-compatible storage (cloud or self-hosted)
- Handles schema conversion and table naming automatically

<!--truncate-->

## The Approach

The goal is simple: take a CSV file and turn it into a proper Iceberg table with full metadata, schema evolution support, and time-travel capabilities.

Here's the high-level flow:

```
Source (URL/File/Glob) → Download (if remote) → PyArrow Table → Iceberg Schema → Create Table → Append Data
```

What makes this flexible:

1. **Multiple Source Types**: Load from HTTP URLs, local files, or batch-load multiple files using glob patterns.

2. **Pluggable Catalog**: Switch between a REST catalog (like Nessie) for production or a local SQLite catalog for development — just change an environment variable.

3. **Storage Agnostic**: Works with any S3-compatible storage. Cloud services or self-hosted solutions — the code doesn't care.

4. **Automatic Schema Inference**: PyArrow reads the CSV and infers types. We then map those to Iceberg types.

5. **Smart Table Naming**: Derive table names from filenames automatically, with optional pluralization.

## Source Types

The loader detects and handles three source types:

### 1. Remote URLs

Load CSV directly from any HTTP/HTTPS URL:

```bash
ICE_SOURCE_PATH="https://raw.githubusercontent.com/user/repo/main/data.csv"
```

Remote files are **streamed chunk-by-chunk** to a temp file (8KB chunks), so large files won't exhaust memory during download.

### 2. Local Files

Load from a local file path:

```bash
ICE_SOURCE_PATH="./data/customers.csv"
```

### 3. Directory with Glob Pattern

Load multiple files matching a pattern:

```bash
ICE_SOURCE_PATH="./data/"
ICE_GLOB_PATTERN="*.csv"
```

Each file becomes a separate table (unless `ICE_GLOB_MERGE_TABLE=true`).

## Key Components

### 1. Reading CSV with PyArrow

PyArrow handles CSV parsing efficiently and gives us a typed table:

```python
import pyarrow.csv as csv

arrow_table = csv.read_csv(
    csv_path,
    read_options=csv.ReadOptions(use_threads=True),
    parse_options=csv.ParseOptions(newlines_in_values=True)
)
```

This returns an Arrow table with inferred schema — integers, strings, floats, timestamps, and more.

### 2. Schema Conversion

Iceberg has its own type system. We map Arrow types to Iceberg types:

| Arrow Type | Iceberg Type |
|------------|--------------|
| string | StringType |
| int64 | LongType |
| int32 | IntegerType |
| float64 | DoubleType |
| timestamp | TimestampType |
| date | DateType |
| boolean | BooleanType |

For unsupported types, we default to string — safe and queryable.

### 3. Configuring the Catalog

The loader supports two catalog types:

**REST Catalog (Nessie)**
```python
from pyiceberg.catalog.rest import RestCatalog

catalog = RestCatalog(
    "nessie",
    uri="https://your-nessie-instance/iceberg/",
    warehouse="s3://your-bucket/warehouse",
    **s3_credentials
)
```

**File-based Catalog (SQLite)**
```python
from pyiceberg.catalog.sql import SqlCatalog

catalog = SqlCatalog(
    "local_catalog",
    uri="sqlite:///iceberg_catalog.db",
    warehouse="s3://your-bucket/warehouse",
    **s3_credentials
)
```

The SQLite option is great for local development and testing. No external services needed.

### 4. Creating and Loading the Table

Once we have the catalog and schema, creating the table is straightforward:

```python
# Create namespace if needed
catalog.create_namespace("default")

# Create the table
table = catalog.create_table(
    identifier="default.my_table",
    schema=iceberg_schema,
    location="s3://bucket/warehouse/my_table"
)

# Append data
table.append(arrow_table)
```

That's it. Iceberg handles:
- Writing Parquet data files to storage
- Creating metadata JSON files
- Managing snapshots for time-travel

## Configuration via Environment Variables

The loader uses environment variables for all configuration:

```bash
# Source Configuration
ICE_SOURCE_PATH=https://example.com/data.csv  # URL, file path, or directory
ICE_GLOB_PATTERN=*.csv                        # Glob pattern for directories
ICE_TABLE_NAME=                               # Optional - derived from filename if empty

# Table Naming Options
ICE_PLURALIZE_TABLE=false    # true to pluralize: users.csv → users
ICE_GLOB_MERGE_TABLE=false   # true to merge glob files into single table

# Catalog Type: 'rest' (Nessie) or 'sql' (SQLite)
ICE_CATALOG_TYPE=rest
ICE_NAMESPACE=default

# For Nessie catalog
ICE_NESSIE_URI=https://your-nessie-instance/iceberg/

# Storage credentials (works with any S3-compatible service)
R2_ENDPOINT=https://your-s3-endpoint.com
R2_ACCESS_KEY=your-access-key
R2_SECRET_KEY=your-secret-key
R2_REGION=auto

# Iceberg settings
ICE_WAREHOUSE_PATH=s3://your-bucket/warehouse
```

## Running the Loader

### Example 1: Remote URL

```bash
ICE_SOURCE_PATH="https://raw.githubusercontent.com/senthilsweb/datasets/main/ticket/users.csv" \
ICE_NAMESPACE="ticketdb" \
ICE_CATALOG_TYPE="rest" \
python bot_iceberg_loader.py
```

Output:
```
🚀 CSV → Iceberg Table Loader
📅 2026-01-02 15:00:08

� Source: https://raw.githubusercontent.com/.../users.csv
   Type: remote
   Files: 1

🔧 Configuring catalog (type: rest)...
   ✅ Nessie at https://nessie-iceberg.fly.dev/iceberg/
   ✅ Namespace 'ticketdb' created

📄 Processing: users.csv
   Table: ticketdb.users
   ✅ Loaded 49990 rows, 18 columns
   ✅ Table created at: s3://iceberg-demo/warehouse/ticketdb/users_...
   📤 Wrote 49990 rows

==================================================
📊 Summary
   ✅ Success: 1
   ❌ Failed: 0
```

### Example 2: Local Directory with Glob

```bash
ICE_SOURCE_PATH="./data/" \
ICE_GLOB_PATTERN="sales_*.csv" \
ICE_NAMESPACE="analytics" \
python bot_iceberg_loader.py
```

This loads all files matching `sales_*.csv`, creating a separate table for each.

## Querying the Data

Once loaded, you can query the table:

```python
from pyiceberg.catalog.rest import RestCatalog

catalog = RestCatalog("nessie", uri="https://your-nessie/iceberg/", ...)
table = catalog.load_table("default.customers")

# Full scan to Pandas
df = table.scan().to_pandas()

# Or filter with row-level predicates
df = table.scan(
    row_filter="country = 'USA'"
).to_pandas()
```

## Cleaning Up

Need to start fresh? A cleanup script removes all objects from the warehouse:

```python
import s3fs

fs = s3fs.S3FileSystem(
    endpoint_url=s3_endpoint,
    key=access_key,
    secret=secret_key
)

# List and delete all files
for file in fs.find("your-bucket/warehouse"):
    fs.rm(file)
```

The script includes confirmation prompts to prevent accidental deletion.

## Lessons Learned

**1. Schema inference works well, but verify**

PyArrow does a good job, but always check the inferred types. A column that looks like integers might have nulls that cause issues.

**2. SQLite catalog is surprisingly useful**

For development and testing, SQLite removes the need for any external services. Switch to Nessie when you need multi-user access or Git-like branching.

**3. S3 path-style access matters**

Some S3-compatible services require path-style access (`bucket.endpoint/key` vs `endpoint/bucket/key`). Set `s3.path-style-access=true` if you hit issues.

**4. Logging saves debugging time**

The loader writes structured JSON logs. When something fails, the logs show exactly where and why.

## What's Next

With data in Iceberg, you can:

- Query with Spark, Trino, or DuckDB
- Set up incremental loads with snapshot isolation
- Implement schema evolution without rewriting data
- Use Nessie branches for safe experimentation

## Source Code

The complete loader script with documentation is available on GitHub:

**Repository:** [github.com/senthilsweb/apache-iceberg](https://github.com/senthilsweb/apache-iceberg)

Files:
- `bot_iceberg_loader.py` — Main loader script
- `utils.py` — Utility functions (URL detection, glob, table naming)
- `clean_r2_warehouse.py` — Cleanup utility
- `sample.env` — Sample configuration

---

In Part 1, we built the infrastructure. In Part 2, we loaded data. The foundation is set.

The lakehouse is ready for queries.
