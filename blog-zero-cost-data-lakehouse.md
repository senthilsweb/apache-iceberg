---
title: "Building a $0 Data Lakehouse with Apache Iceberg, Nessie, Neon, and Cloudflare R2"
description: "A complete guide to setting up a modern, serverless data lakehouse using only free-tier cloud services"
slug: zero-cost-data-lakehouse-iceberg-nessie-neon-r2
authors:
  - name: Senthilnathan Karuppaiah
    title: Data Engineer
    url: https://github.com/senthilsweb
tags: [apache-iceberg, nessie, neon, cloudflare-r2, data-lakehouse, serverless, free-tier]
image: /img/blog/data-lakehouse-architecture.png
hide_table_of_contents: false
date: 2025-12-05
---

# Building a $0 Data Lakehouse with Apache Iceberg, Nessie, Neon, and Cloudflare R2

Modern data lakehouses typically require expensive infrastructure—object storage, metadata catalogs, and databases. But what if you could build a production-ready Apache Iceberg lakehouse using only **free-tier services**?

In this article, I'll show you how to set up a complete data lakehouse stack for **$0/month** using:

- **Nessie** - Git-like catalog for Apache Iceberg (Fly.io free tier)
- **Neon** - Serverless PostgreSQL for catalog metadata (free tier)
- **Cloudflare R2** - S3-compatible object storage (10GB free)

<!--truncate-->

## Architecture Overview

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   PyIceberg     │────▶│     Nessie      │────▶│  Cloudflare R2  │
│   Client        │     │  (Fly.io)       │     │  (Object Store) │
└─────────────────┘     └────────┬────────┘     └─────────────────┘
                                 │
                        ┌────────▼────────┐
                        │      Neon       │
                        │   PostgreSQL    │
                        └─────────────────┘
```

### Component Breakdown

| Component | Role | Free Tier |
|-----------|------|-----------|
| **Nessie** | Iceberg REST catalog with Git-like branching | Fly.io: 3 shared VMs |
| **Neon** | Version store for Nessie metadata | 0.5GB storage, 190 compute hours |
| **Cloudflare R2** | Parquet file storage | 10GB storage, 1M requests |

## Step 1: Set Up Neon PostgreSQL

1. Create account at [neon.tech](https://neon.tech)
2. Create a new project
3. Copy the connection string:

```
postgresql://user:password@ep-xxx.region.aws.neon.tech:5432/neondb?sslmode=require
```

Neon provides serverless PostgreSQL that scales to zero when idle—perfect for a catalog that's not constantly accessed.

## Step 2: Set Up Cloudflare R2

1. Create account at [cloudflare.com](https://cloudflare.com)
2. Navigate to R2 Object Storage
3. Create a bucket (e.g., `iceberg-demo`)
4. Generate R2 API tokens with read/write access
5. Note your Account ID from the URL

Your endpoint will be: `https://<account-id>.r2.cloudflarestorage.com`

## Step 3: Deploy Nessie to Fly.io

### Project Structure

```
nessie-catalog/
├── Dockerfile
├── docker-compose.yml   # Local development
├── fly.toml             # Fly.io config
├── .env                 # Secrets (local only)
└── requirements.txt
```

### Dockerfile

```dockerfile
FROM ghcr.io/projectnessie/nessie:0.93.0

EXPOSE 19120

CMD ["/opt/jboss/container/java/run/run-java.sh"]
```

### fly.toml

```toml
app = 'nessie-iceberg'
primary_region = 'ams'

[build]
  dockerfile = 'Dockerfile'

[env]
  NESSIE_VERSION_STORE_TYPE = "JDBC"
  NESSIE_CATALOG_DEFAULT_WAREHOUSE = "warehouse"
  NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_PATH_STYLE_ACCESS = "true"
  NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_REGION = "auto"
  
  # Critical for HTTPS proxy
  QUARKUS_HTTP_PROXY_PROXY_ADDRESS_FORWARDING = "true"
  QUARKUS_HTTP_PROXY_ENABLE_FORWARDED_HOST = "true"
  NESSIE_CATALOG_SERVICE_BASE_URI = "https://nessie-iceberg.fly.dev/"

[http_service]
  internal_port = 19120
  force_https = true
  auto_stop_machines = 'stop'
  auto_start_machines = true
  min_machines_running = 1

[[vm]]
  memory = '1gb'
  cpu_kind = 'shared'
  cpus = 1
```

### Deploy

```bash
# Install Fly CLI
curl -L https://fly.io/install.sh | sh

# Login
fly auth login

# Create app
fly launch --name nessie-iceberg --region ams --no-deploy

# Set secrets
fly secrets set \
  QUARKUS_DATASOURCE_JDBC_URL="jdbc:postgresql://ep-xxx.neon.tech:5432/neondb?sslmode=require" \
  QUARKUS_DATASOURCE_USERNAME="neondb_owner" \
  QUARKUS_DATASOURCE_PASSWORD="your-password" \
  NESSIE_CATALOG_WAREHOUSES_WAREHOUSE_LOCATION="s3://iceberg-demo/warehouse" \
  NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_ENDPOINT="https://account-id.r2.cloudflarestorage.com" \
  NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_ACCESS_KEY_NAME="your-r2-access-key" \
  NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_ACCESS_KEY_SECRET="your-r2-secret-key"

# Deploy
fly deploy
```

## Step 4: Load Data with PyIceberg

### Install Dependencies

```bash
pip install "pyiceberg[s3fs,pyarrow]" pyarrow python-dotenv structlog boto3
```

### CSV to Iceberg Loader Script

```python
#!/usr/bin/env python3
"""Load CSV files to Apache Iceberg tables via Nessie catalog."""

import pyarrow.csv as csv
import os
from dotenv import load_dotenv
from pyiceberg.catalog import load_catalog

load_dotenv()

# Configuration
CSV_FILE = os.getenv('ICE_CSV_FILE_PATH')
TABLE_NAME = os.getenv('ICE_TABLE_NAME')
NAMESPACE = os.getenv('ICE_NAMESPACE', 'default')

# Connect to Nessie catalog
catalog = load_catalog('nessie', **{
    'type': 'rest',
    'uri': os.getenv('ICE_NESSIE_URI'),
    'warehouse': os.getenv('ICE_WAREHOUSE_PATH'),
    's3.endpoint': os.getenv('R2_ENDPOINT'),
    's3.access-key-id': os.getenv('R2_ACCESS_KEY'),
    's3.secret-access-key': os.getenv('R2_SECRET_KEY'),
    's3.region': 'auto',
})

# Read CSV
arrow_table = csv.read_csv(CSV_FILE)
print(f"Loaded {arrow_table.num_rows} rows")

# Create namespace if needed
try:
    catalog.create_namespace(NAMESPACE)
except:
    pass

# Create table and load data
table_id = f"{NAMESPACE}.{TABLE_NAME}"
table = catalog.create_table(
    identifier=table_id,
    schema=arrow_table.schema,
)
table.append(arrow_table)

print(f"✅ Created table: {table_id}")
print(f"📍 Location: {table.location()}")
```

### Usage

```bash
# Set environment variables
export ICE_NESSIE_URI=https://nessie-iceberg.fly.dev/iceberg/
export ICE_WAREHOUSE_PATH=s3://iceberg-demo/warehouse
export R2_ENDPOINT=https://account-id.r2.cloudflarestorage.com
export R2_ACCESS_KEY=your-access-key
export R2_SECRET_KEY=your-secret-key

# Load a CSV
ICE_CSV_FILE_PATH=/data/sales.csv ICE_TABLE_NAME=sales python bot_iceberg_loader.py
```

## Step 5: Query Your Data

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog('nessie', **{
    'type': 'rest',
    'uri': 'https://nessie-iceberg.fly.dev/iceberg/',
    'warehouse': 's3://iceberg-demo/warehouse',
    's3.endpoint': 'https://account-id.r2.cloudflarestorage.com',
    's3.access-key-id': 'your-key',
    's3.secret-access-key': 'your-secret',
    's3.region': 'auto',
})

# List tables
print(catalog.list_tables('default'))

# Query table
table = catalog.load_table('default.sales')
df = table.scan().to_arrow().to_pandas()
print(df.head())
```

## Cleanup Script

```python
#!/usr/bin/env python3
"""Clean up R2 warehouse - deletes all Iceberg data!"""

import boto3
from botocore.config import Config
import os
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client(
    's3',
    endpoint_url=os.getenv('R2_ENDPOINT'),
    aws_access_key_id=os.getenv('R2_ACCESS_KEY'),
    aws_secret_access_key=os.getenv('R2_SECRET_KEY'),
    region_name='auto',
    config=Config(signature_version='s3v4')
)

bucket = 'iceberg-demo'
prefix = 'warehouse/'

# List and delete all objects
paginator = s3.get_paginator('list_objects_v2')
for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
    for obj in page.get('Contents', []):
        s3.delete_object(Bucket=bucket, Key=obj['Key'])
        print(f"Deleted: {obj['Key']}")

print("✅ Warehouse cleaned")
```

## Challenges and Solutions

### Challenge 1: Nessie URN Secret Resolution

**Problem:** Nessie's default secret mechanism (`urn:nessie-secret:quarkus:`) wasn't resolving R2 credentials from environment variables.

**Solution:** Use direct credential configuration instead:
```
NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_ACCESS_KEY_NAME=<key>
NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_ACCESS_KEY_SECRET=<secret>
```

### Challenge 2: HTTPS Proxy on Fly.io

**Problem:** Nessie returned `http://` URLs in API responses, causing PyIceberg to fail with redirect/validation errors.

**Solution:** Configure Quarkus to recognize the HTTPS proxy:
```toml
QUARKUS_HTTP_PROXY_PROXY_ADDRESS_FORWARDING = "true"
QUARKUS_HTTP_PROXY_ENABLE_FORWARDED_HOST = "true"
NESSIE_CATALOG_SERVICE_BASE_URI = "https://nessie-iceberg.fly.dev/"
```

### Challenge 3: R2 SSL Handshake Failures

**Problem:** SSL handshake failures when connecting to Cloudflare R2.

**Solution:** This was caused by a typo in the R2 Account ID. Double-check your endpoint URL matches exactly what's shown in the Cloudflare dashboard.

### Challenge 4: Health Endpoint Returns 404

**Problem:** `/q/health` returns 404 on Fly.io.

**Solution:** Quarkus health endpoints are on a separate management port (9000), which isn't exposed on Fly.io. Use `/api/v2/trees` to verify the service is running.

## Cost Breakdown

| Service | Free Tier Limits | Our Usage |
|---------|-----------------|-----------|
| **Fly.io** | 3 shared VMs, 160GB bandwidth | 1 VM, minimal bandwidth |
| **Neon** | 0.5GB storage, 190 compute hours | ~10MB, few hours |
| **Cloudflare R2** | 10GB storage, 1M requests | ~50MB, few thousand requests |

**Total Monthly Cost: $0** ✅

## API Endpoints Reference

Replace `{BASE_URL}` with your Nessie deployment URL (e.g., `https://your-app.fly.dev` or `http://localhost:19120`).

The `{prefix}` placeholder represents the URL-encoded prefix pattern: `{branch}|{warehouse}` (e.g., `main%7Cs3%3A%2F%2Ficeberg-demo%2Fwarehouse`).

### Core Endpoints

| Endpoint | URL Pattern | Description |
|----------|-------------|-------------|
| Web UI | `{BASE_URL}/` | Nessie web interface |
| REST Catalog | `{BASE_URL}/iceberg/` | PyIceberg/Spark catalog URI |
| Catalog Config | `{BASE_URL}/iceberg/v1/config?warehouse={warehouse}` | Get catalog configuration |
| Nessie Core API | `{BASE_URL}/api/` | Nessie native API base |

### Iceberg REST Catalog API (v1)

| Operation | Method | URL Pattern |
|-----------|--------|-------------|
| **Namespaces** | | |
| List Namespaces | `GET` | `{BASE_URL}/iceberg/v1/{prefix}/namespaces` |
| Create Namespace | `POST` | `{BASE_URL}/iceberg/v1/{prefix}/namespaces` |
| Get Namespace | `GET` | `{BASE_URL}/iceberg/v1/{prefix}/namespaces/{namespace}` |
| Drop Namespace | `DELETE` | `{BASE_URL}/iceberg/v1/{prefix}/namespaces/{namespace}` |
| Update Namespace Properties | `POST` | `{BASE_URL}/iceberg/v1/{prefix}/namespaces/{namespace}/properties` |
| **Tables** | | |
| List Tables | `GET` | `{BASE_URL}/iceberg/v1/{prefix}/namespaces/{namespace}/tables` |
| Create Table | `POST` | `{BASE_URL}/iceberg/v1/{prefix}/namespaces/{namespace}/tables` |
| Load Table | `GET` | `{BASE_URL}/iceberg/v1/{prefix}/namespaces/{namespace}/tables/{table}` |
| Update Table | `POST` | `{BASE_URL}/iceberg/v1/{prefix}/namespaces/{namespace}/tables/{table}` |
| Drop Table | `DELETE` | `{BASE_URL}/iceberg/v1/{prefix}/namespaces/{namespace}/tables/{table}` |
| Rename Table | `POST` | `{BASE_URL}/iceberg/v1/{prefix}/tables/rename` |
| Table Metrics | `POST` | `{BASE_URL}/iceberg/v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics` |
| **Views** | | |
| List Views | `GET` | `{BASE_URL}/iceberg/v1/{prefix}/namespaces/{namespace}/views` |
| Create View | `POST` | `{BASE_URL}/iceberg/v1/{prefix}/namespaces/{namespace}/views` |
| Load View | `GET` | `{BASE_URL}/iceberg/v1/{prefix}/namespaces/{namespace}/views/{view}` |
| Replace View | `PUT` | `{BASE_URL}/iceberg/v1/{prefix}/namespaces/{namespace}/views/{view}` |
| Drop View | `DELETE` | `{BASE_URL}/iceberg/v1/{prefix}/namespaces/{namespace}/views/{view}` |
| Rename View | `POST` | `{BASE_URL}/iceberg/v1/{prefix}/views/rename` |
| **S3 Signing** | | |
| Sign S3 Request | `POST` | `{BASE_URL}/iceberg/v1/{prefix}/s3-sign/{identifier}` |

### Nessie API (v2)

| Operation | Method | URL Pattern |
|-----------|--------|-------------|
| **Trees/Branches** | | |
| List Branches/Tags | `GET` | `{BASE_URL}/api/v2/trees` |
| Get Default Branch | `GET` | `{BASE_URL}/api/v2/trees/-` |
| Get Branch/Tag | `GET` | `{BASE_URL}/api/v2/trees/{ref}` |
| Create Branch/Tag | `POST` | `{BASE_URL}/api/v2/trees` |
| Delete Branch/Tag | `DELETE` | `{BASE_URL}/api/v2/trees/{ref}` |
| Assign Branch/Tag | `PUT` | `{BASE_URL}/api/v2/trees/{ref}` |
| **Entries** | | |
| List Entries | `GET` | `{BASE_URL}/api/v2/trees/{ref}/entries` |
| Get Multiple Contents | `POST` | `{BASE_URL}/api/v2/trees/{ref}/contents` |
| Get Content | `GET` | `{BASE_URL}/api/v2/trees/{ref}/contents/{key}` |
| **History** | | |
| Commit Log | `GET` | `{BASE_URL}/api/v2/trees/{ref}/history` |
| **Commits** | | |
| Commit Changes | `POST` | `{BASE_URL}/api/v2/trees/{ref}/history/commit` |
| Merge Branch | `POST` | `{BASE_URL}/api/v2/trees/{ref}/history/merge` |
| Transplant Commits | `POST` | `{BASE_URL}/api/v2/trees/{ref}/history/transplant` |
| **Diffs** | | |
| Get Diff | `GET` | `{BASE_URL}/api/v2/trees/{fromRef}/diff/{toRef}` |
| **Config** | | |
| Get Config | `GET` | `{BASE_URL}/api/v2/config` |

### Example API Calls

```bash
# Get catalog configuration
curl "{BASE_URL}/iceberg/v1/config?warehouse=s3://your-bucket/warehouse"

# List all namespaces
curl "{BASE_URL}/iceberg/v1/{prefix}/namespaces"

# List tables in a namespace
curl "{BASE_URL}/iceberg/v1/{prefix}/namespaces/default/tables"

# Get table metadata
curl "{BASE_URL}/iceberg/v1/{prefix}/namespaces/default/tables/my_table"

# List Nessie branches
curl "{BASE_URL}/api/v2/trees"

# Get commit history
curl "{BASE_URL}/api/v2/trees/main/history"

# List all entries on main branch
curl "{BASE_URL}/api/v2/trees/main/entries"
```

### URL Encoding Reference

The `{prefix}` must be URL-encoded. Common encodings:

| Character | Encoded |
|-----------|---------|
| `\|` (pipe) | `%7C` |
| `:` | `%3A` |
| `/` | `%2F` |

**Example:** `main|s3://iceberg-demo/warehouse` → `main%7Cs3%3A%2F%2Ficeberg-demo%2Fwarehouse`

## Conclusion

You now have a fully functional Apache Iceberg data lakehouse running on:

- ✅ **Serverless PostgreSQL** (Neon) for metadata
- ✅ **S3-compatible storage** (Cloudflare R2) for Parquet files
- ✅ **REST Catalog** (Nessie on Fly.io) for multi-client access
- ✅ **Git-like versioning** with Nessie branches

All for **$0/month** on free tiers!

### Next Steps

- Add authentication to Nessie (OIDC/OAuth2)
- Set up Spark/Trino to query the lakehouse
- Implement data quality checks
- Create CI/CD pipeline for schema evolution

---

**Repository:** [github.com/senthilsweb/nessie-catalog](https://github.com/senthilsweb)

**Live Demo:** [nessie-iceberg.fly.dev](https://nessie-iceberg.fly.dev/)
