# Apache Iceberg Data Lakehouse

Zero-cost Apache Iceberg lakehouse lab: Nessie catalog on Fly.io, Cloudflare R2 storage, Neon PostgreSQL backend, and PyIceberg data loader.

## 🏗️ Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   CSV Source    │────▶│   PyIceberg      │────▶│  Iceberg Table  │
│ (URL/File/Glob) │     │   + PyArrow      │     │    (Parquet)    │
└─────────────────┘     └────────┬─────────┘     └────────▲────────┘
                                 │                        │
                        ┌────────▼─────────┐     ┌────────┴────────┐
                        │  Nessie Catalog  │     │  Cloudflare R2  │
                        │   (Fly.io)       │────▶│    (S3 API)     │
                        └────────┬─────────┘     └─────────────────┘
                                 │
                        ┌────────▼─────────┐
                        │ Neon PostgreSQL  │
                        │   (Metadata)     │
                        └──────────────────┘
```

## ✨ Features

- **Pluggable Catalogs**: Nessie REST catalog or SQLite for local dev
- **Flexible Data Sources**: Load from remote URLs, local files, or directories with glob patterns
- **Auto Schema Inference**: PyArrow auto-detects CSV column types → Iceberg schema
- **Table Naming**: Auto-derive from filename or specify explicitly
- **Multi-file Loading**: Process multiple CSV files with glob patterns
- **Zero Cost**: Uses free tiers of Fly.io, Cloudflare R2, and Neon

## 🚀 Quick Start

### 1. Clone and Setup

```bash
git clone https://github.com/senthilsweb/apache-iceberg.git
cd apache-iceberg
python3 -m venv env
source env/bin/activate
pip install -r requirements.txt
```

### 2. Configure Environment

```bash
cp sample.env .env
# Edit .env with your credentials
```

### 3. Load Data

```bash
# Option A: Using Nessie REST Catalog (Production)
ICE_SOURCE_PATH="https://raw.githubusercontent.com/senthilsweb/datasets/main/ticket/users.csv" \
ICE_NAMESPACE="ticketdb" \
ICE_TABLE_NAME="" \
ICE_CATALOG_TYPE="rest" \
ICE_NESSIE_URI="https://nessie-iceberg.fly.dev/iceberg/" \
ICE_WAREHOUSE_PATH="s3://iceberg-demo/warehouse" \
R2_ENDPOINT="https://your-account.r2.cloudflarestorage.com" \
R2_ACCESS_KEY="your-access-key" \
R2_SECRET_KEY="your-secret-key" \
python bot_iceberg_loader.py

# Option B: Using SQLite Catalog (Local Development)
ICE_SOURCE_PATH="./data/sales.csv" \
ICE_NAMESPACE="default" \
ICE_TABLE_NAME="sales" \
ICE_CATALOG_TYPE="sql" \
ICE_CATALOG_DB_PATH="./iceberg_catalog.db" \
ICE_WAREHOUSE_PATH="s3://your-bucket/warehouse" \
R2_ENDPOINT="https://your-account.r2.cloudflarestorage.com" \
R2_ACCESS_KEY="your-access-key" \
R2_SECRET_KEY="your-secret-key" \
python bot_iceberg_loader.py

# Directory with glob pattern (Nessie)
ICE_SOURCE_PATH="./data/" \
ICE_GLOB_PATTERN="*.csv" \
ICE_NAMESPACE="imports" \
ICE_CATALOG_TYPE="rest" \
ICE_NESSIE_URI="https://nessie-iceberg.fly.dev/iceberg/" \
ICE_WAREHOUSE_PATH="s3://iceberg-demo/warehouse" \
R2_ENDPOINT="https://your-account.r2.cloudflarestorage.com" \
R2_ACCESS_KEY="your-access-key" \
R2_SECRET_KEY="your-secret-key" \
python bot_iceberg_loader.py
```

## ⚙️ Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `ICE_SOURCE_PATH` | URL, file path, or directory | Required |
| `ICE_GLOB_PATTERN` | Glob pattern for directories | `*.csv` |
| `ICE_TABLE_NAME` | Table name (derived from filename if empty) | - |
| `ICE_NAMESPACE` | Iceberg namespace | `default` |
| `ICE_CATALOG_TYPE` | `rest` (Nessie) or `sql` (SQLite) | `rest` |
| `ICE_PLURALIZE_TABLE` | Pluralize table names | `false` |
| `ICE_GLOB_MERGE_TABLE` | Merge glob files into single table | `false` |
| `ICE_NESSIE_URI` | Nessie catalog URI | - |
| `ICE_WAREHOUSE_PATH` | S3 warehouse path | - |
| `R2_ENDPOINT` | Cloudflare R2 endpoint | - |
| `R2_ACCESS_KEY` | R2 access key | - |
| `R2_SECRET_KEY` | R2 secret key | - |
| `R2_REGION` | R2 region | `auto` |

### Sample .env

```env
# Source
ICE_SOURCE_PATH=https://example.com/data.csv
ICE_NAMESPACE=default

# Catalog
ICE_CATALOG_TYPE=rest
ICE_NESSIE_URI=https://nessie-iceberg.fly.dev/iceberg/
ICE_WAREHOUSE_PATH=s3://iceberg-demo/warehouse

# Storage (Cloudflare R2)
R2_ENDPOINT=https://your-account.r2.cloudflarestorage.com
R2_ACCESS_KEY=your-access-key
R2_SECRET_KEY=your-secret-key
R2_REGION=auto
```

## 📁 Project Structure

```
├── bot_iceberg_loader.py  # Main CSV to Iceberg loader
├── utils.py               # Utility functions (URL detection, glob, temp files)
├── clean_r2_warehouse.py  # Utility to clean R2 warehouse
├── requirements.txt       # Python dependencies
├── sample.env             # Sample environment configuration
├── docker-compose.yml     # Local Nessie development setup
├── Dockerfile             # Nessie server for Fly.io
├── fly.toml               # Fly.io deployment config
└── logs/                  # Execution logs
```

## 🧪 Tested Configuration

Successfully tested with:
- **Nessie Catalog**: `https://nessie-iceberg.fly.dev/iceberg/`
- **Storage**: Cloudflare R2 (`s3://iceberg-demo/warehouse`)
- **Data Source**: Remote CSV from GitHub
- **Result**: 49,990 rows loaded to `ticketdb.users` table

##  License

MIT License
