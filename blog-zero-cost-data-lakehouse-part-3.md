---
title: "Part 3 - Querying Apache Iceberg Tables with PyIceberg, DuckDB, and JupySQL"
description: "Interactive SQL analytics on your Iceberg lakehouse using PyIceberg for catalog access, DuckDB for fast queries, and JupySQL for clean notebook workflows."
slug: zero-cost-data-lakehouse-part-3-querying-data
authors:
  - name: Senthilnathan Karuppaiah
    title: Data Engineer
    url: https://github.com/senthilsweb
tags: [apache-iceberg, pyiceberg, duckdb, jupysql, data-lakehouse, data-analytics]
image: /coverimages/ci_part-3-querying-apache-iceberg-tables-with-pyiceberg-duckdb-jupysql.png
hide_table_of_contents: false
date: 2026-01-02
linkedin_post: |
  Part 3 of my $0 Data Lakehouse series is live!

  This time: Querying Iceberg tables with DuckDB + JupySQL.

  What you'll learn:
  → Why DuckDB can't query R2 directly (SSL handshake issue)
  → The PyIceberg → Arrow → DuckDB bridge pattern
  → Writing pure SQL in Jupyter notebooks with %%sql magic
  → Aggregations, filtering, and data export

  Full walkthrough with working notebook 👇

  #ApacheIceberg #DuckDB #DataLakehouse #DataEngineering #JupySQL
---

# Part 3 - Querying Apache Iceberg Tables with PyIceberg, DuckDB, and JupySQL

In [Part 1](https://www.linkedin.com/pulse/building-0-data-lakehouse-apache-iceberg-nessie-neon-r2-karuppaiah-lgeqe/), we built the infrastructure. In [Part 2](./blog-zero-cost-data-lakehouse-part-2.md), we loaded CSV data into Iceberg tables.

Now for the fun part: **querying the data**.

The goal was simple — run SQL queries against our Iceberg tables using DuckDB. But I ran into a snag that forced a different approach. Here's what happened and how I solved it.

<!--truncate-->

## The Plan vs. Reality

**The plan:** Use DuckDB's native Iceberg extension to query tables directly from Cloudflare R2.

```sql
-- This was the dream
INSTALL iceberg;
LOAD iceberg;
SELECT * FROM iceberg_scan('s3://bucket/warehouse/table/metadata.json');
```

**The reality:** SSL handshake failures.

```
IO Error: Could not establish SSL connection
```

DuckDB's Iceberg extension has compatibility issues with Cloudflare R2's SSL configuration when running from local environments. The connection just doesn't work.

## The Workaround: PyIceberg → Arrow → DuckDB

Instead of fighting the SSL issue, I found a cleaner path:

1. **PyIceberg** connects to Nessie and reads table metadata
2. **Scan to Arrow** pulls the data as an Arrow table
3. **Register in DuckDB** for SQL queries

```
Nessie (Catalog) → PyIceberg → Arrow Table → DuckDB (in-memory) → SQL Queries
```

This actually has an advantage: PyIceberg handles table location discovery automatically. Iceberg tables have auto-generated UUIDs in their paths like `s3://bucket/warehouse/ticketdb/users_2550a354-480b-412d-89a0-d3473a40f820`. Without PyIceberg, you'd need to remember these hash values.

## Setting Up the Query Environment

### Connect to Nessie Catalog

```python
from pyiceberg.catalog.rest import RestCatalog

catalog = RestCatalog(
    'nessie',
    uri='https://nessie-iceberg.fly.dev/iceberg/',
    warehouse='s3://iceberg-demo/warehouse',
    **{
        's3.endpoint': 'https://your-r2-endpoint.r2.cloudflarestorage.com',
        's3.access-key-id': 'your-access-key',
        's3.secret-access-key': 'your-secret-key',
        's3.region': 'auto',
        's3.path-style-access': 'true'
    }
)
```

### List Available Tables

```python
for ns in catalog.list_namespaces():
    print(f"📁 {ns[0]}")
    for table in catalog.list_tables(ns[0]):
        print(f"    └── {table[1]}")
```

Output:
```
📁 default
    └── nyc_brooklyn_temperature_sensors
    └── ppp_loans
    └── sap_partners
📁 ticketdb
    └── netflix_titles
    └── users
```

### Load Table into DuckDB

```python
import duckdb

table = catalog.load_table('ticketdb.users')
arrow_table = table.scan().to_arrow()

conn = duckdb.connect()
conn.register('users', arrow_table)

print(f"✅ Loaded {arrow_table.num_rows:,} rows")
print(f"📍 Location: {table.location()}")
```

Output:
```
✅ Loaded 49,990 rows
📍 Location: s3://iceberg-demo/warehouse/ticketdb/users_2550a354-480b-412d-89a0-d3473a40f820
```

## Enter JupySQL

Writing `conn.execute("SELECT...")` gets tedious. JupySQL lets you write pure SQL in notebook cells with `%%sql` magic.

### Setup JupySQL

```python
%load_ext sql
%config SqlMagic.autopandas = True
%config SqlMagic.feedback = True
%sql duckdb:///:memory:
```

There's a trick here — JupySQL creates its own DuckDB connection, separate from the one we used to register the Arrow table. We need to register the table in JupySQL's connection:

```python
from sql import connection as sql_connection

current_conn = sql_connection.ConnectionManager.current
raw_conn = current_conn._connection.connection.dbapi_connection
raw_conn.register('users', arrow_table)
```

Now we can write clean SQL cells.

## Running Queries

### Count Rows

```sql
%%sql
SELECT COUNT(*) as total_users FROM users
```

| total_users |
|-------------|
| 49990 |

### View Schema

```sql
%%sql
SELECT column_name, column_type 
FROM (DESCRIBE SELECT * FROM users)
```

| column_name | column_type |
|-------------|-------------|
| userid | BIGINT |
| username | VARCHAR |
| firstname | VARCHAR |
| lastname | VARCHAR |
| city | VARCHAR |
| state | VARCHAR |
| email | VARCHAR |
| phone | VARCHAR |
| likesports | BOOLEAN |
| liketheatre | BOOLEAN |
| ... | ... |

### Sample Data

```sql
%%sql
SELECT userid, username, firstname, lastname, city, state 
FROM users 
LIMIT 10
```

### Aggregations

```sql
%%sql
SELECT 
    state,
    COUNT(*) as user_count
FROM users
GROUP BY state
ORDER BY user_count DESC
LIMIT 10
```

| state | user_count |
|-------|------------|
| NT | 1998 |
| NB | 1960 |
| BC | 1958 |
| QC | 1929 |
| YT | 1919 |

### Preference Analysis

```sql
%%sql
SELECT 
    'Sports' as preference, COUNT(*) as count FROM users WHERE likesports = true
UNION ALL
SELECT 'Theatre', COUNT(*) FROM users WHERE liketheatre = true
UNION ALL
SELECT 'Concerts', COUNT(*) FROM users WHERE likeconcerts = true
UNION ALL
SELECT 'Jazz', COUNT(*) FROM users WHERE likejazz = true
ORDER BY count DESC
```

### Filtered Queries

```sql
%%sql
SELECT userid, username, firstname, lastname, city, state
FROM users
WHERE likesports = true AND likejazz = true
LIMIT 10
```

## Saving Results to Variables

JupySQL can capture query results into Python variables:

```sql
%%sql result <<
SELECT state, COUNT(*) as count 
FROM users 
GROUP BY state 
ORDER BY count DESC
```

```python
# With autopandas=True, result is already a DataFrame
df = result
df.head(10)
```

## Visualization

Use the captured DataFrame for charts:

```python
import matplotlib.pyplot as plt

top_states = df.head(10)

plt.figure(figsize=(10, 5))
plt.bar(top_states['state'], top_states['count'], color='#0ea5e9')
plt.xlabel('State')
plt.ylabel('User Count')
plt.title('Top 10 States by User Count')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
```

## Exporting Data

DuckDB can export directly to files:

### Export to CSV

```sql
%%sql
COPY (
    SELECT userid, username, firstname, lastname, city, state, email
    FROM users
    WHERE state = 'WA'
) TO 'output/wa_users.csv' (HEADER, DELIMITER ',')
```

### Export to Parquet

```sql
%%sql
COPY (
    SELECT * FROM users
    WHERE likesports = true
) TO 'output/sports_fans.parquet' (FORMAT PARQUET)
```

## Loading Multiple Tables

A helper function makes loading multiple tables easy:

```python
def load_iceberg_table(catalog, namespace, table_name, conn):
    """Load an Iceberg table into DuckDB."""
    table = catalog.load_table(f'{namespace}.{table_name}')
    arrow_table = table.scan().to_arrow()
    conn.register(table_name, arrow_table)
    print(f"✅ Loaded {table_name} ({arrow_table.num_rows:,} rows)")
    return arrow_table

# Load all tables from a namespace
for table_info in catalog.list_tables('ticketdb'):
    load_iceberg_table(catalog, 'ticketdb', table_info[1], raw_conn)
```

Output:
```
✅ Loaded netflix_titles (49,990 rows)
✅ Loaded users (49,990 rows)
```

Now you can join across tables:

```sql
%%sql
SELECT u.username, n.title
FROM users u
JOIN netflix_titles n ON u.userid = n.show_id
LIMIT 10
```

## Why This Pattern Works

**1. PyIceberg handles the catalog complexity**

Table locations, schema, partitioning, snapshots — PyIceberg manages all of it. You just say `load_table('namespace.table')`.

**2. Arrow is the universal interchange format**

Every modern analytics tool speaks Arrow. PyIceberg produces it, DuckDB consumes it, zero conversion overhead.

**3. DuckDB is ridiculously fast**

For analytical queries on in-memory data, DuckDB is hard to beat. It's optimized for exactly this use case.

**4. JupySQL makes notebooks feel like SQL IDEs**

No more wrapping every query in `conn.execute()`. Just write SQL.

## The Complete Stack

| Component | Role | Cost |
|-----------|------|------|
| Nessie on Fly.io | Iceberg catalog | Free tier |
| Cloudflare R2 | Object storage | Free tier (10GB) |
| PyIceberg | Catalog client | Open source |
| DuckDB | Query engine | Open source |
| JupySQL | Notebook SQL magic | Open source |

Total cost: $0

## Lessons Learned

**1. Don't fight SSL issues — find another path**

DuckDB's native Iceberg extension is great, but not with R2. The PyIceberg bridge works reliably.

**2. JupySQL connection handling is tricky**

It creates its own DuckDB connection. You need to access `_connection.connection.dbapi_connection` to register Arrow tables.

**3. DESCRIBE and SHOW TABLES behave differently with Arrow tables**

Standard DuckDB commands don't work on registered Arrow views. Use `DESCRIBE SELECT * FROM table` or `information_schema.tables` instead.

**4. Arrow tables are registered as VIEWs**

When you `conn.register('name', arrow_table)`, DuckDB creates a view, not a physical table. This is efficient but affects some metadata commands.

## Source Code

The complete Jupyter notebook is available on GitHub:

**Repository:** [github.com/senthilsweb/apache-iceberg](https://github.com/senthilsweb/apache-iceberg)

Files:
- `query_iceberg.ipynb` — Complete notebook with all examples
- `requirements.txt` — Python dependencies

---

In Part 1, we built infrastructure. In Part 2, we loaded data. In Part 3, we queried it.

The lakehouse is operational. Next up: time-travel queries and schema evolution.
