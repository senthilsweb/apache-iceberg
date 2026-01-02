# Fly.io Secrets Configuration

This project requires secrets to be set via `fly secrets set`.

**Do not commit secrets to the repository.**

## Required Secrets

```bash
fly secrets set \
  QUARKUS_DATASOURCE_JDBC_URL="jdbc:postgresql://<host>:5432/<db>?sslmode=require" \
  QUARKUS_DATASOURCE_USERNAME="<username>" \
  QUARKUS_DATASOURCE_PASSWORD="<password>" \
  NESSIE_CATALOG_WAREHOUSES_WAREHOUSE_LOCATION="s3://<bucket>/warehouse" \
  NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_ENDPOINT="https://<account>.r2.cloudflarestorage.com" \
  NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_ACCESS_KEY_NAME="<access-key>" \
  NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_ACCESS_KEY_SECRET="<secret-key>"
```

## Secrets Reference

| Secret | Description |
|--------|-------------|
| `QUARKUS_DATASOURCE_JDBC_URL` | PostgreSQL connection string (Neon) |
| `QUARKUS_DATASOURCE_USERNAME` | Database username |
| `QUARKUS_DATASOURCE_PASSWORD` | Database password |
| `NESSIE_CATALOG_WAREHOUSES_WAREHOUSE_LOCATION` | S3 warehouse path |
| `NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_ENDPOINT` | S3-compatible endpoint |
| `NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_ACCESS_KEY_NAME` | S3 access key |
| `NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_ACCESS_KEY_SECRET` | S3 secret key |
