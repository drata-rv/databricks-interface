# Databricks Interface

Python interface to the Databricks REST API using the official `databricks-sdk`. Authenticates against a Databricks workspace, browses Unity Catalog metadata, executes SQL against a target warehouse, and exports table data as a JSON payload.

---

## Prerequisites

- Python 3.8+
- A Databricks workspace with Unity Catalog enabled
- A running SQL warehouse (Serverless or Pro tier)
- An account with at least `CAN USE` on the warehouse and `SELECT` on the target catalog/schema

---

## Databricks Setup

### 1. Create a SQL Warehouse

Go to **SQL > SQL Warehouses > Create warehouse**. After creation, open the warehouse and go to the **Connection details** tab to copy the warehouse ID. You will need it in your `.env` file.

### 2. Generate a Personal Access Token

Go to **Settings > Developer > Access tokens > Generate new token**. Set a reasonable expiry and copy the token immediately. This is used for local development. For production deployments use a service principal with OAuth M2M (see step 4).

### 3. Grant permissions

The user or service principal running the scripts needs the following:

- `CAN USE` on the SQL warehouse
- `USE CATALOG` on the target catalog
- `USE SCHEMA` on the target schema
- `SELECT` on the tables to be queried

Set these in **Catalog Explorer** or via SQL:

```sql
GRANT USE CATALOG ON CATALOG <catalog> TO `user@example.com`;
GRANT USE SCHEMA ON SCHEMA <catalog>.<schema> TO `user@example.com`;
GRANT SELECT ON SCHEMA <catalog>.<schema> TO `user@example.com`;
```

### 4. (Production) Create a Service Principal

In the Databricks account console go to **User management > Service principals > Add service principal**. Generate a client secret and grant the principal the same permissions listed above. Use `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`, and `DATABRICKS_AZURE_TENANT_ID` in place of `DATABRICKS_TOKEN` in your `.env` file.

---

## Local Setup

**Mac/Linux:**
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

**Windows:**
```cmd
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

Copy the credential template and fill in your values:

```bash
cp .env.example .env        # Mac/Linux
copy .env.example .env      # Windows
```

Minimum required fields in `.env`:

```
DATABRICKS_HOST=https://your-workspace.azuredatabricks.net
DATABRICKS_TOKEN=dapi...
DATABRICKS_WAREHOUSE_ID=<warehouse-id>
```

Optional fields for discovery and extraction:

```
DATABRICKS_CATALOG=<catalog-name>
DATABRICKS_SCHEMA=<schema-name>
DATABRICKS_TABLE=<catalog.schema.table_name>
DATABRICKS_LIMIT=1000
```

The `.env` file is git-ignored. Never commit credentials.

---

## Running the Script

**Connectivity and discovery only:**
```bash
python scripts/test_connection.py
```

**Pull a specific table and export to JSON:**
```bash
python scripts/test_connection.py --table catalog.schema.table_name
```

**Limit rows and specify output path:**
```bash
python scripts/test_connection.py --table catalog.schema.table_name --limit 500 --output results/export.json
```

The `--table` flag overrides `DATABRICKS_TABLE` in `.env`. If neither is set, step 7 is skipped.

---

## What the Script Does

The script runs 8 steps in sequence. Each step prints `[OK]`, `[FAIL]`, or `[SKIP]`. Steps are independent so a failure in one does not block the rest.

| Step | What it checks | Requires |
|------|---------------|----------|
| 0 | Resolved auth config (host and auth type) | `DATABRICKS_HOST` + token |
| 1 | Workspace root connectivity | Auth |
| 2 | Available SQL warehouses and their state | Auth |
| 3 | Unity Catalog list | Auth |
| 4 | Schemas within the target catalog | Auth |
| 5 | Tables within the target schema | Auth |
| 6 | SQL smoke test (`SELECT 1`) | `DATABRICKS_WAREHOUSE_ID` |
| 7 | Table pull and JSON export | `DATABRICKS_WAREHOUSE_ID` + `--table` |

Step 7 writes output to `output/<catalog>_<schema>_<table>_<timestamp>.json` by default. The `output/` directory is git-ignored.

---

## Project Structure

```
db/
  auth.py            -- WorkspaceClient factory; reads credentials from environment
  queries.py         -- Catalog browsing and SQL execution helpers
scripts/
  test_connection.py -- Connectivity test, data discovery, and JSON export
output/              -- Extracted JSON files (git-ignored)
.env.example         -- Credential and config template
requirements.txt
```

---

## Authentication Reference

The SDK resolves credentials in this order with no code changes required:

1. Environment variables (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`, etc.)
2. Named profile in `~/.databrickscfg` (select via `DATABRICKS_CONFIG_PROFILE`)
3. Cloud-native auth (Azure CLI, AWS IAM, GCP service account)

Set the appropriate environment variables in your CI/CD or orchestration platform to use the same script in deployed contexts.
