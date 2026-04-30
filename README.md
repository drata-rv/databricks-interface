# Databricks Interface

Python interface to the Databricks REST API using the official `databricks-sdk`. Provides authenticated access to Unity Catalog metadata and SQL query execution against a target warehouse.

---

## Prerequisites

- Python 3.8+
- A Databricks workspace with Unity Catalog enabled
- A SQL warehouse (Serverless or Pro tier)
- An account with at least `CAN USE` on the warehouse and `SELECT` on the target catalog/schema

---

## Databricks Setup

### 1. Create a SQL Warehouse

In your workspace go to **SQL > SQL Warehouses > Create warehouse**. Note the warehouse ID from the warehouse settings URL or the connection details tab. You will need it for the `.env` file.

### 2. Generate a Personal Access Token

Go to **Settings > Developer > Access tokens > Generate new token**. Set a reasonable expiry and copy the token immediately. This is used for local development. For production deployments use a service principal with OAuth M2M (see below).

### 3. Grant permissions

Ensure the user or service principal running the scripts has:

- `CAN USE` on the SQL warehouse
- `USE CATALOG` on the target catalog
- `USE SCHEMA` on the target schema
- `SELECT` on any tables to be queried

These can be set in **Catalog Explorer** or via SQL:

```sql
GRANT USE CATALOG ON CATALOG <catalog> TO `user@example.com`;
GRANT USE SCHEMA ON SCHEMA <catalog>.<schema> TO `user@example.com`;
GRANT SELECT ON SCHEMA <catalog>.<schema> TO `user@example.com`;
```

### 4. (Production) Create a Service Principal

In the Databricks account console go to **User management > Service principals > Add service principal**. Generate a client secret, then grant the principal the same permissions as above. Supply `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`, and `DATABRICKS_AZURE_TENANT_ID` in place of `DATABRICKS_TOKEN`.

---

## Local Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Copy the credential template and fill in your values:

```bash
cp .env.example .env
```

Edit `.env`:

```
DATABRICKS_HOST=https://your-workspace.azuredatabricks.net
DATABRICKS_TOKEN=dapi...

DATABRICKS_WAREHOUSE_ID=<warehouse-id>
DATABRICKS_CATALOG=<catalog-name>
DATABRICKS_SCHEMA=<schema-name>
```

The `.env` file is git-ignored. Never commit credentials.

---

## Running the Connection Test

```bash
python scripts/test_connection.py
```

The script runs seven probes in sequence:

| Step | What it checks |
|------|----------------|
| 0 | Resolved auth config (host and auth type) |
| 1 | Workspace root connectivity |
| 2 | Available SQL warehouses and their state |
| 3 | Unity Catalog catalog list |
| 4 | Schemas within the target catalog |
| 5 | Tables within the target schema |
| 6 | SQL smoke test (`SELECT 1`) |
| 7 | Sample `SELECT *` from the target schema (first 5 rows) |

Steps 6 and 7 require `DATABRICKS_WAREHOUSE_ID` to be set. All other steps run on catalog metadata alone.

Each step prints `[OK]` on success or `[FAIL]` with the error message on failure. Steps are independent so a failure in one does not block the rest.

---

## Project Structure

```
db/
  auth.py       -- WorkspaceClient factory; reads credentials from environment
  queries.py    -- Catalog browsing and SQL execution helpers
scripts/
  test_connection.py  -- Local connectivity and query verification
.env.example    -- Credential template
requirements.txt
```

---

## Authentication Reference

The SDK resolves credentials in this order without any code changes required:

1. Environment variables (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`, etc.)
2. Named profile in `~/.databrickscfg` (select via `DATABRICKS_CONFIG_PROFILE`)
3. Cloud-native auth (Azure CLI, AWS IAM, GCP service account)

Set environment variables in your CI/CD or orchestration platform to use the same code in deployed contexts.
