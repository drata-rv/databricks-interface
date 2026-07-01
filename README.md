# Databricks Interface

Python interface to the Databricks REST API. Authenticates against two Databricks workspaces, pulls SCCM device, user, and endpoint-protection tables through an extensible table registry, joins them per employee, and produces a JSON payload in the Drata Custom Device Connection format.

---

## Prerequisites

- Python 3.8+
- Two Databricks workspaces (prod and test) with Unity Catalog enabled
- A running SQL warehouse in each workspace (Serverless or Pro tier)
- An account with `CAN USE` on each warehouse and `SELECT` on the target catalogs and schemas
- A personal access token for each workspace (or a shared token if both workspaces accept the same one)

---

## Databricks Setup

### 1. Create SQL Warehouses

In each workspace, go to **SQL > SQL Warehouses > Create warehouse**. After creation, open the warehouse, go to the **Connection details** tab, and copy the warehouse ID.

- Prod warehouse ID goes into `DATABRICKS_WAREHOUSE_ID`
- Test warehouse ID goes into `DATABRICKS_WAREHOUSE_ID_TEST`

### 2. Generate Personal Access Tokens

Go to **Settings > Developer > Access tokens > Generate new token** in each workspace. Set a reasonable expiry and copy the token immediately. The token needs the `sql`, `unity-catalog`, and `workspace` scopes.

If both workspaces accept the same token, set only `DATABRICKS_TOKEN` and leave the workspace-specific vars unset.

### 3. Grant Permissions

The account or service principal running the scripts needs the following in each workspace:

```sql
GRANT CAN USE ON SQL WAREHOUSE <warehouse-id> TO `user@example.com`;
GRANT USE CATALOG ON CATALOG <catalog> TO `user@example.com`;
GRANT USE SCHEMA ON SCHEMA <catalog>.<schema> TO `user@example.com`;
GRANT SELECT ON SCHEMA <catalog>.<schema> TO `user@example.com`;
```

### 4. (Production) Service Principal

In the Databricks account console go to **User management > Service principals > Add service principal**. Generate a client secret and grant it the same permissions above. Use `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`, and `DATABRICKS_AZURE_TENANT_ID` in place of `DATABRICKS_TOKEN`.

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

Copy the credential template:

```bash
cp .env.example .env        # Mac/Linux
copy .env.example .env      # Windows
```

The `.env.example` file is pre-filled with all known workspace URLs, warehouse IDs, and table paths. The only values you need to supply are the tokens.

---

## Configuration

| Variable | Required | Description |
|---|---|---|
| `DATABRICKS_HOST_PROD` | Yes | Prod workspace URL |
| `DATABRICKS_TOKEN_PROD` | Yes* | Token for the prod workspace |
| `DATABRICKS_WAREHOUSE_ID` | Yes | Warehouse ID in the prod workspace |
| `DATABRICKS_HOST_TEST` | Yes | Test workspace URL |
| `DATABRICKS_TOKEN_TEST` | Yes* | Token for the test workspace |
| `DATABRICKS_WAREHOUSE_ID_TEST` | Yes | Warehouse ID in the test workspace |
| `DATABRICKS_TABLE_DEVICES` | Yes | Fully qualified devices table path (prod catalog) |
| `DATABRICKS_TABLE_WINDOWS_UPDATE` | Yes | Fully qualified path to t_sccm_gs_windowsupdate (test catalog) |
| `DATABRICKS_TABLE_INSTALLED_SOFTWARE` | Yes | Fully qualified path to t_sccm_gs_installed_software (test catalog) |
| `DATABRICKS_TABLE_ANTIVIRUS` | No | Path to t_sccm_gs_antivirusproduct -- feeds `antivirusEnabled` (any registered row counts as protected) |
| `DATABRICKS_TABLE_FIREWALL` | No | Path to t_sccm_gs_firewallproduct -- pulled into raw output only, not yet wired to a Drata field |
| `DATABRICKS_TABLE_USERS` | Yes | Fully qualified path to the user identity table (test catalog); not required if `--local-users` is passed |
| `DATABRICKS_LIMIT` | No | Max users to process per run (default: 1000); bypassed by `--full` |
| `DATABRICKS_QUERY_TIMEOUT` | No | Per-query timeout in seconds, covers cold warehouse start (default: 300) |
| `DATABRICKS_TOKEN` | No | Shared token fallback used by both workspaces if workspace-specific vars are not set |
| `DRATA_API_KEY` | No* | Drata public API Bearer token |
| `DRATA_CONNECTION_ID` | No* | UUID of the Custom Device Connection in Drata |
| `DATABRICKS_TABLE_BITLOCKER` | No | Path to BitLocker details table -- enables `encryptionEnabled` |
| `DATABRICKS_TABLE_SCREENSAVER` | No | Path to screensaver settings table -- enables `screenLockEnabled` |
| `DATABRICKS_TABLE_SERVICES` | No | Path to Windows services table -- enables `firewallEnabled`, `windowsServices` |
| `DATABRICKS_TABLE_NETWORK_ADAPTER` | No | Path to network adapter config table -- enables `macAddress` |

*`DRATA_API_KEY` and `DRATA_CONNECTION_ID` are required for the Drata push step. If either is unset, the pipeline writes JSON output but skips the push (equivalent to `--dry-run`).

*If both workspaces share a token, set only `DATABRICKS_TOKEN`.

For `test_connection.py`, which uses the SDK's single-workspace credential chain, set `DATABRICKS_HOST` to the workspace you want to probe (copy from `DATABRICKS_HOST_PROD` or `DATABRICKS_HOST_TEST`) and `DATABRICKS_TOKEN` to the matching token.

---

## Running the Scripts

### Step 1: Verify connectivity

Run this first to confirm auth, warehouse access, and table visibility before running the full ETL:

```bash
python scripts/test_connection.py
```

To pull a sample from a specific table and inspect the raw data:

```bash
python scripts/test_connection.py --table catalog.schema.table_name --limit 10
```

### Step 2: Run the ETL

```bash
python scripts/extract_devices.py
```

Up to three output files are written per run:

- `output/devices_<timestamp>_raw.json` -- merged SCCM data exactly as pulled
- `output/devices_<timestamp>_drata.json` -- transformed into Drata Custom Device Connection format
- `output/devices_<timestamp>_rejected.json` -- records excluded from the push (missing personnelId, empty appList, or missing externalId), each tagged with a `rejection_reason`; only written if any records were excluded

Useful flags:

| Flag | Effect |
|---|---|
| `--local-users` | Load users from the local xlsx instead of `DATABRICKS_TABLE_USERS` (sandbox testing only) |
| `--full` | Bypass `--limit` and process every user -- production sync |
| `--test-mode` | Force all 5 monitoring fields to a passing state while keeping real identities |
| `--sandbox` | Rewrite `personnelId` from `@nationwide.com` to `@sandbox.nationwide.com` before pushing |
| `--dry-run` | Run the full pipeline and write output files, but skip the Drata push |
| `--debug` | Print the full resolved environment before running |

A typical sandbox test run combines several of these:

```bash
python scripts/extract_devices.py --local-users --test-mode --full --sandbox
```

### Step 3: Push to Drata

Set `DRATA_API_KEY` and `DRATA_CONNECTION_ID` in `.env`, then run without `--dry-run`. The script pushes all records automatically after writing the output files.

To push a small batch first and verify records appear in Drata before a full run:

```bash
python scripts/extract_devices.py --limit 5
```

---

## What the ETL Does

1. Loads users first, from `DATABRICKS_TABLE_USERS` or the local xlsx via `--local-users` -- users anchor everything downstream
2. Filters users against Drata personnel status, keeping only current employees and contractors
3. Processes users in chunks of 500: pulls devices scoped to that chunk (excluding servers, VMs, and decommissioned/inactive machines), then pulls secondary tables (Windows Update, installed software, antivirus, firewall) via `TABLE_REGISTRY`
4. Merges using users as the anchor (inner join): only devices with a matched user record are included; unmatched devices are counted and logged
5. Extracts the Drata monitoring signals from the merged data (antivirus, auto-update, password manager; encryption and screen lock remain null pending additional SCCM tables)
6. Formats each merged record into the Drata Custom Device Connection JSON shape
7. Applies pre-push quality gates -- records missing a personnelId, appList, or externalId are excluded and written to `_rejected.json` instead of being pushed
8. Writes all output files
9. Pushes to the Drata API if `DRATA_API_KEY` and `DRATA_CONNECTION_ID` are set

The `output/` directory is git-ignored. Each run produces a new timestamped set of files.

### Adding a new SCCM table

1. Set the matching `DATABRICKS_TABLE_*` env var in `.env`
2. Uncomment the corresponding `TableSpec` line in `TABLE_REGISTRY` in [`scripts/extract_devices.py`](scripts/extract_devices.py)

No other code changes are needed. The new table is automatically pulled with an IN-clause filter and passed to the merge and feature-extraction stages.

---

## test_connection.py Steps

| Step | What it checks | Requires |
|---|---|---|
| 0 | Resolved auth config (host and auth type) | `DATABRICKS_HOST` + token |
| 1 | Workspace root connectivity | Auth |
| 2 | Available SQL warehouses and their state | Auth |
| 3 | Unity Catalog list | Auth |
| 4 | Schemas within the target catalog | Auth |
| 5 | Tables within the target schema | Auth |
| 6 | SQL smoke test (`SELECT 1`) | `DATABRICKS_WAREHOUSE_ID` |
| 7 | Table pull and JSON export | `DATABRICKS_WAREHOUSE_ID` + `--table` |

Each step prints `[OK]`, `[FAIL]`, or `[SKIP]`. Steps are independent -- a failure in one does not block the rest.

---

## Project Structure

```
db/
  auth.py            -- WorkspaceClient factory; reads credentials from environment
  queries.py         -- Catalog browsing, SQL execution, result helpers
  transform.py       -- Feature extraction (extract_features) and Drata format assembly (format_for_drata)
  drata_client.py    -- Drata Custom Device Connection API client
scripts/
  test_connection.py -- Connectivity probe and single-table export
  extract_devices.py -- ETL: pull via registry, user-centric merge, transform, write, push
output/              -- Extracted JSON files (git-ignored)
.env.example         -- Credential and config template (pre-filled)
requirements.txt
```

---

## Authentication Reference

The SDK resolves credentials in this order with no code changes required:

1. Environment variables (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`, etc.)
2. Named profile in `~/.databrickscfg` (select via `DATABRICKS_CONFIG_PROFILE`)
3. Cloud-native auth (Azure CLI, AWS IAM, GCP service account)

Set the appropriate environment variables in your CI/CD or orchestration platform to use the same scripts in deployed contexts.
