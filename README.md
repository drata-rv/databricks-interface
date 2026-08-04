# Databricks Interface

Python interface to the Databricks REST API. Authenticates against two Databricks workspaces, pulls SCCM device, user, and endpoint-protection tables through an extensible table registry, joins them per employee, and produces a JSON payload in the Drata Custom Device Connection format.

This project runs both as a local CLI and, once packaged, as a native Databricks Job (`python_wheel_task`) -- see [Running on Databricks](#running-on-databricks-native-job) below.

---

## Prerequisites

- Python 3.8+
- Two Databricks workspaces (prod and test) with Unity Catalog enabled
- A running SQL warehouse in each workspace (Serverless or Pro tier)
- An account with `CAN USE` on each warehouse and `SELECT` on the target catalogs and schemas
- A personal access token for each workspace, or OAuth service-principal credentials if preferred (see step 4 below) -- a shared token also works if both workspaces accept the same one

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

### 4. Service Principal / OAuth M2M

Nationwide's security policy prefers OAuth over a personal access token (confirmed by Terry Hardaway, 2026-07-13). In the Databricks account console go to **User management > Service principals > Add service principal**, generate a client secret, and grant it the same permissions above. Databricks service-principal OAuth M2M tokens only support one scope -- `all-apis` -- there is no finer-grained scope to request.

Set `DATABRICKS_CLIENT_ID_PROD`/`DATABRICKS_CLIENT_SECRET_PROD` and `DATABRICKS_CLIENT_ID_TEST`/`DATABRICKS_CLIENT_SECRET_TEST` per workspace. `get_client_for_env()` in `db/auth.py` prefers these over the matching `DATABRICKS_TOKEN_*` automatically when both are set -- no flag or code change needed to switch. Leave them unset to keep using a PAT (e.g. local dev without a provisioned service principal).

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

The `.env.example` file is pre-filled with all known workspace URLs, warehouse IDs, and table paths. The only values you need to supply are your own credentials (tokens, or OAuth client ID/secret if you have them).

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
| `DATABRICKS_TABLE_BITLOCKER` | No | Path to t_sccm_gs_encryptable_volume -- enables `encryptionEnabled` (`protection_status0`) |
| `DATABRICKS_TABLE_COMPUTER_SYSTEM` | No | Path to t_sccm_gs_computer_system -- enables a real hardware `model` (`model0`); falls back to CPU type if absent |
| `DATABRICKS_TABLE_SCREENSAVER` | No | Path to screensaver settings table -- enables `screenLockEnabled` |
| `DATABRICKS_TABLE_SERVICES` | No | Path to Windows services table -- enables `firewallEnabled`, `windowsServices` |
| `DATABRICKS_TABLE_NETWORK_ADAPTER` | No | Path to network adapter config table -- enables `macAddress` |
| `DATABRICKS_CLIENT_ID_PROD` / `_TEST` | No | Service-principal application ID; takes priority over the matching `DATABRICKS_TOKEN_*` when both `CLIENT_ID` and `CLIENT_SECRET` are set |
| `DATABRICKS_CLIENT_SECRET_PROD` / `_TEST` | No | Service-principal OAuth secret (paired with `CLIENT_ID` above) |
| `DATABRICKS_SECRET_SCOPE_PROD` / `_TEST` | No | Databricks secret scope name per workspace; only consulted inside a Databricks Job (see `db/secrets.py`) |

*`DRATA_API_KEY` and `DRATA_CONNECTION_ID` are required for the Drata push step. If either is unset, the pipeline writes JSON output but skips the push (equivalent to `--dry-run`).

*If both workspaces share a token, set only `DATABRICKS_TOKEN`.

For `test_connection.py`, which uses the SDK's single-workspace credential chain, set `DATABRICKS_HOST` to the workspace you want to probe (copy from `DATABRICKS_HOST_PROD` or `DATABRICKS_HOST_TEST`) and `DATABRICKS_TOKEN` to the matching token.

---

## Running the Scripts

### Step 1: Verify connectivity

Run this first to confirm auth, warehouse access, and table visibility before running the full ETL:

```bash
python etl/test_connection.py
```

To pull a sample from a specific table and inspect the raw data:

```bash
python etl/test_connection.py --table catalog.schema.table_name --limit 10
```

### Step 2: Run the ETL

```bash
python etl/extract_devices.py
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
python etl/extract_devices.py --local-users --test-mode --full --sandbox
```

### Step 3: Push to Drata

Set `DRATA_API_KEY` and `DRATA_CONNECTION_ID` in `.env`, then run without `--dry-run`. The script pushes all records automatically after writing the output files.

To push a small batch first and verify records appear in Drata before a full run:

```bash
python etl/extract_devices.py --limit 5
```

---

## Running on Databricks (native Job)

The same `db/`/`etl/` code is packaged as a wheel (`pyproject.toml`) and deployed as a `python_wheel_task` inside a Databricks Asset Bundle (`databricks.yml`) -- the job's compute runs inside Databricks (serverless, single-node), not merely triggered externally while running elsewhere. There is no Spark distribution in this job; all data movement is via the SQL Statement Execution API and the Drata push is driver-side, exactly as it is locally.

Credentials are resolved automatically depending on where the code runs, via `db/secrets.py::get_secret()` and `db/auth.py::get_client_for_env()`:
- Inside a Databricks Job (detected via the `DATABRICKS_RUNTIME_VERSION` environment variable Databricks sets on all clusters and serverless compute): credentials come from a Databricks secret scope -- one per workspace (`DATABRICKS_SECRET_SCOPE_PROD`/`_TEST`), since Nationwide provisions them separately rather than sharing one scope.
- Locally: the existing `.env` behavior is completely unchanged.

Within either environment, OAuth M2M (service-principal `client_id`/`client_secret`) is preferred over a personal access token whenever both are present, falling back to PAT otherwise -- see [Service Principal / OAuth M2M](#4-service-principal--oauth-m2m) above.

Nothing else in the codebase needs to know which environment it's running in -- this is the only place that branches on it.

### Deploying

`databricks bundle deploy` builds the wheel via `python -m build --wheel` (see the `artifacts` block in `databricks.yml`), so the `build` package must be installed first -- it's included in the `dev` extra:

```bash
pip install -e ".[dev]"
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run extract_devices_job -t dev
```

Validated live against a real Databricks CLI and the `nationwide-irm-test-ohio` workspace on 2026-07-17 -- `bundle validate`/`deploy` both succeed cleanly aside from one known warning (`alert_on_last_attempt` misplaced in the task's `email_notifications` block -- deferred, doesn't block deploy). Triggering an actual run still needs the real secret scope names from Nationwide (see below).

### Known gaps in this skeleton

- Task-level environment variables (table paths, `DATABRICKS_SECRET_SCOPE_PROD`/`_TEST`, etc.) are not yet wired into the running process -- `databricks.yml` declares them but the job spec doesn't consume them. The correct mechanism for a `python_wheel_task` on serverless compute (an `environments.spec` env key, or a different passthrough) still needs to be confirmed against a real CLI.
- Only `--limit` is currently forwarded to the wheel task's entry point (see `python_wheel_task.parameters` in `databricks.yml`). `--full`, `--sandbox`, `--test-mode`, and `--dry-run` are declared as job parameters (table below) but not yet passed through -- these are `store_true` flags with no value, so forwarding them needs either an argparse change (accept an explicit `true`/`false`) or a different plumbing approach. Not yet decided.

**Serverless environment caching (resolved 2026-07-27):** Databricks caches the built environment by dependency spec and will silently keep running an old wheel if the version string is unchanged between deploys -- this caused an identical library-install failure to persist across multiple substantive code changes before it was traced back to `pyproject.toml`'s version never being bumped. `databricks.yml`'s artifact block now sets `dynamic_version: true` (requires Databricks CLI >=0.245.0) so every build gets a unique version automatically -- no manual bump needed going forward.

### CLI flags -> job parameters

| CLI flag | Job parameter | Wired today? |
|---|---|---|
| `--limit` | `limit` | Yes |
| `--full` | `full` | No -- see Known gaps above |
| `--sandbox` | `sandbox` | No -- see Known gaps above |
| `--test-mode` | `test_mode` | No -- see Known gaps above |
| `--dry-run` | `dry_run` | No -- see Known gaps above |

Table paths, warehouse IDs, and credentials are **not** job parameters by design -- they stay env/secret-driven, so there's one configuration surface instead of two, and credentials never flow through job parameters or notebook widgets.

### Prerequisites before deploying to `prod`

None of the following block writing or merging code -- they block only the `prod` target deploy. Terry Hardaway (Nationwide) replied 2026-07-13:

- Tables will be available in `nationwide-irm-prod-ohio` once prod switches over -- confirmed, no further action needed.
- Egress to `public-api.drata.com` is clear -- confirmed, no further action needed.
- **Still open**: the actual secret scope names for the Test and Production scopes Terry confirmed were created, and a named owner/application ID for the OAuth service-principal identity. `databricks.yml` marks the corresponding placeholders with a `CHANGE_ME_` prefix.

---

## What the ETL Does

1. Loads users first, from `DATABRICKS_TABLE_USERS` or the local xlsx via `--local-users` -- users anchor everything downstream
2. Filters users against Drata personnel status, keeping only current employees and contractors
3. Processes users in chunks of 500: pulls devices scoped to that chunk (excluding servers, VMs, and decommissioned/inactive machines), then pulls secondary tables (Windows Update, installed software, antivirus, firewall, encrypted volume, computer system) via `TABLE_REGISTRY`
4. Merges using users as the anchor (inner join): only devices with a matched user record are included; unmatched devices are counted and logged
5. Extracts the Drata monitoring signals from the merged data (antivirus, auto-update, password manager, encryption; screen lock remains null pending an additional SCCM table)
6. Formats each merged record into the Drata Custom Device Connection JSON shape
7. Applies pre-push quality gates -- records missing a personnelId, appList, or externalId are excluded and written to `_rejected.json` instead of being pushed
8. Writes all output files
9. Pushes to the Drata API if `DRATA_API_KEY` and `DRATA_CONNECTION_ID` are set

The `output/` directory is git-ignored. Each run produces a new timestamped set of files.

### Adding a new SCCM table

1. Set the matching `DATABRICKS_TABLE_*` env var in `.env`
2. Uncomment the corresponding `TableSpec` line in `TABLE_REGISTRY` in [`etl/extract_devices.py`](etl/extract_devices.py)

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
  __init__.py
  auth.py            -- WorkspaceClient factory; get_client_for_env() resolves credentials via db/secrets.py
  secrets.py         -- Credential resolution: .env locally, Databricks secret scope inside a Job
  queries.py         -- Catalog browsing, SQL execution, result helpers
  transform.py       -- Feature extraction (extract_features) and Drata format assembly (format_for_drata)
  drata_client.py    -- Drata Custom Device Connection API client
etl/
  __init__.py
  test_connection.py -- Connectivity probe and single-table export
  extract_devices.py -- ETL: pull via registry, user-centric merge, transform, write, push
tests/               -- pytest suite; mocks WorkspaceClient, no network access required
output/              -- Extracted JSON files (git-ignored)
.env.example         -- Credential and config template (pre-filled)
pyproject.toml       -- Package definition + extract-devices console script entry point
databricks.yml       -- Databricks Asset Bundle (native Job deployment)
requirements.txt
```

---

## Authentication Reference

The SDK resolves credentials in this order with no code changes required:

1. Environment variables (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`, etc.)
2. Named profile in `~/.databrickscfg` (select via `DATABRICKS_CONFIG_PROFILE`)
3. Cloud-native auth (Azure CLI, AWS IAM, GCP service account)

Set the appropriate environment variables in your CI/CD or orchestration platform to use the same scripts in deployed contexts.

`get_client_for_env()` (used by the ETL pipeline, not `test_connection.py`) layers its own resolution on top: when running inside a Databricks Job, credentials are additionally resolvable via `dbutils.secrets.get()` against a per-workspace scope (`DATABRICKS_SECRET_SCOPE_PROD`/`_TEST`) -- see `db/secrets.py`. Within that resolution, OAuth M2M (`databricks-client-id-{workspace}` + `databricks-client-secret-{workspace}`) takes priority over a PAT (`databricks-token-{workspace}`) whenever both are present.
