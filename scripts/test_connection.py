#!/usr/bin/env python3
"""
Local connectivity test script.

Run:
    python scripts/test_connection.py

Reads credentials from .env (or ~/.databrickscfg / env vars).
Set DATABRICKS_WAREHOUSE_ID, DATABRICKS_CATALOG, DATABRICKS_SCHEMA
in .env to run the SQL probes against your target database.
"""

import os
import sys
from pathlib import Path
from typing import Any

# Allow running from repo root or from scripts/
sys.path.insert(0, str(Path(__file__).parent.parent))

from db.auth import get_client, get_config
from db import queries


def section(title: str) -> None:
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


def ok(label: str, value: Any = None) -> None:
    msg = f"  [OK] {label}"
    if value is not None:
        msg += f": {value}"
    print(msg)


def fail(label: str, err: Exception) -> None:
    print(f"  [FAIL] {label}: {err}")


def main() -> None:
    # ------------------------------------------------------------------ #
    # 0. Show resolved auth config                                         #
    # ------------------------------------------------------------------ #
    section("0. Resolved authentication")
    try:
        cfg = get_config()
        print(f"  Host    : {cfg.host}")
        print(f"  Auth    : {cfg.auth_type}")
    except Exception as e:
        fail("Could not resolve config", e)
        sys.exit(1)

    client = get_client()

    # ------------------------------------------------------------------ #
    # 1. Workspace connectivity                                            #
    # ------------------------------------------------------------------ #
    section("1. Workspace connectivity")
    try:
        info = queries.check_workspace(client)
        ok("Workspace root", info)
    except Exception as e:
        fail("Workspace check", e)

    # ------------------------------------------------------------------ #
    # 2. SQL warehouses                                                    #
    # ------------------------------------------------------------------ #
    section("2. SQL warehouses")
    try:
        warehouses = queries.list_sql_warehouses(client)
        if warehouses:
            for wh in warehouses:
                ok(wh["name"], f"id={wh['id']}  state={wh['state']}")
        else:
            print("  (no warehouses found)")
    except Exception as e:
        fail("List warehouses", e)

    # ------------------------------------------------------------------ #
    # 3. Unity Catalog — catalogs                                         #
    # ------------------------------------------------------------------ #
    section("3. Unity Catalog — catalogs")
    try:
        catalogs = queries.list_catalogs(client)
        for name in catalogs:
            ok(name)
    except Exception as e:
        fail("List catalogs", e)

    # ------------------------------------------------------------------ #
    # 4. Schemas in target catalog                                         #
    # ------------------------------------------------------------------ #
    target_catalog = os.getenv("DATABRICKS_CATALOG", "hive_metastore")
    section(f"4. Schemas in '{target_catalog}'")
    try:
        schemas = queries.list_schemas(client, target_catalog)
        for name in schemas:
            ok(name)
    except Exception as e:
        fail(f"List schemas in {target_catalog}", e)

    # ------------------------------------------------------------------ #
    # 5. Tables in target schema                                           #
    # ------------------------------------------------------------------ #
    target_schema = os.getenv("DATABRICKS_SCHEMA", "default")
    section(f"5. Tables in '{target_catalog}.{target_schema}'")
    try:
        tables = queries.list_tables(client, target_catalog, target_schema)
        if tables:
            for t in tables[:20]:  # cap output
                ok(t["name"], f"type={t['table_type']}")
            if len(tables) > 20:
                print(f"  ... and {len(tables) - 20} more")
        else:
            print("  (no tables found)")
    except Exception as e:
        fail(f"List tables in {target_catalog}.{target_schema}", e)

    # ------------------------------------------------------------------ #
    # 6. SQL smoke test                                                    #
    # ------------------------------------------------------------------ #
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID", "")
    section("6. SQL smoke test (SELECT 1)")
    if not warehouse_id:
        print("  Skipped — set DATABRICKS_WAREHOUSE_ID in .env to enable")
    else:
        try:
            result = queries.run_sql(
                client,
                statement="SELECT 1 AS ping",
                warehouse_id=warehouse_id,
            )
            ok("Query succeeded", result)
        except Exception as e:
            fail("SQL smoke test", e)

    # ------------------------------------------------------------------ #
    # 7. Sample table query (optional)                                     #
    # ------------------------------------------------------------------ #
    section(f"7. Sample data from '{target_catalog}.{target_schema}'")
    if not warehouse_id:
        print("  Skipped — set DATABRICKS_WAREHOUSE_ID in .env to enable")
    else:
        try:
            sample_sql = (
                f"SELECT * FROM {target_catalog}.{target_schema} LIMIT 5"
            )
            result = queries.run_sql(
                client,
                statement=sample_sql,
                warehouse_id=warehouse_id,
                catalog=target_catalog,
                schema=target_schema,
            )
            print(f"  Columns : {result['columns']}")
            for row in result["rows"]:
                print(f"  Row     : {row}")
        except Exception as e:
            fail("Sample data query", e)

    print("\nDone.\n")


if __name__ == "__main__":
    main()
