#!/usr/bin/env python3
"""
Connectivity and data extraction test.

Usage:
    python etl/test_connection.py
    python etl/test_connection.py --table catalog.schema.table_name
    python etl/test_connection.py --table catalog.schema.table_name --limit 500
    python etl/test_connection.py --table catalog.schema.table_name --output path/to/out.json

Phases:
    0-2  Auth and connectivity (always runs)
    3-5  Data source discovery (always runs)
    6    SQL smoke test (requires DATABRICKS_WAREHOUSE_ID)
    7    Table pull and JSON export (requires --table or DATABRICKS_TABLE, and DATABRICKS_WAREHOUSE_ID)

Table path resolution order:
    1. --table argument
    2. DATABRICKS_TABLE environment variable
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# Allow running from repo root or from etl/
sys.path.insert(0, str(Path(__file__).parent.parent))

from db.auth import get_client, get_config, load_env
from db import queries
from db.queries import rows_to_records

# Load .env before parse_args() so os.getenv() defaults are populated
load_env()


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

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


def skip(reason: str) -> None:
    print(f"  [SKIP] {reason}")


# ---------------------------------------------------------------------------
# JSON serialisation
# ---------------------------------------------------------------------------

def write_json(records: List[Dict[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, default=str)


def default_output_path(table: str) -> Path:
    table_slug = table.replace(".", "_")
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return Path("output") / f"{table_slug}_{timestamp}.json"


# --table/DATABRICKS_TABLE is spliced directly into a SQL statement (step 7) -- restrict it
# to the standard Unity Catalog identifier shape before it ever reaches run_sql().
_TABLE_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+$")


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def _safe_int_env(name: str, default: int) -> int:
    """Read an int env var without crashing before any output is printed.

    A raw int(os.getenv(...)) as an argparse default evaluates at parse_args() call time --
    a non-numeric value (typo, stray unit suffix) would raise ValueError before section 0
    ever prints, killing the whole probe with a bare traceback and zero diagnostic output.
    """
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        print(f"  [WARN] {name}={raw!r} is not a valid integer -- using default {default}.")
        return default


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Test Databricks connectivity and extract table data as JSON."
    )
    parser.add_argument(
        "--table",
        metavar="CATALOG.SCHEMA.TABLE",
        default=os.getenv("DATABRICKS_TABLE", ""),
        help="Fully qualified table path. Overrides DATABRICKS_TABLE env var.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=_safe_int_env("DATABRICKS_LIMIT", 1000),
        help="Max rows to pull (default: 1000).",
    )
    parser.add_argument(
        "--output",
        metavar="FILE",
        default="",
        help="Output file path. Defaults to output/<table>_<timestamp>.json.",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    table: str = args.table.strip()
    limit: int = args.limit
    output_path: Optional[Path] = Path(args.output) if args.output else None

    # ------------------------------------------------------------------ #
    # 0. Resolved auth                                                     #
    # ------------------------------------------------------------------ #
    section("0. Resolved authentication")
    try:
        cfg = get_config()
        print(f"  Host    : {cfg.host}")
        print(f"  Auth    : {cfg.auth_type}")
        client = get_client()
    except Exception as e:
        fail("Could not resolve config", e)
        sys.exit(1)

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
    # 3. Unity Catalog -- catalogs                                         #
    # ------------------------------------------------------------------ #
    section("3. Unity Catalog -- catalogs")
    available_catalogs: List[str] = []
    try:
        available_catalogs = queries.list_catalogs(client)
        for name in available_catalogs:
            ok(name)
    except Exception as e:
        fail("List catalogs", e)

    # ------------------------------------------------------------------ #
    # 4. Schemas in target catalog                                         #
    # ------------------------------------------------------------------ #
    env_catalog = os.getenv("DATABRICKS_CATALOG", "")
    if env_catalog:
        target_catalog = env_catalog
    elif available_catalogs:
        target_catalog = "main" if "main" in available_catalogs else available_catalogs[0]
    else:
        target_catalog = "main"

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
            for t in tables[:20]:
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
        skip("set DATABRICKS_WAREHOUSE_ID in .env to enable")
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
    # 7. Table pull and JSON export                                        #
    # ------------------------------------------------------------------ #
    section("7. Table pull and JSON export")

    if not warehouse_id:
        skip("set DATABRICKS_WAREHOUSE_ID in .env to enable")
    elif not table:
        skip("provide --table CATALOG.SCHEMA.TABLE or set DATABRICKS_TABLE in .env")
    elif not _TABLE_IDENTIFIER_RE.match(table):
        fail("Table pull", ValueError(
            f"{table!r} doesn't look like a catalog.schema.table identifier -- refusing to "
            "interpolate it into a SQL statement"
        ))
    else:
        print(f"  Table   : {table}")
        print(f"  Limit   : {limit}")
        try:
            result = queries.run_sql(
                client,
                statement=f"SELECT * FROM {table} LIMIT {limit}",
                warehouse_id=warehouse_id,
            )
            records = rows_to_records(result["columns"], result["rows"])
            out = output_path or default_output_path(table)
            write_json(records, out)
            ok(f"Wrote {len(records)} records to {out}")
        except Exception as e:
            fail("Table pull", e)

    print("\nDone.\n")


if __name__ == "__main__":
    main()
