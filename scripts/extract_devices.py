#!/usr/bin/env python3
"""
Device ETL: pulls four SCCM tables from Databricks, joins them on resource_id
and Netbios_Name0, and writes a single JSON file ready for Drata Custom Device Connection.

Output structure per device:
    {
        "resource_id": 12345,
        "device":             { ...fields from the main device table... },
        "windows_update":     { ...fields from t_sccm_gs_windowsupdate... },
        "installed_software": [ ...one entry per row from t_sccm_gs_installed_software... ],
        "user":               { ...fields from the user identity table... }
    }

Usage:
    python scripts/extract_devices.py \\
        --devices    catalog.schema.t_sccm_v_r_system \\
        --wu         catalog.schema.t_sccm_gs_windowsupdate \\
        --software   catalog.schema.t_sccm_gs_installed_software \\
        --users      catalog.schema.t_sccm_user_table

Table paths and warehouse IDs can also be set via environment variables:
    DATABRICKS_TABLE_DEVICES
    DATABRICKS_TABLE_WINDOWS_UPDATE
    DATABRICKS_TABLE_INSTALLED_SOFTWARE
    DATABRICKS_TABLE_USERS
    DATABRICKS_WAREHOUSE_ID          -- used for the devices table (prod)
    DATABRICKS_WAREHOUSE_ID_TEST     -- used for all test catalog tables (wu, software, users)
                                        Falls back to DATABRICKS_WAREHOUSE_ID if not set.

All four tables are required. Script exits with a non-zero code if any pull fails.
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.auth import get_client, get_client_for, get_config, load_env
from db import queries
from db.transform import transform_all

# Load .env before parse_args() so os.getenv() defaults are populated
load_env()


# ---------------------------------------------------------------------------
# Internal / pipeline metadata columns -- excluded from the output payload
# ---------------------------------------------------------------------------
STRIP_PREFIXES = ("__",)


def is_internal(col: str) -> bool:
    return any(col.startswith(p) for p in STRIP_PREFIXES)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def rows_to_records(columns: List[str], rows: List[List[Any]]) -> List[Dict[str, Any]]:
    return [dict(zip(columns, row)) for row in rows]


def clean(record: Dict[str, Any]) -> Dict[str, Any]:
    """Strip internal pipeline columns from a record."""
    return {k: v for k, v in record.items() if not is_internal(k)}


def get_resource_id(record: Dict[str, Any]) -> Optional[int]:
    """Resolve resource_id regardless of column name casing."""
    for key in ("resource_id", "ResourceID", "RESOURCEID"):
        if key in record:
            val = record[key]
            try:
                return int(val) if val is not None else None
            except (ValueError, TypeError):
                return None
    return None


def _ids_filter(ids: List[int], column: str = "resource_id") -> str:
    """Build a SQL IN filter for integer IDs. Returns '1=0' if the list is empty."""
    if not ids:
        return "1=0"
    return f"{column} IN ({', '.join(str(int(i)) for i in ids)})"


def _names_filter(names: List[str], column: str = "Netbios_Name0") -> str:
    """Build a SQL IN filter for string names with single-quote escaping."""
    if not names:
        return "1=0"
    escaped = ", ".join("'" + n.replace("'", "''") + "'" for n in names)
    return f"{column} IN ({escaped})"


def pull_table(
    client: Any,
    table: str,
    warehouse_id: str,
    label: str,
    limit: Optional[int] = None,
    filter_sql: Optional[str] = None,
    timeout: int = 300,
) -> List[Dict[str, Any]]:
    """Pull a table and return cleaned records. Exits on failure.

    Secondary tables (wu, software, users) are queried with an IN-clause filter
    scoped to the device set -- no LIMIT applied to those pulls.
    """
    parts = [f"SELECT * FROM {table}"]
    if filter_sql:
        parts.append(f"WHERE {filter_sql}")
    if limit is not None:
        parts.append(f"LIMIT {limit}")
    statement = " ".join(parts)
    print(f"  Pulling {label} ({table}) ...")
    try:
        result = queries.run_sql(
            client,
            statement=statement,
            warehouse_id=warehouse_id,
            timeout_seconds=timeout,
        )
        records = rows_to_records(result["columns"], result["rows"])
        print(f"  {len(records)} rows retrieved.")
        return [clean(r) for r in records]
    except Exception as e:
        raw = str(e)
        short = raw.split(". Config:")[0].split(". Env:")[0].strip()
        print(f"  [FAIL] {label}")
        print(f"         Table     : {table}")
        print(f"         Warehouse : {warehouse_id}")
        print(f"         Error     : {short}")
        sys.exit(1)


def merge(
    devices: List[Dict[str, Any]],
    windows_update: List[Dict[str, Any]],
    installed_software: List[Dict[str, Any]],
    users: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Left-join all secondary tables onto devices.

    - windows_update and installed_software join on resource_id.
    - users joins on Netbios_Name0 (machine name present in both tables).

    Devices with no match in a secondary table still appear; their key is {} or [].
    """
    wu_index: Dict[int, Dict[str, Any]] = {}
    for row in windows_update:
        rid = get_resource_id(row)
        if rid is not None:
            wu_index[rid] = {k: v for k, v in row.items() if k not in ("resource_id", "ResourceID")}

    sw_index: Dict[int, List[Dict[str, Any]]] = {}
    for row in installed_software:
        rid = get_resource_id(row)
        if rid is not None:
            entry = {k: v for k, v in row.items() if k not in ("resource_id", "ResourceID")}
            sw_index.setdefault(rid, []).append(entry)

    # User table: keyed by Netbios_Name0 -- one record per machine
    user_index: Dict[str, Dict[str, Any]] = {}
    for row in users:
        netbios = row.get('Netbios_Name0') or row.get('netbios_name0')
        if netbios:
            user_index[netbios] = {k: v for k, v in row.items() if k not in ('Netbios_Name0', 'netbios_name0')}

    output = []
    for device in devices:
        rid = get_resource_id(device)
        # Netbios_Name0 is the join key between the device and user tables
        netbios = device.get('Netbios_Name0') or device.get('Name0')
        device_fields = {k: v for k, v in device.items() if k not in ("resource_id", "ResourceID", "ResourceType")}
        output.append({
            "resource_id": rid,
            "device": device_fields,
            "windows_update": wu_index.get(rid, {}),
            "installed_software": sw_index.get(rid, []),
            "user": user_index.get(netbios, {}),
        })

    return output


def write_json(payload: List[Dict[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)


def default_output_paths() -> Tuple[Path, Path]:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return (
        Path("output") / f"devices_{timestamp}_raw.json",
        Path("output") / f"devices_{timestamp}_drata.json",
    )


# ---------------------------------------------------------------------------
# Args
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pull and join SCCM device tables from Databricks into a single JSON payload."
    )
    parser.add_argument(
        "--devices",
        metavar="CATALOG.SCHEMA.TABLE",
        default=os.getenv("DATABRICKS_TABLE_DEVICES", ""),
        help="Fully qualified path to the main device table.",
    )
    parser.add_argument(
        "--wu",
        metavar="CATALOG.SCHEMA.TABLE",
        default=os.getenv("DATABRICKS_TABLE_WINDOWS_UPDATE", ""),
        help="Fully qualified path to t_sccm_gs_windowsupdate.",
    )
    parser.add_argument(
        "--software",
        metavar="CATALOG.SCHEMA.TABLE",
        default=os.getenv("DATABRICKS_TABLE_INSTALLED_SOFTWARE", ""),
        help="Fully qualified path to t_sccm_gs_installed_software.",
    )
    parser.add_argument(
        "--users",
        metavar="CATALOG.SCHEMA.TABLE",
        default=os.getenv("DATABRICKS_TABLE_USERS", ""),
        help="Fully qualified path to the user identity table (si_test_catalog). Uses DATABRICKS_TABLE_USERS.",
    )
    parser.add_argument(
        "--warehouse-prod",
        metavar="WAREHOUSE_ID",
        default=os.getenv("DATABRICKS_WAREHOUSE_ID", ""),
        help="Warehouse ID for the prod devices table. Uses DATABRICKS_WAREHOUSE_ID.",
    )
    parser.add_argument(
        "--warehouse-test",
        metavar="WAREHOUSE_ID",
        default=os.getenv("DATABRICKS_WAREHOUSE_ID_TEST", ""),
        help="Warehouse ID for the test catalog tables. Uses DATABRICKS_WAREHOUSE_ID_TEST.",
    )
    parser.add_argument(
        "--host-prod",
        metavar="URL",
        default=os.getenv("DATABRICKS_HOST_PROD", ""),
        help="Prod workspace URL. Uses DATABRICKS_HOST_PROD.",
    )
    parser.add_argument(
        "--host-test",
        metavar="URL",
        default=os.getenv("DATABRICKS_HOST_TEST", ""),
        help="Test workspace URL. Uses DATABRICKS_HOST_TEST.",
    )
    parser.add_argument(
        "--token-prod",
        metavar="TOKEN",
        default=os.getenv("DATABRICKS_TOKEN_PROD", "") or os.getenv("DATABRICKS_TOKEN", ""),
        help="Token for the prod workspace. Uses DATABRICKS_TOKEN_PROD, falls back to DATABRICKS_TOKEN.",
    )
    parser.add_argument(
        "--token-test",
        metavar="TOKEN",
        default=os.getenv("DATABRICKS_TOKEN_TEST", "") or os.getenv("DATABRICKS_TOKEN", ""),
        help="Token for the test workspace. Uses DATABRICKS_TOKEN_TEST, falls back to DATABRICKS_TOKEN.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=int(os.getenv("DATABRICKS_LIMIT", "1000")),
        help="Max rows to pull from the devices table (default: 1000). Secondary tables pull all matching rows.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=int(os.getenv("DATABRICKS_QUERY_TIMEOUT", "300")),
        help="Per-query timeout in seconds, covering cold warehouse start (default: 300). Uses DATABRICKS_QUERY_TIMEOUT.",
    )
    parser.add_argument(
        "--output-raw",
        metavar="FILE",
        default="",
        help="Path for the raw merged JSON. Defaults to output/devices_<timestamp>_raw.json.",
    )
    parser.add_argument(
        "--output-drata",
        metavar="FILE",
        default="",
        help="Path for the Drata-formatted JSON. Defaults to output/devices_<timestamp>_drata.json.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        default=False,
        help="Print full resolved config and env var sources before running.",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()

    missing = [name for name, val in [
        ("--devices (or DATABRICKS_TABLE_DEVICES)", args.devices),
        ("--wu (or DATABRICKS_TABLE_WINDOWS_UPDATE)", args.wu),
        ("--software (or DATABRICKS_TABLE_INSTALLED_SOFTWARE)", args.software),
        ("--users (or DATABRICKS_TABLE_USERS)", args.users),
        ("--host-prod (or DATABRICKS_HOST_PROD)", args.host_prod),
        ("--host-test (or DATABRICKS_HOST_TEST)", args.host_test),
        ("--warehouse-prod (or DATABRICKS_WAREHOUSE_ID)", args.warehouse_prod),
        ("--warehouse-test (or DATABRICKS_WAREHOUSE_ID_TEST)", args.warehouse_test),
    ] if not val.strip()]

    if missing:
        print("Error: the following required values are not set:")
        for m in missing:
            print(f"  {m}")
        sys.exit(1)

    prod_client = get_client_for(host=args.host_prod, token=args.token_prod)
    test_client = get_client_for(host=args.host_test, token=args.token_test)
    default_raw, default_drata = default_output_paths()
    raw_path = Path(args.output_raw) if args.output_raw else default_raw
    drata_path = Path(args.output_drata) if args.output_drata else default_drata

    print(f"\nProd workspace   : {args.host_prod}")
    print(f"Test workspace   : {args.host_test}")
    print(f"Warehouse (prod) : {args.warehouse_prod}")
    print(f"Warehouse (test) : {args.warehouse_test}")
    print(f"Limit (devices)  : {args.limit} rows")
    print(f"Query timeout    : {args.timeout}s per table")
    print(f"Output (raw)     : {raw_path}")
    print(f"Output (drata)   : {drata_path}")

    if args.debug:
        databrickscfg = Path.home() / ".databrickscfg"
        print(f"\n-- DEBUG --")
        print(f"DATABRICKS_HOST_PROD             : {os.getenv('DATABRICKS_HOST_PROD', '(not set)')}")
        print(f"DATABRICKS_HOST_TEST             : {os.getenv('DATABRICKS_HOST_TEST', '(not set)')}")
        print(f"DATABRICKS_TOKEN_PROD env        : {'(set)' if os.getenv('DATABRICKS_TOKEN_PROD') or os.getenv('DATABRICKS_TOKEN') else '(not set)'}")
        print(f"DATABRICKS_TOKEN_TEST env        : {'(set)' if os.getenv('DATABRICKS_TOKEN_TEST') or os.getenv('DATABRICKS_TOKEN') else '(not set)'}")
        print(f"DATABRICKS_WAREHOUSE_ID          : {os.getenv('DATABRICKS_WAREHOUSE_ID', '(not set)')}")
        print(f"DATABRICKS_WAREHOUSE_ID_TEST     : {os.getenv('DATABRICKS_WAREHOUSE_ID_TEST', '(not set)')}")
        print(f"DATABRICKS_TABLE_DEVICES         : {os.getenv('DATABRICKS_TABLE_DEVICES', '(not set)')}")
        print(f"DATABRICKS_TABLE_WINDOWS_UPDATE  : {os.getenv('DATABRICKS_TABLE_WINDOWS_UPDATE', '(not set)')}")
        print(f"DATABRICKS_TABLE_INSTALLED_SOFTWARE: {os.getenv('DATABRICKS_TABLE_INSTALLED_SOFTWARE', '(not set)')}")
        print(f"DATABRICKS_TABLE_USERS           : {os.getenv('DATABRICKS_TABLE_USERS', '(not set)')}")
        print(f"DATABRICKS_QUERY_TIMEOUT         : {os.getenv('DATABRICKS_QUERY_TIMEOUT', '(not set, using 300)')}")
        print(f"~/.databrickscfg exists          : {databrickscfg.exists()}")
        print(f"-- END DEBUG --\n")
    else:
        print()

    # Step 1: devices sets the scope for all downstream table pulls
    devices = pull_table(prod_client, args.devices, args.warehouse_prod, "devices",
                         limit=args.limit, timeout=args.timeout)
    if not devices:
        print("  [FAIL] devices returned 0 rows -- verify table path and warehouse permissions")
        sys.exit(1)

    # Build IN-clause filters scoped to the pulled device set
    resource_ids = [rid for rid in (get_resource_id(r) for r in devices) if rid is not None]
    netbios_names = [n for n in (r.get('Netbios_Name0') or r.get('Name0') for r in devices) if n]
    rid_filter = _ids_filter(resource_ids)
    name_filter = _names_filter(netbios_names)

    # Step 2: pull secondary tables scoped to the device set (no row cap)
    wu = pull_table(test_client, args.wu, args.warehouse_test, "windows_update",
                    filter_sql=rid_filter, timeout=args.timeout)
    software = pull_table(test_client, args.software, args.warehouse_test, "installed_software",
                          filter_sql=rid_filter, timeout=args.timeout)
    users = pull_table(test_client, args.users, args.warehouse_test, "users",
                       filter_sql=name_filter, timeout=args.timeout)

    for label, result in [("windows_update", wu), ("installed_software", software), ("users", users)]:
        if not result:
            print(f"  [WARN] {label} returned 0 rows")

    print("\nMerging on resource_id / Netbios_Name0 ...")
    merged = merge(devices, wu, software, users)
    print(f"  {len(merged)} device records assembled.")

    print("Transforming to Drata MDM format ...")
    drata_payload = transform_all(merged)
    print(f"  {len(drata_payload)} records transformed.")

    write_json(merged, raw_path)
    print(f"\n[OK] Raw merged JSON  : {raw_path}")

    write_json(drata_payload, drata_path)
    print(f"[OK] Drata MDM JSON   : {drata_path}\n")


if __name__ == "__main__":
    main()
