#!/usr/bin/env python3
"""
Device ETL: pulls three SCCM tables from Databricks, joins them on resource_id,
and writes a single JSON file ready for Drata Custom Device Connection.

Output structure per device:
    {
        "resource_id": 12345,
        "device":            { ...fields from the main device table... },
        "windows_update":    { ...fields from t_sccm_gs_windowsupdate... },
        "installed_software": [ ...one entry per row from t_sccm_gs_installed_software... ]
    }

Usage:
    python scripts/extract_devices.py \\
        --devices    catalog.schema.t_sccm_v_r_system \\
        --wu         catalog.schema.t_sccm_gs_windowsupdate \\
        --software   catalog.schema.t_sccm_gs_installed_software

Table paths and warehouse IDs can also be set via environment variables:
    DATABRICKS_TABLE_DEVICES
    DATABRICKS_TABLE_WINDOWS_UPDATE
    DATABRICKS_TABLE_INSTALLED_SOFTWARE
    DATABRICKS_WAREHOUSE_ID          -- used for the devices table (prod)
    DATABRICKS_WAREHOUSE_ID_TEST     -- used for windows_update and installed_software (test)
                                        Falls back to DATABRICKS_WAREHOUSE_ID if not set.

All three tables are required. Script exits with a non-zero code if any pull fails.
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


def pull_table(client: Any, table: str, warehouse_id: str, limit: int, label: str) -> List[Dict[str, Any]]:
    """Pull a table and return cleaned records. Exits on failure."""
    print(f"  Pulling {label} ({table}) ...")
    try:
        result = queries.run_sql(
            client,
            statement=f"SELECT * FROM {table} LIMIT {limit}",
            warehouse_id=warehouse_id,
        )
        records = rows_to_records(result["columns"], result["rows"])
        print(f"  {len(records)} rows retrieved.")
        return [clean(r) for r in records]
    except Exception as e:
        # Extract the meaningful part of the error before the SDK config dump
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
) -> List[Dict[str, Any]]:
    """
    Left-join windows_update and installed_software onto devices using resource_id.
    Devices with no match in the secondary tables still appear in the output.
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

    output = []
    for device in devices:
        rid = get_resource_id(device)
        device_fields = {k: v for k, v in device.items() if k not in ("resource_id", "ResourceID", "ResourceType")}
        output.append({
            "resource_id": rid,
            "device": device_fields,
            "windows_update": wu_index.get(rid, {}),
            "installed_software": sw_index.get(rid, []),
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
        help="Max rows to pull per table (default: 1000).",
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
    print(f"Limit            : {args.limit} rows per table")
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
        print(f"~/.databrickscfg exists          : {databrickscfg.exists()}")
        print(f"-- END DEBUG --\n")
    else:
        print()

    devices = pull_table(prod_client, args.devices, args.warehouse_prod, args.limit, "devices")
    wu = pull_table(test_client, args.wu, args.warehouse_test, args.limit, "windows_update")
    software = pull_table(test_client, args.software, args.warehouse_test, args.limit, "installed_software")

    print("\nMerging on resource_id ...")
    merged = merge(devices, wu, software)
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
