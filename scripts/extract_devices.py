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

Table paths can also be set via environment variables:
    DATABRICKS_TABLE_DEVICES
    DATABRICKS_TABLE_WINDOWS_UPDATE
    DATABRICKS_TABLE_INSTALLED_SOFTWARE

All three tables are required. Script exits with a non-zero code if any pull fails.
"""

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.auth import get_client
from db import queries


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
        print(f"  [FAIL] Could not pull {label}: {e}")
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


def default_output_path() -> Path:
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    return Path("output") / f"devices_{timestamp}.json"


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
        "--limit",
        type=int,
        default=int(os.getenv("DATABRICKS_LIMIT", "1000")),
        help="Max rows to pull per table (default: 1000).",
    )
    parser.add_argument(
        "--output",
        metavar="FILE",
        default="",
        help="Output file path. Defaults to output/devices_<timestamp>.json.",
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
    ] if not val.strip()]

    if missing:
        print("Error: the following required table paths are not set:")
        for m in missing:
            print(f"  {m}")
        sys.exit(1)

    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID", "")
    if not warehouse_id:
        print("Error: DATABRICKS_WAREHOUSE_ID is not set.")
        sys.exit(1)

    client = get_client()
    output_path = Path(args.output) if args.output else default_output_path()

    print(f"\nWarehouse : {warehouse_id}")
    print(f"Limit     : {args.limit} rows per table")
    print(f"Output    : {output_path}\n")

    devices = pull_table(client, args.devices, warehouse_id, args.limit, "devices")
    wu = pull_table(client, args.wu, warehouse_id, args.limit, "windows_update")
    software = pull_table(client, args.software, warehouse_id, args.limit, "installed_software")

    print("\nMerging on resource_id ...")
    payload = merge(devices, wu, software)
    print(f"  {len(payload)} device records assembled.")

    write_json(payload, output_path)
    print(f"\n[OK] Written to {output_path}\n")


if __name__ == "__main__":
    main()
