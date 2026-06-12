"""
Databricks query helpers.

Each function accepts a WorkspaceClient so the caller controls auth;
functions are independently testable and composable in larger scripts.
"""

import os
import time
from typing import Any, Dict, Iterator, List, Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState


# ---------------------------------------------------------------------------
# Connectivity probes
# ---------------------------------------------------------------------------

def check_workspace(client: WorkspaceClient) -> Dict[str, Any]:
    """Verify credentials by fetching workspace root metadata."""
    status = client.workspace.get_status(path="/")
    return {
        "object_id": status.object_id,
        "object_type": status.object_type.value if status.object_type else None,
        "path": status.path,
    }


def list_sql_warehouses(client: WorkspaceClient) -> List[Dict[str, Any]]:
    """Return all SQL warehouses and their current state."""
    return [
        {
            "id": wh.id,
            "name": wh.name,
            "state": wh.state.value if wh.state else None,
            "cluster_size": wh.cluster_size,
            "num_clusters": wh.num_clusters,
        }
        for wh in client.warehouses.list()
    ]


# ---------------------------------------------------------------------------
# Unity Catalog / metastore browsing
# ---------------------------------------------------------------------------

def list_catalogs(client: WorkspaceClient) -> List[str]:
    """Return the names of all catalogs the caller can see."""
    return [c.name for c in client.catalogs.list() if c.name]


def list_schemas(client: WorkspaceClient, catalog: str) -> List[str]:
    """Return schema names within *catalog*."""
    return [s.name for s in client.schemas.list(catalog_name=catalog) if s.name]


def list_tables(
    client: WorkspaceClient,
    catalog: str,
    schema: str,
) -> List[Dict[str, Any]]:
    """Return tables (and views) within *catalog*.*schema*."""
    return [
        {
            "name": t.name,
            "full_name": t.full_name,
            "table_type": t.table_type.value if t.table_type else None,
        }
        for t in client.tables.list(catalog_name=catalog, schema_name=schema)
    ]


# ---------------------------------------------------------------------------
# SQL statement execution
# ---------------------------------------------------------------------------

def run_sql(
    client: WorkspaceClient,
    statement: str,
    warehouse_id: str,
    *,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    timeout_seconds: int = 300,
) -> Dict[str, Any]:
    """Execute *statement* on *warehouse_id* and return rows + column names.

    Uses EXTERNAL_LINKS disposition with CSV format. Databricks writes each
    result chunk to cloud storage and returns pre-signed URLs; this process
    downloads them with no per-response byte limit (replaces INLINE which was
    capped at 25 MB and failed on large tables such as installed_software).

    Returns:
        {"columns": [str, ...], "rows": [[str, ...], ...], "state": "SUCCEEDED"}

    timeout_seconds covers cold warehouse start (1-3 min serverless). The
    server-side wait is capped at 50s per API limits; remaining time polls.
    """
    import csv
    import io
    import requests as _req
    from databricks.sdk.service.sql import Disposition, Format

    server_wait = min(timeout_seconds, 50)
    request_kwargs: Dict[str, Any] = dict(
        statement=statement,
        warehouse_id=warehouse_id,
        wait_timeout=f"{server_wait}s",
        disposition=Disposition.EXTERNAL_LINKS,
        format=Format.CSV,
    )
    if catalog:
        request_kwargs["catalog"] = catalog
    if schema:
        request_kwargs["schema"] = schema

    response = client.statement_execution.execute_statement(**request_kwargs)

    deadline = time.monotonic() + timeout_seconds
    elapsed = 0
    while response.status and response.status.state in (
        StatementState.PENDING,
        StatementState.RUNNING,
    ):
        if time.monotonic() > deadline:
            try:
                client.statement_execution.cancel_execution(
                    statement_id=response.statement_id
                )
            except Exception:
                pass
            raise RuntimeError(
                f"Query timed out after {timeout_seconds}s -- statement {response.statement_id} cancelled"
            )
        print(f"  Waiting for query... ({elapsed}s)", end="\r", flush=True)
        time.sleep(1)
        elapsed += 1
        response = client.statement_execution.get_statement(
            statement_id=response.statement_id
        )
    if elapsed:
        print()

    state = response.status.state if response.status else None

    if state != StatementState.SUCCEEDED:
        error_msg = (
            response.status.error.message
            if response.status and response.status.error
            else "unknown error"
        )
        raise RuntimeError(
            f"Statement finished with state {state}: {error_msg}"
        )

    schema_obj = response.manifest.schema if response.manifest else None
    columns = (
        [col.name for col in schema_obj.columns]
        if schema_obj and schema_obj.columns
        else []
    )

    def _fetch_chunk(url: str) -> List[List[str]]:
        resp = _req.get(url, timeout=300)
        resp.raise_for_status()
        rows = list(csv.reader(io.StringIO(resp.text)))
        # CSV chunks have no header row; strip one defensively if present.
        if rows and rows[0] == columns:
            rows = rows[1:]
        return rows

    all_rows: List[List[Any]] = []
    links = list(response.result.external_links or []) if response.result else []
    next_chunk = response.result.next_chunk_index if response.result else None

    for link in links:
        all_rows.extend(_fetch_chunk(link.external_link))

    while next_chunk is not None:
        chunk = client.statement_execution.get_statement_result_chunk_n(
            statement_id=response.statement_id,
            chunk_index=next_chunk,
        )
        for link in (chunk.external_links or []):
            all_rows.extend(_fetch_chunk(link.external_link))
        next_chunk = chunk.next_chunk_index

    return {"columns": columns, "rows": all_rows, "state": state.value}


_CSV_NULL = frozenset({'null', 'NULL', 'Null'})


def rows_to_records(
    columns: List[str],
    rows: List[List[Any]],
) -> List[Dict[str, Any]]:
    def _clean(v: Any) -> Any:
        return None if isinstance(v, str) and v in _CSV_NULL else v
    return [dict(zip(columns, (_clean(v) for v in row))) for row in rows]
