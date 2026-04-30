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
    wait_timeout_seconds: int = 30,
) -> Dict[str, Any]:
    """Execute *statement* on *warehouse_id* and return rows + column names.

    Returns a dict with keys:
      - columns: list of column names
      - rows:    list of row lists (strings; Databricks returns everything as str)
      - state:   final StatementState value
    """
    from databricks.sdk.service.sql import Disposition

    request_kwargs: Dict[str, Any] = dict(
        statement=statement,
        warehouse_id=warehouse_id,
        wait_timeout=f"{wait_timeout_seconds}s",
        disposition=Disposition.INLINE,
    )
    if catalog:
        request_kwargs["catalog"] = catalog
    if schema:
        request_kwargs["schema"] = schema

    response = client.statement_execution.execute_statement(**request_kwargs)

    # If not yet complete, poll until done or timeout exceeded.
    deadline = time.monotonic() + wait_timeout_seconds
    while response.status and response.status.state in (
        StatementState.PENDING,
        StatementState.RUNNING,
    ):
        if time.monotonic() > deadline:
            break
        time.sleep(1)
        response = client.statement_execution.get_statement(
            statement_id=response.statement_id
        )

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

    schema_obj = (
        response.manifest.schema if response.manifest else None
    )
    columns = (
        [col.name for col in schema_obj.columns]
        if schema_obj and schema_obj.columns
        else []
    )
    rows = response.result.data_array if response.result and response.result.data_array else []

    return {"columns": columns, "rows": rows, "state": state.value}
