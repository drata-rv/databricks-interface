"""
db/queries.py tests. run_sql() and the list_* helpers all take the WorkspaceClient as an
explicit argument, so every test here uses a Mock() in place of a real client -- no network
call, no real credentials, no Databricks workspace needed.
"""

from unittest import mock

from databricks.sdk.service.sql import StatementState

from db import queries


def test_rows_to_records_null_coercion():
    """Pure function, no mocking. Guards the CSV-string-null class of bug (commit 1710721):
    Databricks CSV export represents a null value as the literal string 'null'/'NULL'/'Null',
    which must become Python None, not pass through as a truthy non-empty string."""
    records = queries.rows_to_records(
        ["a", "b", "c"],
        [["1", "null", "x"], ["2", "NULL", "y"], ["3", "Null", "z"], ["4", "not-null", "w"]],
    )
    assert records == [
        {"a": "1", "b": None, "c": "x"},
        {"a": "2", "b": None, "c": "y"},
        {"a": "3", "b": None, "c": "z"},
        {"a": "4", "b": "not-null", "c": "w"},
    ]


def _mock_response(state, statement_id="stmt-1"):
    resp = mock.Mock()
    resp.status.state = state
    resp.statement_id = statement_id
    return resp


def test_run_sql_success():
    client = mock.Mock()
    resp = _mock_response(StatementState.SUCCEEDED)
    col_a, col_b = mock.Mock(), mock.Mock()
    col_a.name, col_b.name = "a", "b"
    resp.manifest.schema.columns = [col_a, col_b]
    link = mock.Mock()
    link.external_link = "https://fake-chunk-url"
    resp.result.external_links = [link]
    resp.result.next_chunk_index = None
    client.statement_execution.execute_statement.return_value = resp

    csv_response = mock.Mock()
    csv_response.text = "1,x\n2,y\n"
    csv_response.raise_for_status.return_value = None

    with mock.patch("requests.get", return_value=csv_response):
        result = queries.run_sql(client, "SELECT 1", "wh1", timeout_seconds=30)

    assert result == {"columns": ["a", "b"], "rows": [["1", "x"], ["2", "y"]], "state": "SUCCEEDED"}


def test_run_sql_timeout():
    client = mock.Mock()
    resp = _mock_response(StatementState.RUNNING, statement_id="stmt-timeout")
    client.statement_execution.execute_statement.return_value = resp
    client.statement_execution.get_statement.return_value = resp  # never leaves RUNNING

    with mock.patch("db.queries.time.sleep", return_value=None):
        try:
            queries.run_sql(client, "SELECT 1", "wh1", timeout_seconds=0)
            assert False, "expected RuntimeError on timeout"
        except RuntimeError as e:
            assert "timed out" in str(e).lower()
            assert "stmt-timeout" in str(e)

    client.statement_execution.cancel_execution.assert_called_once_with(statement_id="stmt-timeout")


def test_run_sql_failed_state():
    client = mock.Mock()
    resp = _mock_response(StatementState.FAILED)
    resp.status.error.message = "Table not found"
    client.statement_execution.execute_statement.return_value = resp

    try:
        queries.run_sql(client, "SELECT 1", "wh1", timeout_seconds=30)
        assert False, "expected RuntimeError on failed statement"
    except RuntimeError as e:
        assert "Table not found" in str(e)


def test_list_catalogs_filters_falsy_names():
    client = mock.Mock()
    named = mock.Mock()
    named.name = "si_prod_catalog"
    unnamed = mock.Mock()
    unnamed.name = ""
    client.catalogs.list.return_value = [named, unnamed]

    assert queries.list_catalogs(client) == ["si_prod_catalog"]


def test_list_schemas_filters_falsy_names():
    client = mock.Mock()
    named = mock.Mock()
    named.name = "nw_harmonized_sensitive"
    unnamed = mock.Mock()
    unnamed.name = None
    client.schemas.list.return_value = [named, unnamed]

    result = queries.list_schemas(client, "si_prod_catalog")
    assert result == ["nw_harmonized_sensitive"]
    client.schemas.list.assert_called_once_with(catalog_name="si_prod_catalog")


def test_list_tables_shape():
    client = mock.Mock()
    t = mock.Mock()
    t.name, t.full_name = "t_sccm_r_user", "catalog.schema.t_sccm_r_user"
    t.table_type.value = "MANAGED"
    client.tables.list.return_value = [t]

    result = queries.list_tables(client, "catalog", "schema")
    assert result == [{"name": "t_sccm_r_user", "full_name": "catalog.schema.t_sccm_r_user", "table_type": "MANAGED"}]
    client.tables.list.assert_called_once_with(catalog_name="catalog", schema_name="schema")
