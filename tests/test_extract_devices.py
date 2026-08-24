"""
etl/extract_devices.py pure-function tests -- _names_filter and _ids_filter build raw SQL
fragments with no external dependencies, so they're directly testable with no mocking.
"""

from unittest import mock

from etl.extract_devices import (
    _ids_filter,
    _names_filter,
    _latest_batch_clause,
    _iamdb_personnel_statement,
    _sccm_employee_bridge_statement,
    pull_table,
)


def test_names_filter_empty_list_is_always_false():
    assert _names_filter([]) == "1=0"


def test_names_filter_escapes_single_quotes():
    assert _names_filter(["O'Brien"]) == "Netbios_Name0 IN ('O''Brien')"


def test_names_filter_escapes_backslash_before_quote_breakout():
    """A name ending in a backslash must not be able to escape the literal's closing quote --
    backslash is the escape metacharacter in Spark SQL string literals (commit ...)."""
    assert _names_filter(["jdoe\\"]) == "Netbios_Name0 IN ('jdoe\\\\')"


def test_names_filter_backslash_and_quote_together():
    assert _names_filter(["a\\'b"]) == "Netbios_Name0 IN ('a\\\\''b')"


def test_names_filter_custom_column():
    assert _names_filter(["jdoe"], column="user_name0") == "user_name0 IN ('jdoe')"


def test_ids_filter_empty_list_is_always_false():
    assert _ids_filter([]) == "1=0"


def test_ids_filter_builds_in_clause():
    assert _ids_filter([1, 2, 3]) == "resource_id IN (1, 2, 3)"


def test_latest_batch_clause_references_table_and_both_columns():
    """Applied to every raw SCCM landing table (devices, the 6 secondary tables, and now
    users) -- must reference __date and __hour against the exact table path passed in,
    since it's interpolated into a raw SQL WHERE clause with no other validation."""
    clause = _latest_batch_clause("catalog.schema.t_sccm_r_user")
    assert "__date" in clause
    assert "__hour" in clause
    assert clause.count("catalog.schema.t_sccm_r_user") == 3
    assert clause.strip().startswith("AND")


def test_latest_batch_clause_is_appendable_to_a_base_filter():
    """Callers append this directly onto an existing filter string (device_filter +=
    _latest_batch_clause(...)) -- it must start with a leading space then AND, not stand
    alone, or the concatenated SQL breaks."""
    clause = _latest_batch_clause("t")
    assert clause.startswith(" AND")


def test_iamdb_personnel_statement_filters_to_active_employees_only():
    """employeetype='E'/employeestatus='A' is the confirmed (2026-08-24, matched against
    Nationwide's own 21,337 active-personnel figure) definition of in-scope personnel --
    other employeetype codes are contractors/non-employees/service accounts."""
    stmt = _iamdb_personnel_statement("catalog.schema.t_iamdb_userdata")
    assert "employeetype = 'E'" in stmt
    assert "employeestatus = 'A'" in stmt
    assert "GROUP BY employeenumber" in stmt
    assert "__date" in stmt and "__hour" in stmt
    assert "catalog.schema.t_iamdb_userdata" in stmt


def test_iamdb_personnel_statement_excludes_blank_employeenumber():
    stmt = _iamdb_personnel_statement("t")
    assert "employeenumber IS NOT NULL" in stmt
    assert "employeenumber != ''" in stmt


def test_sccm_employee_bridge_statement_dedupes_per_employee_number():
    """Devices only carry user_name0/user_domain0, not employee_number -- this bridges
    iamdb's authoritative personnel record to SCCM's device-login username. Must produce
    exactly one row per employee_number (their single most recent), not fan out."""
    stmt = _sccm_employee_bridge_statement("catalog.schema.t_sccm_r_user")
    assert "PARTITION BY employee_number" in stmt
    assert "ROW_NUMBER()" in stmt
    assert "user_name0" in stmt
    assert "windows_nt_domain0" in stmt
    assert "employee_number IS NOT NULL" in stmt
    assert "catalog.schema.t_sccm_r_user" in stmt


def test_pull_table_statement_override_bypasses_select_star_construction():
    """statement= must be used verbatim -- filter_sql/limit are for the default
    SELECT * FROM table path only, and must be silently ignored when statement is given
    (used for iamdb's GROUP BY / window-function dedup, which SELECT * can't express)."""
    fake_result = {"columns": ["employeenumber", "mail"], "rows": []}
    with mock.patch("etl.extract_devices.queries.run_sql", return_value=fake_result) as run_sql:
        pull_table(
            client=mock.Mock(),
            table="t_iamdb_userdata",
            warehouse_id="wh1",
            label="iamdb personnel",
            filter_sql="this_should_be_ignored = 1",
            limit=999,
            statement="SELECT employeenumber, mail FROM t_iamdb_userdata GROUP BY employeenumber",
        )
    actual_statement = run_sql.call_args.kwargs["statement"]
    assert actual_statement == "SELECT employeenumber, mail FROM t_iamdb_userdata GROUP BY employeenumber"
    assert "this_should_be_ignored" not in actual_statement
    assert "LIMIT" not in actual_statement


def test_pull_table_without_statement_still_builds_select_star():
    """Every existing caller (devices, TABLE_REGISTRY, ...) omits statement= -- this must
    keep building the plain SELECT * FROM table [WHERE ...] [LIMIT ...] exactly as before."""
    fake_result = {"columns": ["resource_id"], "rows": []}
    with mock.patch("etl.extract_devices.queries.run_sql", return_value=fake_result) as run_sql:
        pull_table(
            client=mock.Mock(),
            table="t_sccm_r_system",
            warehouse_id="wh1",
            label="devices",
            filter_sql="resource_id IN (1, 2)",
            limit=5,
        )
    actual_statement = run_sql.call_args.kwargs["statement"]
    assert actual_statement == "SELECT * FROM t_sccm_r_system WHERE resource_id IN (1, 2) LIMIT 5"
