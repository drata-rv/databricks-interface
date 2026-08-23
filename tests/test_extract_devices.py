"""
etl/extract_devices.py pure-function tests -- _names_filter and _ids_filter build raw SQL
fragments with no external dependencies, so they're directly testable with no mocking.
"""

from etl.extract_devices import _ids_filter, _names_filter, _latest_batch_clause


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
