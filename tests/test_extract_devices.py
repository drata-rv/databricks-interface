"""
etl/extract_devices.py pure-function tests -- _names_filter and _ids_filter build raw SQL
fragments with no external dependencies, so they're directly testable with no mocking.
"""

from etl.extract_devices import _ids_filter, _names_filter


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
