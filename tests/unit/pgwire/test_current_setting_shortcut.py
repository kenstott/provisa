"""REQ-2167, REQ-2204: only a statement that is SELECT current_setting('x') [AS a] (with
set_config) takes the shortcut; every other statement mentioning current_setting reads the catalog."""

import pytest

from provisa.pgwire.catalog import _handle_current_setting

SETTINGS = {"search_path": '"$user", public', "server_version_num": "170000"}

GRAFANA = [
    "SELECT current_setting('server_version_num')::int/100 as version",
    'SELECT quote_ident(table_name) AS "table" FROM information_schema.tables '
    "WHERE quote_ident(table_schema) IN (SELECT trim(s[i]) FROM generate_series(1, 2) as i, "
    "string_to_array(current_setting('search_path'),',') s)",
    'SELECT quote_ident(column_name) AS "column", data_type AS "type" '
    "FROM information_schema.columns WHERE quote_ident(table_schema) IN "
    "(SELECT trim(s[i]) FROM generate_series(array_lower(string_to_array("
    "current_setting('search_path'),','),1), 2) as i, "
    "string_to_array(current_setting('search_path'),',') s)",
]


@pytest.mark.parametrize("sql", GRAFANA)
def test_grafana_statements_are_not_shortcut(sql):
    assert _handle_current_setting(sql, SETTINGS) is None


def test_plain_select_still_answers():
    r = _handle_current_setting("SELECT current_setting('search_path')", SETTINGS)
    assert r.column_names == ["current_setting"]
    assert r.rows == [('"$user", public',)]


def test_alias_is_kept():
    r = _handle_current_setting("SELECT current_setting('search_path') AS sp", SETTINGS)
    assert r.column_names == ["sp"]


def test_with_set_config():
    r = _handle_current_setting(
        "SELECT current_setting('search_path') AS a, set_config('x', 'y', false) AS b", SETTINGS
    )
    assert r.column_names == ["a", "b"]
    assert r.rows == [('"$user", public', "y")]


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT current_setting('search_path') FROM t",
        "SELECT current_setting('search_path') WHERE 1 = 0",
        "SELECT current_setting('server_version_num')::int",
        "SELECT current_setting('a') || 'x'",
    ],
)
def test_other_shapes_fall_through(sql):
    assert _handle_current_setting(sql, SETTINGS) is None
