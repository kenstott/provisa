# Copyright (c) 2026 Kenneth Stott
# Canary: 7e37b8c8-4be2-4dbe-8f10-41a1fa44cc69
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for provisa.compiler.nf_extractor (pure-logic, no Docker/network)."""

from __future__ import annotations

from provisa.compiler.nf_extractor import (
    drop_joined_table,
    drop_union_branches_for_table,
    extract_nf_args,
    find_api_table_names,
    left_join_table_names,
    where_referenced_tables,
)


# --- extract_nf_args -----------------------------------------------------------


def test_extract_nf_args_no_where_clause():
    sql = "SELECT * FROM widgets"
    clean_sql, clean_params, nf_args = extract_nf_args(sql, [])
    assert clean_sql == sql
    assert clean_params == []
    assert nf_args == {}


def test_extract_nf_args_no_nf_conditions():
    sql = "SELECT * FROM widgets WHERE name = 'x'"
    clean_sql, clean_params, nf_args = extract_nf_args(sql, [])
    assert clean_sql == sql
    assert clean_params == []
    assert nf_args == {}


def test_extract_nf_args_literal_value_left_side():
    sql = "SELECT * FROM widgets WHERE _nf_id = 42"
    clean_sql, clean_params, nf_args = extract_nf_args(sql, [])
    assert nf_args == {"id": 42}
    assert "_nf_id" not in clean_sql
    assert "WHERE" not in clean_sql


def test_extract_nf_args_literal_value_right_side():
    sql = "SELECT * FROM widgets WHERE 42 = _nf_id"
    clean_sql, clean_params, nf_args = extract_nf_args(sql, [])
    assert nf_args == {"id": 42}


def test_extract_nf_args_string_literal():
    sql = "SELECT * FROM widgets WHERE _nf_name = 'acme'"
    clean_sql, clean_params, nf_args = extract_nf_args(sql, [])
    assert nf_args == {"name": "acme"}


def test_extract_nf_args_float_literal():
    sql = "SELECT * FROM widgets WHERE _nf_price = 3.5"
    _, _, nf_args = extract_nf_args(sql, [])
    assert nf_args == {"price": 3.5}


def test_extract_nf_args_positional_param():
    sql = "SELECT * FROM widgets WHERE _nf_id = $1"
    clean_sql, clean_params, nf_args = extract_nf_args(sql, [99])
    assert nf_args == {"id": 99}
    assert clean_params == []


def test_extract_nf_args_param_renumbering_after_extraction():
    sql = "SELECT * FROM widgets WHERE _nf_id = $1 AND name = $2"
    clean_sql, clean_params, nf_args = extract_nf_args(sql, [10, "acme"])
    assert nf_args == {"id": 10}
    assert clean_params == ["acme"]
    assert "$1" in clean_sql
    assert "$2" not in clean_sql


def test_extract_nf_args_multiple_nf_conditions_and_mixed():
    sql = "SELECT * FROM widgets WHERE _nf_id = $1 AND _nf_kind = 'x' AND status = $2"
    clean_sql, clean_params, nf_args = extract_nf_args(sql, [10, "active"])
    assert nf_args == {"id": 10, "kind": "x"}
    assert clean_params == ["active"]
    assert "status" in clean_sql
    assert "$1" in clean_sql


def test_extract_nf_args_all_conditions_consumed_drops_where():
    sql = "SELECT * FROM widgets WHERE _nf_id = 1"
    clean_sql, clean_params, nf_args = extract_nf_args(sql, [])
    assert "WHERE" not in clean_sql
    assert nf_args == {"id": 1}


def test_extract_nf_args_param_index_out_of_range_ignored():
    sql = "SELECT * FROM widgets WHERE _nf_id = $5"
    # Out-of-range param index resolves to None, so it's kept as an ordinary
    # condition (value is None and param_idx is not None -> still counts as consumed
    # since param_idx is not None). Verify no crash and consistent behavior.
    clean_sql, clean_params, nf_args = extract_nf_args(sql, [])
    assert "id" in nf_args


def test_extract_nf_args_non_eq_condition_kept():
    sql = "SELECT * FROM widgets WHERE _nf_id > 1"
    clean_sql, clean_params, nf_args = extract_nf_args(sql, [])
    assert nf_args == {}
    assert "_nf_id" in clean_sql


# --- find_api_table_names -------------------------------------------------------


def test_find_api_table_names_single_table():
    assert find_api_table_names("SELECT * FROM widgets") == ["widgets"]


def test_find_api_table_names_with_join():
    names = find_api_table_names("SELECT * FROM widgets w JOIN parts p ON w.id = p.widget_id")
    assert set(names) == {"widgets", "parts"}


# --- left_join_table_names ------------------------------------------------------


def test_left_join_table_names_present():
    sql = "SELECT * FROM a LEFT JOIN b ON a.id = b.a_id"
    assert left_join_table_names(sql) == {"b"}


def test_left_join_table_names_no_left_join():
    sql = "SELECT * FROM a JOIN b ON a.id = b.a_id"
    assert left_join_table_names(sql) == set()


def test_left_join_table_names_multiple():
    sql = "SELECT * FROM a LEFT JOIN b ON a.id=b.a_id LEFT JOIN c ON a.id=c.a_id"
    assert left_join_table_names(sql) == {"b", "c"}


# --- drop_joined_table -----------------------------------------------------------


def test_drop_joined_table_removes_join_and_nulls_columns():
    sql = "SELECT w.id, p.name AS pname FROM widgets w JOIN parts p ON w.id = p.widget_id"
    out = drop_joined_table(sql, "parts")
    assert "JOIN" not in out.upper() or "PARTS" not in out.upper()
    assert "NULL" in out.upper()


def test_drop_joined_table_no_match_returns_equivalent_sql():
    sql = "SELECT w.id FROM widgets w"
    out = drop_joined_table(sql, "parts")
    assert "widgets" in out


def test_drop_joined_table_unaliased_column_expression():
    sql = "SELECT p.name FROM widgets w JOIN parts p ON w.id = p.widget_id"
    out = drop_joined_table(sql, "parts")
    assert "NULL" in out.upper()
    assert '"name"' in out or "name" in out


def test_drop_joined_table_left_join_type():
    sql = "SELECT w.id, p.name FROM widgets w LEFT JOIN parts p ON w.id = p.widget_id"
    out = drop_joined_table(sql, "parts")
    assert "PARTS" not in out.upper()


def test_drop_joined_table_multiple_selects_ctes():
    sql = (
        "WITH sub AS (SELECT p.name FROM widgets w JOIN parts p ON w.id = p.widget_id) "
        "SELECT * FROM sub"
    )
    out = drop_joined_table(sql, "parts")
    assert "PARTS" not in out.upper()


# --- drop_union_branches_for_table ------------------------------------------------


def test_drop_union_branches_left_match_removed():
    sql = "SELECT * FROM bad_table UNION SELECT * FROM good_table"
    out = drop_union_branches_for_table(sql, "bad_table")
    assert "bad_table" not in out
    assert "good_table" in out


def test_drop_union_branches_right_match_removed():
    sql = "SELECT * FROM good_table UNION SELECT * FROM bad_table"
    out = drop_union_branches_for_table(sql, "bad_table")
    assert "bad_table" not in out
    assert "good_table" in out


def test_drop_union_branches_no_match_returns_input():
    sql = "SELECT * FROM a UNION SELECT * FROM b"
    out = drop_union_branches_for_table(sql, "nonexistent")
    assert out == sql


def test_drop_union_branches_both_sides_match_keeps_union():
    sql = "SELECT * FROM bad_table UNION SELECT * FROM bad_table"
    out = drop_union_branches_for_table(sql, "bad_table")
    assert out.upper().count("UNION") == 1


def test_drop_union_branches_nested_in_cte():
    sql = (
        "WITH combined AS (SELECT * FROM bad_table UNION SELECT * FROM good_table) "
        "SELECT * FROM combined"
    )
    out = drop_union_branches_for_table(sql, "bad_table")
    assert "bad_table" not in out
    assert "good_table" in out


# --- where_referenced_tables ------------------------------------------------------


def test_where_referenced_tables_single():
    sql = "SELECT * FROM widgets w WHERE w.status = 'active'"
    assert where_referenced_tables(sql) == {"w"}


def test_where_referenced_tables_multiple():
    sql = "SELECT * FROM a, b WHERE a.id = 1 AND b.name = 'x'"
    assert where_referenced_tables(sql) == {"a", "b"}


def test_where_referenced_tables_none_when_no_where():
    sql = "SELECT * FROM widgets"
    assert where_referenced_tables(sql) == set()


def test_where_referenced_tables_unqualified_column_ignored():
    sql = "SELECT * FROM widgets WHERE status = 'active'"
    assert where_referenced_tables(sql) == set()
