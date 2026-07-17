# Copyright (c) 2026 Kenneth Stott
# Canary: 2af8ab62-fcda-4876-9364-1040f6919d99
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for provisa.transpiler.transpile_correlated (pure-logic, no Docker/network)."""

from __future__ import annotations

from provisa.transpiler.transpile_correlated import (
    _rewrite_correlated_json_to_ctes,
    rewrite_correlated_subqueries_for_trino,
)


# --- _rewrite_correlated_json_to_ctes -------------------------------------------


def test_rewrite_json_to_ctes_many_to_one():
    sql = (
        "SELECT w.id, "
        "(SELECT json_object('name': p.name) FROM parts p WHERE p.id = w.part_id) AS part "
        "FROM widgets w"
    )
    out = _rewrite_correlated_json_to_ctes(sql)
    assert "WITH" in out.upper()
    assert "_rel_0" in out
    assert "LEFT JOIN" in out.upper()


def test_rewrite_json_to_ctes_no_match_returns_input_unchanged():
    sql = "SELECT w.id, w.name FROM widgets w"
    out = _rewrite_correlated_json_to_ctes(sql)
    assert out == sql


def test_rewrite_json_to_ctes_non_select_tree_returns_input():
    sql = "CREATE TABLE t (id int)"
    out = _rewrite_correlated_json_to_ctes(sql)
    assert "CREATE TABLE" in out.upper()


def test_rewrite_json_to_ctes_sampling_wrapper_hoists_ctes():
    inner = (
        "SELECT w.id, "
        "(SELECT json_object('name': p.name) FROM parts p WHERE p.id = w.part_id) AS part "
        "FROM widgets w"
    )
    sql = f"SELECT * FROM ({inner}) AS sample_wrap LIMIT 100"
    out = _rewrite_correlated_json_to_ctes(sql)
    assert "WITH" in out.upper()
    assert "LIMIT 100" in out


def test_rewrite_json_to_ctes_sampling_wrapper_no_inner_change_returns_input():
    inner = "SELECT w.id, w.name FROM widgets w"
    sql = f"SELECT * FROM ({inner}) AS sample_wrap LIMIT 100"
    out = _rewrite_correlated_json_to_ctes(sql)
    assert out == sql


def test_rewrite_json_to_ctes_nested_json_flattened_with_join():
    sql = (
        "SELECT w.id, "
        "(SELECT json_object('name': p.name, "
        "'maker': (SELECT json_object('mname': m.name) FROM makers m WHERE m.id = p.maker_id)) "
        "FROM parts p WHERE p.id = w.part_id) AS part "
        "FROM widgets w"
    )
    out = _rewrite_correlated_json_to_ctes(sql)
    assert "makers" in out.lower()
    assert "LEFT JOIN" in out.upper()


def test_rewrite_json_to_ctes_many_to_one_no_where_not_rewritten():
    sql = "SELECT w.id, (SELECT json_object('name': p.name) FROM parts p) AS part FROM widgets w"
    out = _rewrite_correlated_json_to_ctes(sql)
    assert out == sql


def test_rewrite_json_to_ctes_many_to_one_non_eq_where_not_rewritten():
    sql = (
        "SELECT w.id, "
        "(SELECT json_object('name': p.name) FROM parts p WHERE p.id > w.part_id) AS part "
        "FROM widgets w"
    )
    out = _rewrite_correlated_json_to_ctes(sql)
    assert out == sql


def test_rewrite_json_to_ctes_many_to_one_from_subquery():
    sql = (
        "SELECT w.id, "
        "(SELECT json_object('name': p.name) "
        "FROM (SELECT id, name FROM parts) p WHERE p.id = w.part_id) AS part "
        "FROM widgets w"
    )
    out = _rewrite_correlated_json_to_ctes(sql)
    assert "_rel_0" in out


def test_rewrite_json_to_ctes_existing_with_clause_merges():
    sql = (
        "WITH base AS (SELECT 1 AS x) "
        "SELECT w.id, "
        "(SELECT json_object('name': p.name) FROM parts p WHERE p.id = w.part_id) AS part "
        "FROM widgets w"
    )
    out = _rewrite_correlated_json_to_ctes(sql)
    assert "base" in out
    assert "_rel_0" in out


# --- rewrite_correlated_subqueries_for_trino ------------------------------------


def test_lift_general_scalar_subquery_no_agg_no_limit():
    sql = (
        "SELECT w.id, "
        "(SELECT p.name FROM parts p WHERE p.widget_id = w.id) AS part_name "
        "FROM widgets w"
    )
    out = rewrite_correlated_subqueries_for_trino(sql)
    assert "_grel_0" in out
    assert "ARBITRARY" in out.upper()
    assert "LEFT JOIN" in out.upper()


def test_lift_general_scalar_subquery_with_limit():
    sql = (
        "SELECT w.id, "
        "(SELECT p.name FROM parts p WHERE p.widget_id = w.id LIMIT 1) AS part_name "
        "FROM widgets w"
    )
    out = rewrite_correlated_subqueries_for_trino(sql)
    assert "_grel_0" in out
    assert "ARBITRARY" not in out.upper()


def test_lift_general_aggregate_subquery():
    sql = (
        "SELECT w.id, "
        "(SELECT COUNT(p.id) FROM parts p WHERE p.widget_id = w.id) AS part_count "
        "FROM widgets w"
    )
    out = rewrite_correlated_subqueries_for_trino(sql)
    assert "_grel_0" in out
    assert "GROUP BY" in out.upper()


def test_lift_general_no_correlated_subquery_returns_input():
    sql = "SELECT w.id, w.name FROM widgets w"
    out = rewrite_correlated_subqueries_for_trino(sql)
    assert out == sql


def test_lift_general_non_select_tree_returns_input():
    sql = "CREATE TABLE t (id int)"
    out = rewrite_correlated_subqueries_for_trino(sql)
    assert "CREATE TABLE" in out.upper()


def test_lift_general_sampling_wrapper_hoists_ctes():
    inner = (
        "SELECT w.id, "
        "(SELECT p.name FROM parts p WHERE p.widget_id = w.id) AS part_name "
        "FROM widgets w"
    )
    sql = f"SELECT * FROM ({inner}) AS sample_wrap LIMIT 50"
    out = rewrite_correlated_subqueries_for_trino(sql)
    assert "_grel_0" in out
    assert "LIMIT 50" in out


def test_lift_general_sampling_wrapper_no_inner_change_returns_input():
    inner = "SELECT w.id, w.name FROM widgets w"
    sql = f"SELECT * FROM ({inner}) AS sample_wrap LIMIT 50"
    out = rewrite_correlated_subqueries_for_trino(sql)
    assert out == sql


def test_lift_general_multi_condition_and_where():
    sql = (
        "SELECT w.id, "
        "(SELECT p.name FROM parts p WHERE p.widget_id = w.id AND p.active = w.active) AS part_name "
        "FROM widgets w"
    )
    out = rewrite_correlated_subqueries_for_trino(sql)
    assert "_grel_0" in out
    assert out.count("_jk0") >= 1
    assert "_jk1" in out


def test_lift_general_local_condition_kept_in_cte_where():
    sql = (
        "SELECT w.id, "
        "(SELECT p.name FROM parts p WHERE p.widget_id = w.id AND p.status = 'active') AS part_name "
        "FROM widgets w"
    )
    out = rewrite_correlated_subqueries_for_trino(sql)
    assert "'active'" in out


def test_lift_general_json_object_expression():
    sql = (
        "SELECT w.id, "
        "json_object('nm': (SELECT p.name FROM parts p WHERE p.widget_id = w.id)) AS blob "
        "FROM widgets w"
    )
    out = rewrite_correlated_subqueries_for_trino(sql)
    assert "_grel_0" in out


def test_lift_general_json_agg_expression():
    sql = (
        "SELECT w.id, "
        "json_agg(json_object('nm': p.name)) AS blob "
        "FROM widgets w, parts p WHERE p.widget_id = w.id"
    )
    out = rewrite_correlated_subqueries_for_trino(sql)
    # No correlated subquery inside json_agg/json_object literal args here -> no lift.
    assert out == sql


def test_lift_general_json_array_agg_expression():
    # JSON_AGG(<subquery>) parses to sqlglot's exp.JSONArrayAgg class (unlike
    # JSON_ARRAYAGG(...), which parses as a bare exp.Anonymous call) — this is
    # what exercises the exp.JSONArrayAgg branch in _lift_correlated_in_expr.
    sql = (
        "SELECT w.id, "
        "JSON_AGG((SELECT p.name FROM parts p WHERE p.widget_id = w.id)) AS blob "
        "FROM widgets w"
    )
    out = rewrite_correlated_subqueries_for_trino(sql)
    assert "_grel_0" in out


def test_lift_general_no_where_clause_inner_select_no_lift():
    sql = "SELECT w.id, (SELECT p.name FROM parts p) AS part_name FROM widgets w"
    out = rewrite_correlated_subqueries_for_trino(sql)
    assert out == sql


def test_lift_general_uncorrelated_subquery_not_lifted():
    sql = (
        "SELECT w.id, "
        "(SELECT p.name FROM parts p WHERE p.status = 'active' LIMIT 1) AS part_name "
        "FROM widgets w"
    )
    out = rewrite_correlated_subqueries_for_trino(sql)
    assert out == sql


def test_lift_general_multiple_select_expressions_only_correlated_rewritten():
    sql = (
        "SELECT w.id, w.name, "
        "(SELECT p.name FROM parts p WHERE p.widget_id = w.id) AS part_name "
        "FROM widgets w"
    )
    out = rewrite_correlated_subqueries_for_trino(sql)
    assert "w.name" in out.lower() or '"name"' in out.lower()
    assert "_grel_0" in out


# --- Regression: one-to-many json_agg(json_object(...)) must lift to a sub-CTE ----
# The compiler (sql_selection.py) emits SQL-standard json_object, which sqlglot parses
# to exp.JSONArrayAgg(this=JSONObject) — not exp.Anonymous. Before the fix the detector
# only matched exp.Anonymous, so the one-to-many aggregate block was never reached.
def test_one_to_many_json_agg_json_object_lifts_to_sub_cte():
    sql = (
        "SELECT o.id, "
        "(SELECT json_agg(json_object(KEY 'id' VALUE t.id)) "
        "FROM t WHERE t.pid = o.id) AS items "
        "FROM o"
    )
    out = _rewrite_correlated_json_to_ctes(sql)
    assert out != sql  # rewrite fired (was dead code)
    assert "_rel_0_agg" in out
    assert "JSON_AGG(_REL_0._JSON)" in out.upper().replace('"', "")
    assert "GROUP BY" in out.upper()


def test_one_to_many_json_agg_json_object_nested_in_parent_object():
    # json_object with a nested one-to-many json_agg(json_object(...)) value
    sql = (
        "SELECT o.id, "
        "(SELECT json_object("
        "KEY 'children' VALUE (SELECT json_agg(json_object(KEY 'cid' VALUE c.id)) "
        "FROM c WHERE c.pid = p.id)) "
        "FROM p WHERE p.oid = o.id) AS tree "
        "FROM o"
    )
    out = _rewrite_correlated_json_to_ctes(sql)
    assert out != sql
    assert "JSON_AGG" in out.upper().replace('"', "")
