# Copyright (c) 2026 Kenneth Stott
# Canary: a06089bd-b453-4daa-8692-7fcbd909b7b1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for provisa.compiler.sql_selection (pure-logic, no Docker/network)."""

from __future__ import annotations

from graphql import FieldNode, parse

from provisa.compiler.params import ParamCollector
from provisa.compiler.sql_types import CompilationContext, JoinMeta, TableMeta
from provisa.compiler.sql_selection import (
    _build_gql_selection,
    _build_rel_json_expr,
    _build_rel_json_kv,
    _collect_nested_columns,
    _emit_agg_subqueries,
    _extract_json_blob_kv,
    _lateral_join,
)


def _field(query: str) -> FieldNode:
    """Parse a single-field GraphQL document and return its root FieldNode."""
    doc = parse(query)
    return doc.definitions[0].selection_set.selections[0]


def _table(table_id: int, table_name: str, field_name: str = "widgets") -> TableMeta:
    return TableMeta(
        table_id=table_id,
        field_name=field_name,
        type_name="Widget",
        source_id="src",
        catalog_name="src",
        schema_name="public",
        table_name=table_name,
    )


def _join(target: TableMeta, **overrides) -> JoinMeta:
    kwargs = dict(
        source_column="id",
        target_column="widget_id",
        source_column_type="integer",
        target_column_type="integer",
        target=target,
        cardinality="one-to-many",
    )
    kwargs.update(overrides)
    return JoinMeta(**kwargs)


# --- _lateral_join ------------------------------------------------------------


def test_lateral_join_basic_no_args():
    target = _table(2, "children")
    jm = _join(target, default_limit=5)
    fn = _field("{ children }")
    sql = _lateral_join(fn, jm, "t1", '"t0"."id"', ParamCollector(), None, use_catalog=False)
    assert "LEFT JOIN LATERAL" in sql
    assert '"public"."children"' in sql
    assert 'WHERE "widget_id" = "t0"."id"' in sql
    assert "LIMIT $1" in sql
    assert sql.endswith('"t1" ON TRUE')


def test_lateral_join_with_where_arg():
    target = _table(2, "children")
    jm = _join(target)
    fn = _field('{ children(where: {name: {eq: "x"}}) }')
    collector = ParamCollector()
    sql = _lateral_join(fn, jm, "t1", '"t0"."id"', collector, None, use_catalog=False)
    assert "AND (" in sql
    assert collector.params == ["x"]


def test_lateral_join_distinct_on_string():
    target = _table(2, "children")
    jm = _join(target)
    fn = _field('{ children(distinct_on: "region") }')
    sql = _lateral_join(fn, jm, "t1", '"t0"."id"', ParamCollector(), None, use_catalog=False)
    assert "SELECT DISTINCT ON" in sql
    assert '"region"' in sql


def test_lateral_join_distinct_on_list_with_exposed_mapping():
    target = _table(2, "children")
    jm = _join(target)
    fn = _field('{ children(distinct_on: ["region", "status"]) }')
    e2p = {(2, "status"): "state"}
    sql = _lateral_join(
        fn,
        jm,
        "t1",
        '"t0"."id"',
        ParamCollector(),
        None,
        use_catalog=False,
        exposed_to_physical=e2p,
    )
    assert '"region", "state"' in sql


def test_lateral_join_order_by_dict_wrapped_to_list():
    target = _table(2, "children")
    jm = _join(target)
    fn = _field("{ children(order_by: {name: asc}) }")
    sql = _lateral_join(fn, jm, "t1", '"t0"."id"', ParamCollector(), None, use_catalog=False)
    assert "ORDER BY" in sql


def test_lateral_join_order_by_list():
    target = _table(2, "children")
    jm = _join(target)
    fn = _field("{ children(order_by: [{name: desc}]) }")
    sql = _lateral_join(fn, jm, "t1", '"t0"."id"', ParamCollector(), None, use_catalog=False)
    assert "ORDER BY" in sql
    assert "DESC" in sql


def test_lateral_join_limit_arg_overrides_default():
    target = _table(2, "children")
    jm = _join(target, default_limit=5)
    fn = _field("{ children(limit: 3) }")
    collector = ParamCollector()
    _lateral_join(fn, jm, "t1", '"t0"."id"', collector, None, use_catalog=False)
    assert collector.params == [3]


def test_lateral_join_offset_arg():
    target = _table(2, "children")
    jm = _join(target)
    fn = _field("{ children(offset: 2) }")
    collector = ParamCollector()
    sql = _lateral_join(fn, jm, "t1", '"t0"."id"', collector, None, use_catalog=False)
    assert "OFFSET" in sql
    assert collector.params == [2]


def test_lateral_join_target_expr_replaces_alias():
    target = _table(2, "children")
    jm = _join(target, target_expr='CONCAT({alias}."a", {alias}."b")')
    fn = _field("{ children }")
    sql = _lateral_join(fn, jm, "t1", '"t0"."id"', ParamCollector(), None, use_catalog=False)
    assert 'CONCAT("t1"."a", "t1"."b")' in sql


def test_lateral_join_use_catalog_true():
    target = _table(2, "children")
    jm = _join(target)
    fn = _field("{ children }")
    sql = _lateral_join(fn, jm, "t1", '"t0"."id"', ParamCollector(), None, use_catalog=True)
    assert '"src"."public"."children"' in sql


# --- _emit_agg_subqueries ------------------------------------------------------


def test_emit_agg_subqueries_scalar_no_extra_joins_no_limit():
    parent = _table(1, "widgets")
    ctx = CompilationContext(exposed_to_physical={}, physical_to_sql={(1, "name"): "name"})
    fn = _field("{ widgets { name } }")
    select_parts: list[str] = []
    columns: list = []
    sources: set[str] = set()
    _emit_agg_subqueries(
        fn.selection_set.selections,
        ctx,
        "Widget",
        parent,
        "public.widgets",
        "TRUE",
        "",
        "t0",
        "root",
        None,
        None,
        False,
        1,
        select_parts,
        columns,
        sources,
    )
    assert len(select_parts) == 1
    assert "ARRAY_AGG" in select_parts[0]
    assert "FROM public.widgets" in select_parts[0]
    assert columns[0].is_agg is True


def test_emit_agg_subqueries_scalar_with_extra_joins_and_limit():
    parent = _table(1, "widgets")
    ctx = CompilationContext(physical_to_sql={(1, "name"): "name"})
    fn = _field("{ widgets { name } }")
    select_parts: list[str] = []
    _emit_agg_subqueries(
        fn.selection_set.selections,
        ctx,
        "Widget",
        parent,
        "public.widgets",
        "TRUE",
        "JOIN other o ON o.id = t0.id",
        "t0",
        "root",
        None,
        5,
        False,
        1,
        select_parts,
        [],
        set(),
    )
    assert "LIMIT 5" in select_parts[0]
    assert "JOIN other o" in select_parts[0]


def test_emit_agg_subqueries_scalar_with_limit_no_extra_joins():
    parent = _table(1, "widgets")
    ctx = CompilationContext(physical_to_sql={(1, "name"): "name"})
    fn = _field("{ widgets { name } }")
    select_parts: list[str] = []
    _emit_agg_subqueries(
        fn.selection_set.selections,
        ctx,
        "Widget",
        parent,
        "public.widgets",
        "TRUE",
        "",
        "t0",
        "root",
        None,
        5,
        False,
        1,
        select_parts,
        [],
        set(),
    )
    assert "LIMIT 5" in select_parts[0]
    assert "ARRAY_AGG" in select_parts[0]


def test_emit_agg_subqueries_recurses_into_sub_relationship():
    parent = _table(1, "widgets", field_name="widgets")
    child = _table(2, "children", field_name="children")
    jm = _join(child, cardinality="one-to-many")
    ctx = CompilationContext(
        joins={("Widget", "children"): jm},
        physical_to_sql={(2, "name"): "name"},
    )
    fn = _field("{ widgets { children { name } } }")
    select_parts: list[str] = []
    columns: list = []
    sources: set[str] = set()
    counter = _emit_agg_subqueries(
        fn.selection_set.selections,
        ctx,
        "Widget",
        parent,
        "public.widgets",
        "TRUE",
        "",
        "t0",
        "root",
        None,
        None,
        False,
        1,
        select_parts,
        columns,
        sources,
    )
    assert counter == 2
    assert "src" in sources
    assert len(select_parts) == 1
    assert columns[0].nested_in == "root.children"


def test_emit_agg_subqueries_recurses_with_source_constant_and_target_expr():
    parent = _table(1, "widgets", field_name="widgets")
    child = _table(2, "children", field_name="children")
    jm = _join(
        child,
        cardinality="one-to-many",
        source_constant="pets",
        target_expr='CONCAT({alias}."a", {alias}."b")',
    )
    ctx = CompilationContext(
        joins={("Widget", "children"): jm},
        physical_to_sql={(2, "name"): "name"},
    )
    fn = _field("{ widgets { children { name } } }")
    select_parts: list = []
    _emit_agg_subqueries(
        fn.selection_set.selections,
        ctx,
        "Widget",
        parent,
        "public.widgets",
        "TRUE",
        "",
        "t0",
        "root",
        None,
        None,
        False,
        1,
        select_parts,
        [],
        set(),
    )
    assert "'pets'" in select_parts[0]
    assert "CONCAT(" in select_parts[0]


def test_emit_agg_subqueries_recurses_with_source_expr():
    parent = _table(1, "widgets", field_name="widgets")
    child = _table(2, "children", field_name="children")
    jm = _join(
        child,
        cardinality="one-to-many",
        source_expr='{alias}."custom"',
        source_column_type="varchar",
    )
    ctx = CompilationContext(
        joins={("Widget", "children"): jm},
        physical_to_sql={(2, "name"): "name"},
    )
    fn = _field("{ widgets { children { name } } }")
    select_parts: list = []
    _emit_agg_subqueries(
        fn.selection_set.selections,
        ctx,
        "Widget",
        parent,
        "public.widgets",
        "TRUE",
        "",
        "t0",
        "root",
        None,
        None,
        False,
        1,
        select_parts,
        [],
        set(),
    )
    assert '"t0"."custom"' in select_parts[0]


def test_emit_agg_subqueries_recurses_with_source_json_key_and_virtual_target():
    parent = _table(1, "widgets", field_name="widgets")
    child = _table(2, "children", field_name="children")
    jm = _join(
        child,
        cardinality="one-to-many",
        source_json_key="cid",
        target_column="_name_",
    )
    ctx = CompilationContext(
        joins={("Widget", "children"): jm},
        physical_to_sql={(2, "name"): "name"},
        virtual_columns={2: {"_name_": "children"}},
    )
    fn = _field("{ widgets { children { name } } }")
    select_parts: list = []
    _emit_agg_subqueries(
        fn.selection_set.selections,
        ctx,
        "Widget",
        parent,
        "public.widgets",
        "TRUE",
        "",
        "t0",
        "root",
        None,
        None,
        False,
        1,
        select_parts,
        [],
        set(),
    )
    assert "AS JSON)>>'cid'" in select_parts[0]
    assert "VARCHAR 'children'" in select_parts[0]


def test_emit_agg_subqueries_skips_relationship_without_selection_set():
    parent = _table(1, "widgets")
    child = _table(2, "children")
    jm = _join(child)
    ctx = CompilationContext(joins={("Widget", "children"): jm})
    fn = _field("{ widgets { children } }")
    select_parts: list = []
    _emit_agg_subqueries(
        fn.selection_set.selections,
        ctx,
        "Widget",
        parent,
        "public.widgets",
        "TRUE",
        "",
        "t0",
        "root",
        None,
        None,
        False,
        1,
        select_parts,
        [],
        set(),
    )
    assert select_parts == []


# --- _extract_json_blob_kv -----------------------------------------------------


def test_extract_json_blob_kv_flat_fields():
    fn = _field("{ x { a b } }")
    pairs = _extract_json_blob_kv(fn.selection_set.selections, "t0.blob")
    assert pairs == [
        "KEY 'a' VALUE t0.blob->>'a'",
        "KEY 'b' VALUE t0.blob->>'b'",
    ]


def test_extract_json_blob_kv_nested_field():
    fn = _field("{ x { a { b } } }")
    pairs = _extract_json_blob_kv(fn.selection_set.selections, "t0.blob")
    assert pairs == ["KEY 'a' VALUE json_object(KEY 'b' VALUE t0.blob->'a'->>'b')"]


# --- _build_gql_selection -------------------------------------------------------


def test_build_gql_selection_flat():
    fn = _field("{ x { a b } }")
    out = _build_gql_selection("x", fn.selection_set)
    assert out == "x { a b }"


def test_build_gql_selection_nested():
    fn = _field("{ x { a { b } } }")
    out = _build_gql_selection("x", fn.selection_set)
    assert out == "x { a { b } }"


# --- _build_rel_json_kv / _build_rel_json_expr ----------------------------------


def test_build_rel_json_expr_many_to_one():
    parent = _table(2, "children")
    ctx = CompilationContext(physical_to_sql={(2, "name"): "name"})
    fn = _field("{ x { name } }")
    expr, counter = _build_rel_json_expr(
        fn.selection_set.selections,
        ctx,
        "Widget",
        parent,
        "t1",
        "public.children t1",
        '"t1"."id" = "t0"."x_id"',
        "many-to-one",
        None,
        False,
        1,
        set(),
    )
    assert expr.startswith("(SELECT json_object(")
    assert "LIMIT 1" in expr
    assert counter == 1


def test_build_rel_json_expr_one_to_many_no_limit():
    parent = _table(2, "children")
    ctx = CompilationContext(physical_to_sql={(2, "name"): "name"})
    fn = _field("{ x { name } }")
    expr, _ = _build_rel_json_expr(
        fn.selection_set.selections,
        ctx,
        "Widget",
        parent,
        "t1",
        "public.children t1",
        "TRUE",
        "one-to-many",
        None,
        False,
        1,
        set(),
    )
    assert expr.startswith("(SELECT json_agg(json_object(")
    assert "_sub" not in expr


def test_build_rel_json_expr_one_to_many_with_agg_limit():
    parent = _table(2, "children")
    ctx = CompilationContext(physical_to_sql={(2, "name"): "name"})
    fn = _field("{ x { name } }")
    expr, _ = _build_rel_json_expr(
        fn.selection_set.selections,
        ctx,
        "Widget",
        parent,
        "t1",
        "public.children t1",
        "TRUE",
        "one-to-many",
        3,
        False,
        1,
        set(),
    )
    assert "json_agg(_t)" in expr
    assert "LIMIT 3" in expr
    assert "_sub" in expr


def test_build_rel_json_kv_source_constant_and_json_key():
    child = _table(2, "children", field_name="children")
    jm = _join(child, source_constant="pets", cardinality="many-to-one")
    ctx = CompilationContext(joins={("Widget", "children"): jm})
    fn = _field("{ x { children { name } } }")
    kv, counter = _build_rel_json_kv(
        fn.selection_set.selections,
        ctx,
        "Widget",
        _table(1, "widgets"),
        "t0",
        False,
        1,
        set(),
        None,
    )
    assert counter == 2
    assert "'pets'" in kv[0]


def test_build_rel_json_kv_source_json_key():
    child = _table(2, "children", field_name="children")
    jm = _join(child, source_json_key="cid", cardinality="many-to-one")
    ctx = CompilationContext(joins={("Widget", "children"): jm})
    fn = _field("{ x { children { name } } }")
    kv, _ = _build_rel_json_kv(
        fn.selection_set.selections,
        ctx,
        "Widget",
        _table(1, "widgets"),
        "t0",
        False,
        1,
        set(),
        None,
    )
    assert "AS JSON)>>'cid'" in kv[0]


def test_build_rel_json_kv_source_expr_with_parent_src_val():
    child = _table(2, "children", field_name="children")
    jm = _join(
        child,
        source_expr='{alias}."custom"',
        source_column_type="varchar",
        cardinality="many-to-one",
    )
    ctx = CompilationContext(joins={("Widget", "children"): jm})
    fn = _field("{ x { children { name } } }")
    kv, _ = _build_rel_json_kv(
        fn.selection_set.selections,
        ctx,
        "Widget",
        _table(1, "widgets"),
        "t0",
        False,
        1,
        set(),
        None,
        parent_src_val="'literal-parent-val'",
    )
    assert "literal-parent-val" in kv[0]


def test_build_rel_json_kv_target_expr_and_virtual_column():
    child = _table(2, "children", field_name="children")
    jm = _join(child, target_expr='CONCAT({alias}."a", {alias}."b")', cardinality="many-to-one")
    ctx = CompilationContext(joins={("Widget", "children"): jm})
    fn = _field("{ x { children { name } } }")
    kv, _ = _build_rel_json_kv(
        fn.selection_set.selections,
        ctx,
        "Widget",
        _table(1, "widgets"),
        "t0",
        False,
        1,
        set(),
        None,
    )
    assert "CONCAT(" in kv[0]


def test_build_rel_json_kv_gql_json_blob_column():
    ctx = CompilationContext(gql_json_columns={(1, "meta")})
    fn = _field("{ x { meta { a } } }")
    kv, _ = _build_rel_json_kv(
        fn.selection_set.selections,
        ctx,
        "Widget",
        _table(1, "widgets"),
        "t0",
        False,
        1,
        set(),
        None,
    )
    assert "json_object(KEY 'a' VALUE" in kv[0]


def test_build_rel_json_kv_plain_scalar_column():
    ctx = CompilationContext(physical_to_sql={(1, "name"): "nm"})
    fn = _field("{ x { name } }")
    kv, _ = _build_rel_json_kv(
        fn.selection_set.selections,
        ctx,
        "Widget",
        _table(1, "widgets"),
        "t0",
        False,
        1,
        set(),
        None,
    )
    assert kv == ['KEY \'name\' VALUE "t0"."nm"']


# --- _collect_nested_columns -----------------------------------------------------


def test_collect_nested_columns_plain_left_join():
    # flat=True + no default_limit + no lateral-force args -> _use_agg is False,
    # so the else branch (plain LEFT JOIN) is taken instead of the agg-subquery path.
    child = _table(2, "children", field_name="children")
    jm = _join(child, cardinality="many-to-one")
    ctx = CompilationContext(
        joins={("Widget", "children"): jm}, physical_to_sql={(2, "name"): "name"}
    )
    fn = _field("{ x { children { name } } }")
    select_parts: list = []
    columns: list = []
    join_clauses: list = []
    counter, has_lateral = _collect_nested_columns(
        fn.selection_set.selections,
        "t0",
        "Widget",
        _table(1, "widgets"),
        "root",
        ctx,
        select_parts,
        columns,
        join_clauses,
        set(),
        1,
        False,
        ParamCollector(),
        None,
        flat=True,
    )
    assert counter == 2
    assert not has_lateral
    assert any("LEFT JOIN" in jc for jc in join_clauses)
    assert columns[0].field_name == "name"


def test_collect_nested_columns_lateral_via_default_limit():
    child = _table(2, "children", field_name="children")
    jm = _join(child, cardinality="one-to-many", default_limit=5)
    ctx = CompilationContext(
        joins={("Widget", "children"): jm}, physical_to_sql={(2, "name"): "name"}
    )
    fn = _field("{ x { children { name } } }")
    select_parts: list = []
    columns: list = []
    join_clauses: list = []
    counter, has_lateral = _collect_nested_columns(
        fn.selection_set.selections,
        "t0",
        "Widget",
        _table(1, "widgets"),
        "root",
        ctx,
        select_parts,
        columns,
        join_clauses,
        set(),
        1,
        False,
        ParamCollector(),
        None,
        flat=True,
    )
    assert has_lateral
    assert any("LATERAL" in jc for jc in join_clauses)


def test_collect_nested_columns_agg_subquery_path():
    child = _table(2, "children", field_name="children")
    jm = _join(child, cardinality="one-to-many")
    ctx = CompilationContext(
        joins={("Widget", "children"): jm}, physical_to_sql={(2, "name"): "name"}
    )
    fn = _field("{ x { children { name } } }")
    select_parts: list = []
    columns: list = []
    join_clauses: list = []
    counter, has_lateral = _collect_nested_columns(
        fn.selection_set.selections,
        "t0",
        "Widget",
        _table(1, "widgets"),
        "root",
        ctx,
        select_parts,
        columns,
        join_clauses,
        set(),
        1,
        False,
        ParamCollector(),
        None,
        flat=False,
    )
    assert not has_lateral
    assert any("ARRAY_AGG" in sp for sp in select_parts)
    assert columns[0].is_agg is True


def test_collect_nested_columns_source_constant_and_target_expr():
    child = _table(2, "children", field_name="children")
    jm = _join(
        child,
        cardinality="many-to-one",
        source_constant=7,
        target_expr='CONCAT({alias}."a", {alias}."b")',
    )
    ctx = CompilationContext(
        joins={("Widget", "children"): jm}, physical_to_sql={(2, "name"): "name"}
    )
    fn = _field("{ x { children { name } } }")
    join_clauses: list = []
    _collect_nested_columns(
        fn.selection_set.selections,
        "t0",
        "Widget",
        _table(1, "widgets"),
        "root",
        ctx,
        [],
        [],
        join_clauses,
        set(),
        1,
        False,
        ParamCollector(),
        None,
        flat=True,
    )
    assert any("CONCAT(" in jc and "7 = " in jc for jc in join_clauses)


def test_collect_nested_columns_virtual_source_column():
    child = _table(2, "children", field_name="children")
    jm = _join(child, cardinality="many-to-one", source_column="_name_")
    ctx = CompilationContext(
        joins={("Widget", "children"): jm},
        physical_to_sql={(2, "name"): "name"},
        virtual_columns={1: {"_name_": "widgets"}},
    )
    fn = _field("{ x { children { name } } }")
    join_clauses: list = []
    _collect_nested_columns(
        fn.selection_set.selections,
        "t0",
        "Widget",
        _table(1, "widgets"),
        "root",
        ctx,
        [],
        [],
        join_clauses,
        set(),
        1,
        False,
        ParamCollector(),
        None,
        flat=True,
    )
    assert any("VARCHAR 'widgets'" in jc for jc in join_clauses)


def test_collect_nested_columns_scalar_with_alias():
    ctx = CompilationContext(physical_to_sql={(1, "name"): "nm"})
    fn = _field("{ x { display: name } }")
    select_parts: list = []
    columns: list = []
    _collect_nested_columns(
        fn.selection_set.selections,
        "t0",
        "Widget",
        _table(1, "widgets"),
        "root",
        ctx,
        select_parts,
        columns,
        [],
        set(),
        1,
        False,
        ParamCollector(),
        None,
    )
    assert select_parts == ['"t0"."nm" AS "display"']
    assert columns[0].field_name == "display"


def test_collect_nested_columns_scalar_no_alias():
    ctx = CompilationContext(physical_to_sql={(1, "name"): "nm"})
    fn = _field("{ x { name } }")
    select_parts: list = []
    columns: list = []
    _collect_nested_columns(
        fn.selection_set.selections,
        "t0",
        "Widget",
        _table(1, "widgets"),
        "root",
        ctx,
        select_parts,
        columns,
        [],
        set(),
        1,
        False,
        ParamCollector(),
        None,
    )
    assert select_parts == ['"t0"."nm"']


def test_collect_nested_columns_virtual_column():
    ctx = CompilationContext(
        physical_to_sql={(1, "_name_"): "_name_"},
        virtual_columns={1: {"_name_": "widgets"}},
    )
    fn = _field("{ x { _name_ } }")
    select_parts: list = []
    columns: list = []
    _collect_nested_columns(
        fn.selection_set.selections,
        "t0",
        "Widget",
        _table(1, "widgets"),
        "root",
        ctx,
        select_parts,
        columns,
        [],
        set(),
        1,
        False,
        ParamCollector(),
        None,
    )
    assert select_parts == ["VARCHAR 'widgets'"]


def test_collect_nested_columns_gql_json_blob_expansion():
    ctx = CompilationContext(gql_json_columns={(1, "meta")})
    fn = _field("{ x { meta { a b } } }")
    select_parts: list = []
    columns: list = []
    _collect_nested_columns(
        fn.selection_set.selections,
        "t0",
        "Widget",
        _table(1, "widgets"),
        "root",
        ctx,
        select_parts,
        columns,
        [],
        set(),
        1,
        False,
        ParamCollector(),
        None,
    )
    assert select_parts == [
        '"t0"."meta"->>\'a\' AS "meta__a"',
        '"t0"."meta"->>\'b\' AS "meta__b"',
    ]
    assert len(columns) == 2


def test_collect_nested_columns_graphql_remote_undeclared_object_hydration():
    parent = TableMeta(
        table_id=1,
        field_name="widgets",
        type_name="Widget",
        source_id="src",
        catalog_name="src",
        schema_name="public",
        table_name="widgets",
        source_type="graphql_remote",
    )
    ctx = CompilationContext()
    fn = _field("{ x { extra { deep } } }")
    select_parts: list = []
    columns: list = []
    _collect_nested_columns(
        fn.selection_set.selections,
        "t0",
        "Widget",
        parent,
        "root",
        ctx,
        select_parts,
        columns,
        [],
        set(),
        1,
        False,
        ParamCollector(),
        None,
    )
    assert (1, "extra") in ctx.gql_json_columns
    assert ctx.gql_remote_extra_selections["widgets"]["extra"] == "extra { deep }"


def test_collect_nested_columns_nested_join_recursion_child_lateral_bubbles_up():
    grandchild = _table(3, "grandchildren", field_name="grandchildren")
    child = _table(2, "children", field_name="children")
    jm_child = _join(child, cardinality="many-to-one")
    jm_grandchild = _join(grandchild, cardinality="one-to-many", default_limit=2)
    ctx = CompilationContext(
        joins={
            ("Widget", "children"): jm_child,
            ("Widget", "grandchildren"): jm_grandchild,
        },
        physical_to_sql={(3, "name"): "name"},
    )
    fn = _field("{ x { children { grandchildren { name } } } }")
    select_parts: list = []
    columns: list = []
    join_clauses: list = []
    counter, has_lateral = _collect_nested_columns(
        fn.selection_set.selections,
        "t0",
        "Widget",
        _table(1, "widgets"),
        "root",
        ctx,
        select_parts,
        columns,
        join_clauses,
        set(),
        1,
        False,
        ParamCollector(),
        None,
        flat=True,
    )
    assert has_lateral
    assert counter == 3
