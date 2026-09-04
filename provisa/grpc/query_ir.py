# Copyright (c) 2026 Kenneth Stott
# Canary: 3dd08557-1e8d-4234-82cb-3dcf4dd6e0fb
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Lower a gRPC table request directly to the query IR (a semantic SELECT).

Shared by the native gRPC servicer and the HTTP gRPC proxy so both follow the one pipeline every
transport uses — query language → IR → governed IR → plan → physical — and gRPC never round-trips
through GraphQL.
"""

from __future__ import annotations

from typing import Any

from provisa.compiler.aggregate_gen import _is_comparable, _is_numeric
from provisa.compiler.naming import active_gql_convention, apply_gql_name
from provisa.compiler.sql_gen import _q
from provisa.compiler.sql_rewrite import _semantic_table_ref


def _find_table_meta(ctx: Any, type_name: str) -> Any | None:
    """Match ``type_name`` to a ``TableMeta`` case/separator-insensitively (proto collapses the
    domain separator: ``PS__Inquiries`` → ``PsInquiries``). Shared by every grpc_table_to_* lowering
    so gRPC's request-type lookup has exactly one matching rule."""

    def _n(s: str) -> str:
        return s.replace("_", "").lower()

    return next((m for m in ctx.tables.values() if _n(m.type_name) == _n(type_name)), None)


def grpc_table_to_semantic_sql(ctx: Any, type_name: str, limit: int) -> str | None:
    """Semantic SELECT over the table matching ``type_name``, or None if none matches. proto collapses
    the domain separator (``PS__Inquiries`` → ``PsInquiries``), so match case/separator-insensitively."""
    meta = _find_table_meta(ctx, type_name)
    if meta is None:
        return None
    cols = ", ".join(_q(c) for c, _t in ctx.aggregate_columns.get(meta.table_id, [])) or "*"
    sql = f"SELECT {cols} FROM {_semantic_table_ref(meta)}"
    return f"{sql} LIMIT {int(limit)}" if limit and limit > 0 else sql


def _aggregate_field_name(field_name: str) -> str:
    """The GraphQL root field name schema_gen's ``_build_aggregate_query_field`` exposes for this
    table (REQ-1359), so gRPC's synthesized query text targets exactly what the schema built."""
    if active_gql_convention() == "apollo_graphql":
        return f"{field_name}Aggregate"
    return f"{field_name}_aggregate"


def _group_by_field_name(field_name: str) -> str:
    """The GraphQL root field name schema_gen's ``_build_group_by_query_field`` exposes for this
    table (REQ-1359)."""
    if active_gql_convention() == "apollo_graphql":
        return f"{field_name}GroupBy"
    return f"{field_name}_group_by"


AGG_FUNCS = ("count", "sum", "avg", "stddev", "variance", "min", "max")


def _agg_fields_selection(ctx: Any, table_id: int, funcs: list[str] | None = None) -> str:
    """``{ count sum { ... } avg { ... } stddev { ... } variance { ... } min { ... } max { ... } }``
    selection text, only including sub-selections the schema actually exposes for this table —
    mirrors ``build_agg_fields_type``'s numeric/comparable classification (REQ-196), reused rather
    than reimplemented.

    ``funcs`` restricts the selection to a caller-chosen subset (REQ-1361), matching the
    ``aggregate=count,sum`` function filter JSON:API/REST already support — None/empty means
    every function the schema exposes for this table."""
    cols = ctx.aggregate_columns.get(table_id, [])
    numeric = [c for c, t in cols if _is_numeric(t)]
    comparable = [c for c, t in cols if _is_comparable(t)]
    want = set(funcs) if funcs else None
    parts = []
    if want is None or "count" in want:
        parts.append("count")
    if numeric:
        numeric_sel = "{ " + " ".join(numeric) + " }"
        for fn in ("sum", "avg", "stddev", "variance"):
            if want is None or fn in want:
                parts.append(f"{fn} {numeric_sel}")
    if comparable:
        comparable_sel = "{ " + " ".join(comparable) + " }"
        for fn in ("min", "max"):
            if want is None or fn in want:
                parts.append(f"{fn} {comparable_sel}")
    if not parts:
        parts.append("count")
    return "{ " + " ".join(parts) + " }"


def split_agg_columns(columns: list[Any], row: tuple) -> tuple[dict, dict]:
    """Split a compiled aggregate result row into (top-level scalars, {func: {col: val}}) using
    the ColumnRef.nested_in metadata compile_query already attaches — "count" is nested_in ==
    the aggregate key alone (no dot); "sum"/"avg"/etc are nested_in == "{agg_key}.{func}" (plain
    _aggregate) or "aggregate.{func}" (_group_by's nested aggregate block). Either way the last
    dot-segment is the function name, so one split rule covers both compiled shapes. Shared by
    the gRPC servicer (proto message construction) and the NL executor's gRPC result preview
    (REQ-1359) so both shape aggregate rows identically."""
    from provisa.executor.serialize import _convert_value

    top: dict[str, Any] = {}
    nested: dict[str, dict[str, Any]] = {}
    for col_ref, val in zip(columns, row):
        if val is None:
            continue
        val = _convert_value(val)
        parts = col_ref.nested_in.split(".")
        if len(parts) == 1:
            top[col_ref.column] = val
        else:
            nested.setdefault(parts[-1], {})[col_ref.column] = val
    return top, nested


def split_group_by_columns(columns: list[Any]) -> tuple[list[Any], list[int], list[Any], list[int]]:
    """Partition compiled group-by columns into (group_key_cols, group_key_idx, agg_cols, agg_idx).
    ColumnRef is an unhashable plain dataclass, so columns are split by index rather than by
    dict-keying on it. Shared by the gRPC servicer and the NL executor's gRPC result preview."""
    group_key_idx = [i for i, c in enumerate(columns) if c.nested_in == "groupKey"]
    agg_idx = [i for i, c in enumerate(columns) if c.nested_in != "groupKey"]
    return (
        [columns[i] for i in group_key_idx],
        group_key_idx,
        [columns[i] for i in agg_idx],
        agg_idx,
    )


def grpc_table_to_aggregate_graphql_text(
    ctx: Any, type_name: str, funcs: list[str] | None = None
) -> str | None:
    """GraphQL query text for ``Query{Type}Aggregate`` (REQ-1359): targets the same
    ``{field}_aggregate`` root field JSON:API/REST synthesize, so gRPC runs the identical
    parse_query/compile_query pipeline instead of a third, divergent aggregate implementation.

    ``funcs`` restricts to a caller-chosen subset of aggregate functions (REQ-1361)."""
    meta = _find_table_meta(ctx, type_name)
    if meta is None:
        return None
    agg_field = _aggregate_field_name(meta.field_name)
    selection = _agg_fields_selection(ctx, meta.table_id, funcs)
    # {Type}Aggregate nests its functions under an "aggregate" sub-field (build_aggregate_types
    # in aggregate_gen.py: {"aggregate": ..., "nodes": ...}) — the compiler's
    # _collect_agg_aliases looks for a selection literally named "aggregate", so the synthesized
    # text must nest the same way _group_by's synthesized text already does.
    return f"{{ {agg_field} {{ aggregate {selection} }} }}"


def grpc_relation_scalars(ctx: Any, type_name: str, rel_field: str) -> list[str]:
    """Scalar column names of the related table for a many-to-one relationship field on
    ``type_name`` (REQ-1405), sourced from ``ctx.joins`` rather than GraphQL schema
    introspection — query_ir has no schema, only the compiler context every transport shares."""
    join_meta = ctx.joins.get((type_name, rel_field))
    if join_meta is None or join_meta.cardinality != "many-to-one":
        return []
    return [c for c, _t in ctx.aggregate_columns.get(join_meta.target.table_id, [])]


def _insert_include_path(
    ctx: Any, table_id: int, type_name: str, tree: dict[str, Any], segments: list[str]
) -> None:
    """Insert one dot-path's segments into a nested selection tree, recursing through
    many-to-one relations at any depth (REQ-1405/REQ-1408). A leaf ``None`` value marks a
    selected scalar; a ``dict`` value marks a relation with its own nested selection. A
    relation segment with no remaining scalar/relation descendants is pruned so an unresolvable
    tail (unknown column, non-many-to-one hop) drops the whole entry rather than emitting an
    empty ``{ }`` block."""
    head, *rest = segments
    join_meta = ctx.joins.get((type_name, head))
    if join_meta is not None and join_meta.cardinality == "many-to-one":
        child = tree.setdefault(head, {})
        if rest:
            _insert_include_path(
                ctx, join_meta.target.table_id, join_meta.target.type_name, child, rest
            )
        else:
            for column, _col_type in ctx.aggregate_columns.get(join_meta.target.table_id, []):
                child.setdefault(column, None)
        if not child:
            tree.pop(head, None)
        return
    if rest:
        return
    scalars = {c for c, _t in ctx.aggregate_columns.get(table_id, [])}
    if head in scalars:
        tree.setdefault(head, None)


def _render_include_tree(tree: dict[str, Any]) -> list[str]:
    fields = []
    for name, sub in tree.items():
        if sub is None:
            fields.append(apply_gql_name(name))
        else:
            fields.append(f"{name} {{ {' '.join(_render_include_tree(sub))} }}")
    return fields


def _include_node_fields(ctx: Any, meta: Any, include: list[str]) -> list[str]:
    """The ``nodes { ... }`` selection for a group-by query's ``include`` list (REQ-1408).

    Entries are either a relationship field (``user`` — every scalar of the related table, the
    REQ-1405 shape), a dot-path at any depth (``user.email``, ``assignment.employee.firstName``
    — recursing through many-to-one relations), or a base-table scalar (``status``). Dot-paths
    make the gRPC ``include`` accept the same projection REST/JSON:API express through
    ``?includeNodes=id,status,assignment.employee.firstName``, so one plan drives every surface.
    Naming no base scalar keeps all of them, which is what ``include_nodes=true`` alone means.
    Entries that resolve to neither a many-to-one relation chain nor a known column are skipped,
    same as an unknown relationship field has always been.
    """
    base_scalars = [c for c, _t in ctx.aggregate_columns.get(meta.table_id, [])]
    tree: dict[str, Any] = {}
    for entry in include:
        segments = [s for s in entry.split(".") if s]
        if segments:
            _insert_include_path(ctx, meta.table_id, meta.type_name, tree, segments)

    selected_base = [name for name, sub in tree.items() if sub is None]
    rel_fields = [
        f"{name} {{ {' '.join(_render_include_tree(sub))} }}"
        for name, sub in tree.items()
        if sub is not None
    ]
    fields = [apply_gql_name(c) for c in (selected_base or base_scalars)]
    fields.extend(rel_fields)
    return fields


def grpc_table_to_group_by_graphql_text(
    ctx: Any,
    type_name: str,
    by_columns: list[str],
    funcs: list[str] | None = None,
    include_nodes: bool = False,
    include: list[str] | None = None,
) -> str | None:
    """GraphQL query text for ``Query{Type}GroupBy`` (REQ-1359): targets the same
    ``{field}_group_by(by: [...])`` root field JSON:API/REST synthesize.

    ``funcs`` restricts to a caller-chosen subset of aggregate functions (REQ-1361).
    ``include_nodes`` (REQ-1401) appends a ``nodes { ... }`` sub-selection of the base table's
    scalar columns, mirroring JSON:API/REST's ``?includeNodes=true`` (provisa/api/jsonapi/
    generator.py::_build_group_by_graphql_query). ``include`` (REQ-1405/REQ-1408) selects what
    ``nodes`` projects — many-to-one relationship fields, ``rel.col`` dot-paths, and base-table
    scalars — mirroring JSON:API's ``?include=`` sideloading and REST's ``?includeNodes=``
    dot-path list; see ``_include_node_fields``."""
    meta = _find_table_meta(ctx, type_name)
    if meta is None:
        return None
    if not by_columns:
        return None
    gb_field = _group_by_field_name(meta.field_name)
    by_arg = "[" + ", ".join(apply_gql_name(c) for c in by_columns) + "]"
    agg_selection = _agg_fields_selection(ctx, meta.table_id, funcs)
    nodes_part = ""
    if include_nodes:
        node_fields = _include_node_fields(ctx, meta, include or [])
        nodes_part = f" nodes {{ {' '.join(node_fields)} }}"
    return f"{{ {gb_field}(by: {by_arg}) {{ groupKey aggregate {agg_selection}{nodes_part} }} }}"
