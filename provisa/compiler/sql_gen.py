# Copyright (c) 2026 Kenneth Stott
# Canary: a06089bd-b453-4daa-8692-7fcbd909b7b1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Walk validated GraphQL AST → PG-style SQL (REQ-009, REQ-066).

Single SQL statement per query field. No resolver chain, no N+1.
Double-quoted identifiers, $1-style positional parameters.
Table aliases (t0, t1, ...) used when JOINs are present.
"""

# Requirements: REQ-007, REQ-008, REQ-009, REQ-010, REQ-011, REQ-032, REQ-033, REQ-034, REQ-035, REQ-036, REQ-037, REQ-066, REQ-151, REQ-152, REQ-153, REQ-196, REQ-197, REQ-198, REQ-199, REQ-252, REQ-253, REQ-259, REQ-262, REQ-263, REQ-264, REQ-265, REQ-301, REQ-367, REQ-372, REQ-393, REQ-403, REQ-411, REQ-412, REQ-416, REQ-423, REQ-426, REQ-429, REQ-478

from __future__ import annotations

import os as _os
import re as _re

from typing import TYPE_CHECKING
from provisa.otel_compat import get_tracer as _get_tracer

if TYPE_CHECKING:
    pass

from graphql import (
    DocumentNode,
    FieldNode,
    OperationDefinitionNode,
    VariableDefinitionNode,
)

from provisa.compiler.params import ParamCollector
from provisa.cache.warm_tables import QueryCounter
from provisa.core.source_registry import TIME_TRAVEL_SOURCES

from provisa.compiler.sql_types import (
    ColumnRef,
    CompilationContext,
    CompiledQuery,
    JoinMeta,  # noqa: F401 — re-exported; many modules import JoinMeta from sql_gen
    TableMeta,  # noqa: F401 — re-exported; many modules import TableMeta from sql_gen
)
from provisa.compiler.sql_rewrite import (
    _join_column_expr,
    _q,
    _sql_str_literal,
    _table_ref,
)
from provisa.compiler.sql_where import (
    _VIRTUAL_COLS,
    _compile_order_by,
    _compile_where,
    _explicit_limit,
    _extract_value,
    _has_joins,
    _has_lateral_force_args,
)
from provisa.compiler.sql_selection import (
    _build_gql_selection,
    _build_rel_json_expr,
    _collect_nested_columns,
    _lateral_join,
)

_tracer = _get_tracer(__name__)


# Hard cap on rows returned when the caller supplies no explicit LIMIT.
# Resolved at query time from state.server_limits; falls back to env var then 10000.
def _get_default_row_limit() -> int:
    try:
        from provisa.api.app import state

        return int(
            _os.environ.get(
                "PROVISA_DEFAULT_ROW_LIMIT",
                str(state.server_limits.get("default_row_limit", 100)),
            )
        )
    except Exception:
        return int(_os.environ.get("PROVISA_DEFAULT_ROW_LIMIT", "100"))


# Module-level query counter for warm-table tracking (REQ-AD5)
query_counter = QueryCounter()


def _compile_root_field(  # REQ-009, REQ-011, REQ-032, REQ-033, REQ-034, REQ-035, REQ-036, REQ-151, REQ-152, REQ-153, REQ-262, REQ-264, REQ-265, REQ-300, REQ-301, REQ-372, REQ-403, REQ-478
    field_node: FieldNode,
    ctx: CompilationContext,
    variables: dict | None,
    use_catalog: bool = False,
    flat: bool = False,
) -> CompiledQuery:
    """Compile a single root query field to SQL."""
    root_name = field_node.alias.value if field_node.alias else field_node.name.value
    table = ctx.tables[field_node.name.value]
    collector = ParamCollector()
    sources: set[str] = {table.source_id}

    use_aliases = _has_joins(field_node, ctx, table.type_name)
    root_alias: str | None = "t0" if use_aliases else None
    alias_counter = 1
    has_lateral_ops_joins = False

    # Collect SELECT columns and JOINs
    select_parts: list[str] = []
    columns: list[ColumnRef] = []
    join_clauses: list[str] = []

    assert field_node.selection_set is not None
    for sel in field_node.selection_set.selections:
        if not isinstance(sel, FieldNode):
            continue

        sel_name = sel.name.value
        join_key = (table.type_name, sel_name)

        if join_key in ctx.joins:
            # Relationship field → JOIN
            assert root_alias is not None  # joins only exist when use_aliases=True
            join_meta = ctx.joins[join_key]
            join_alias = f"t{alias_counter}"
            alias_counter += 1
            sources.add(join_meta.target.source_id)

            if join_meta.source_expr is not None:
                src_expr = join_meta.source_expr.replace("{alias}", _q(root_alias))
            elif join_meta.source_constant is not None:
                src_expr = (
                    _sql_str_literal(join_meta.source_constant)
                    if isinstance(join_meta.source_constant, str)
                    else str(join_meta.source_constant)
                )
            elif join_meta.source_column in _VIRTUAL_COLS:
                _svc = (ctx.virtual_columns.get(table.table_id) or {}).get(
                    join_meta.source_column, ""
                )
                src_expr = _sql_str_literal(_svc)
            elif join_meta.source_json_key:
                src_expr = f"CAST({_q(root_alias)}.{_q(join_meta.source_column)} AS JSON)->>'{join_meta.source_json_key}'"
            else:
                src_expr = _join_column_expr(
                    root_alias,
                    join_meta.source_column,
                    join_meta.source_column_type,
                    join_meta.target_column_type,
                )
            if join_meta.target_expr is not None:
                tgt_expr = join_meta.target_expr.replace("{alias}", _q(join_alias))
            elif join_meta.target_column in _VIRTUAL_COLS:
                _tvc = (ctx.virtual_columns.get(join_meta.target.table_id) or {}).get(
                    join_meta.target_column, ""
                )
                tgt_expr = _sql_str_literal(_tvc)
            else:
                tgt_expr = _join_column_expr(
                    join_alias,
                    join_meta.target_column,
                    join_meta.target_column_type,
                    join_meta.source_column_type,
                )
            _use_agg = not flat and not _has_lateral_force_args(sel)
            if (
                join_meta.default_limit is not None or _has_lateral_force_args(sel)
            ) and not _use_agg:
                if join_meta.default_limit is not None:
                    has_lateral_ops_joins = True
                join_clauses.append(
                    _lateral_join(
                        sel,
                        join_meta,
                        join_alias,
                        src_expr,
                        collector,
                        variables,
                        use_catalog,
                        ctx.exposed_to_physical,
                    )
                )
                if sel.selection_set:
                    alias_counter, _child_lateral = _collect_nested_columns(
                        sel.selection_set.selections,
                        join_alias,
                        join_meta.target.type_name,
                        join_meta.target,
                        sel_name,
                        ctx,
                        select_parts,
                        columns,
                        join_clauses,
                        sources,
                        alias_counter,
                        use_catalog,
                        collector,
                        variables,
                        cardinality=join_meta.cardinality,
                        flat=flat,
                    )
                    has_lateral_ops_joins |= _child_lateral
            elif _use_agg and sel.selection_set:
                _agg_limit = _explicit_limit(sel, variables) or join_meta.default_limit
                _from_clause = f"{_table_ref(join_meta.target, use_catalog)} {_q(join_alias)}"
                _where_expr = f"{tgt_expr} = {src_expr}"
                _rel_key = sel.alias.value if sel.alias else sel_name
                _pv = (
                    join_meta.child_src_val
                    if join_meta.child_src_val is not None
                    else (src_expr if join_meta.source_column_type != "integer" else None)
                )
                json_expr, alias_counter = _build_rel_json_expr(
                    sel.selection_set.selections,
                    ctx,
                    join_meta.target.type_name,
                    join_meta.target,
                    join_alias,
                    _from_clause,
                    _where_expr,
                    join_meta.cardinality,
                    _agg_limit,
                    use_catalog,
                    alias_counter,
                    sources,
                    variables,
                    parent_src_val=_pv,
                )
                select_parts.append(f"{json_expr} AS {_q(_rel_key)}")
                columns.append(
                    ColumnRef(
                        alias=join_alias,
                        column=_rel_key,
                        field_name=_rel_key,
                        nested_in=None,
                        cardinality=join_meta.cardinality,
                        is_agg=True,
                    )
                )
            else:
                _join_on = f"{src_expr} = {tgt_expr}"
                join_clauses.append(
                    f"LEFT JOIN {_table_ref(join_meta.target, use_catalog)}"
                    f" {_q(join_alias)}"
                    f" ON {_join_on}"
                )
                if sel.selection_set:
                    alias_counter, _child_lateral = _collect_nested_columns(
                        sel.selection_set.selections,
                        join_alias,
                        join_meta.target.type_name,
                        join_meta.target,
                        sel_name,
                        ctx,
                        select_parts,
                        columns,
                        join_clauses,
                        sources,
                        alias_counter,
                        use_catalog,
                        collector,
                        variables,
                        cardinality=join_meta.cardinality,
                        flat=flat,
                    )
                    has_lateral_ops_joins |= _child_lateral
        else:
            # Undeclared OBJECT field on a graphql_remote table — hydrate from remote endpoint
            if (
                sel.selection_set
                and table.source_type == "graphql_remote"
                and (table.table_id, sel_name) not in ctx.gql_json_columns
            ):
                _gql_sel = _build_gql_selection(sel_name, sel.selection_set)
                ctx.gql_json_columns.add((table.table_id, sel_name))
                ctx.gql_remote_extra_selections.setdefault(table.table_name, {})[sel_name] = (
                    _gql_sel
                )
            # GQL OBJECT column stored as JSON — expand sub-selections recursively via -> / ->>
            if sel.selection_set and (table.table_id, sel_name) in ctx.gql_json_columns:

                def _emit_root_json_cols(
                    sels,
                    json_base: str,
                    col_prefix: str,
                    nesting: str,
                ) -> None:
                    for ss in sels:
                        if not isinstance(ss, FieldNode):
                            continue
                        sn = ss.name.value
                        sk = ss.alias.value if ss.alias else sn
                        if ss.selection_set:
                            _emit_root_json_cols(
                                ss.selection_set.selections,
                                f"{json_base}->'{sn}'",
                                f"{col_prefix}__{sn}",
                                f"{nesting}.{sn}",
                            )
                        else:
                            expr = f"{json_base}->>'{sn}'"
                            col_alias = f"{col_prefix}__{sn}"
                            select_parts.append(f"{expr} AS {_q(col_alias)}")
                            columns.append(
                                ColumnRef(
                                    alias=root_alias,
                                    column=col_alias,
                                    field_name=sk,
                                    nested_in=nesting,
                                    cardinality=None,
                                )
                            )

                if use_aliases:
                    assert root_alias is not None
                    base = f"{_q(root_alias)}.{_q(sel_name)}"
                else:
                    base = _q(sel_name)
                _emit_root_json_cols(sel.selection_set.selections, base, sel_name, sel_name)
                continue
            # Scalar field — check for JSON path extraction
            response_key = sel.alias.value if sel.alias else sel_name
            gql_field_name = response_key
            col_path = ctx.column_paths.get((table.table_id, sel_name))
            phys_name = ctx.exposed_to_physical.get((table.table_id, sel_name), sel_name)
            sql_name = ctx.physical_to_sql.get((table.table_id, phys_name), phys_name)
            if col_path:
                # path is "source_col.key1.key2" → PG JSON extraction, or just "key"
                # when the column is aliased (phys_name is the JSON source column).
                # Emits PG syntax; SQLGlot transpiles to the engine json_extract_scalar
                path_parts = col_path.split(".")
                if len(path_parts) == 1:
                    # Single-key path: phys_name is the JSON column, col_path is the key.
                    source_col = phys_name
                    keys = [col_path]
                else:
                    source_col = path_parts[0]
                    keys = path_parts[1:]
                if use_aliases:
                    assert root_alias is not None
                    expr = f"{_q(root_alias)}.{_q(source_col)}"
                else:
                    expr = _q(source_col)
                # Navigate with -> for intermediate keys, ->> for final (text extract)
                for i, key in enumerate(keys):
                    op = "->>" if i == len(keys) - 1 else "->"
                    expr = f"{expr}{op}'{key}'"
                if sel.alias:
                    expr = f"{expr} AS {_q(response_key)}"
                select_parts.append(expr)
            elif phys_name in _VIRTUAL_COLS:
                vval = (ctx.virtual_columns.get(table.table_id) or {}).get(phys_name, "")
                expr = _sql_str_literal(vval)
                select_parts.append(f"{expr} AS {_q(response_key)}")
            elif use_aliases:
                assert root_alias is not None
                col_expr = f"{_q(root_alias)}.{_q(sql_name)}"
                if sel.alias:
                    select_parts.append(f"{col_expr} AS {_q(response_key)}")
                else:
                    select_parts.append(col_expr)
            else:
                if sel.alias:
                    select_parts.append(f"{_q(sql_name)} AS {_q(response_key)}")
                else:
                    select_parts.append(_q(sql_name))
            columns.append(
                ColumnRef(
                    alias=root_alias if use_aliases else None,
                    column=sql_name,
                    field_name=gql_field_name,
                    nested_in=None,
                )
            )

    # FROM clause
    ref = _table_ref(table, use_catalog)
    if use_aliases:
        assert root_alias is not None
        from_clause = f"{ref} {_q(root_alias)}"
    else:
        from_clause = ref

    # Process arguments before building SQL so as_of can modify the FROM clause
    args = {}
    if field_node.arguments:
        for arg in field_node.arguments:
            args[arg.name.value] = _extract_value(arg.value, variables)

    # Time-travel: append FOR TIMESTAMP/VERSION AS OF to table ref in FROM clause (REQ-372)
    if "as_of" in args:
        if table.source_type not in TIME_TRAVEL_SOURCES:
            raise ValueError(
                f"as_of is not supported for source type {table.source_type!r}; "
                f"only iceberg and delta_lake sources support time-travel"
            )
        as_of_val = args["as_of"]
        # Numeric → version; string → timestamp
        try:
            version = int(as_of_val)
            time_travel_clause = f" FOR VERSION AS OF {version}"
        except (TypeError, ValueError):
            time_travel_clause = f" FOR TIMESTAMP AS OF TIMESTAMP '{as_of_val}'"
        if use_aliases:
            assert root_alias is not None
            from_clause = f"{ref}{time_travel_clause} {_q(root_alias)}"
        else:
            from_clause = f"{ref}{time_travel_clause}"

    # When ops LATERAL joins are present, wrap the base table in a subquery so that
    # the base row count is capped before the lateral Cartesian expansion.
    # Without this cap, the engine runs one full Iceberg scan per base row (no secondary index).
    result_limit: int | None = None
    if has_lateral_ops_joins:
        base_limit = int(args["limit"]) if "limit" in args else _get_default_row_limit()
        result_limit = base_limit if "limit" in args else None
        if use_aliases:
            assert root_alias is not None
            from_clause = f"(SELECT * FROM {ref} LIMIT {base_limit}) {_q(root_alias)}"
        else:
            from_clause = f"(SELECT * FROM {ref} LIMIT {base_limit})"

    # REQ-263a: statistical sampling — TABLESAMPLE BERNOULLI(<pct>) on the base table.
    # PG places TABLESAMPLE after the (optional) alias; SQLGlot transpiles per dialect.
    # Not compatible with time-travel or lateral op-join wrapping.
    if "sample" in args:
        if "as_of" in args or has_lateral_ops_joins:
            raise ValueError("sample cannot be combined with as_of or op-relationship joins")
        _pct = float(args["sample"])
        if not 0 < _pct <= 100:
            raise ValueError(f"sample must be a percentage in (0, 100], got {_pct}")
        from_clause = f"{from_clause} TABLESAMPLE BERNOULLI ({_pct})"

    sql = f"SELECT {', '.join(select_parts)} FROM {from_clause}"

    # JOIN clauses
    for join_clause in join_clauses:
        sql += f" {join_clause}"

    # DISTINCT ON — inject after SELECT keyword
    if "distinct_on" in args:
        distinct_cols = args["distinct_on"]
        if isinstance(distinct_cols, str):
            distinct_cols = [distinct_cols]
        _e2p_d = ctx.exposed_to_physical
        _tid_d = table.table_id
        parts_d = [
            _q(_e2p_d.get((_tid_d, c), c))
            if root_alias is None
            else f"{_q(root_alias)}.{_q(_e2p_d.get((_tid_d, c), c))}"
            for c in distinct_cols
        ]
        if table.source_type in ("postgresql", ""):
            # PostgreSQL supports DISTINCT ON natively.
            distinct_prefix = f"DISTINCT ON ({', '.join(parts_d)}) "
            sql = f"SELECT {distinct_prefix}{sql[len('SELECT ') :]}"
        else:
            # Non-PostgreSQL sources (the engine, Iceberg, etc.) do not support DISTINCT ON.
            # Wrap as ROW_NUMBER() window function to deduplicate by the distinct columns.
            partition_cols = ", ".join(parts_d)
            inner_alias = "__distinct_inner"
            rn_expr = f"ROW_NUMBER() OVER (PARTITION BY {partition_cols}) AS __rn"
            inner_sql = f"SELECT *, {rn_expr} FROM ({sql}) AS {_q(inner_alias)}"
            sql = f"SELECT * FROM ({inner_sql}) AS {_q('__distinct_dedup')} WHERE __rn = 1"

    # WHERE
    if "where" in args:
        _vvals = ctx.virtual_columns.get(table.table_id)
        where_sql = _compile_where(
            args["where"], collector, root_alias, _vvals, table.table_id, ctx.exposed_to_physical
        )
        sql += f" WHERE {where_sql}"

    # ORDER BY
    if "order_by" in args:
        order_by_val = args["order_by"]
        if isinstance(order_by_val, dict):
            order_by_val = [order_by_val]
        order_sql = _compile_order_by(
            order_by_val, root_alias, table.table_id, ctx.exposed_to_physical
        )
        sql += f" ORDER BY {order_sql}"

    # LIMIT / OFFSET
    # When ops LATERAL joins are present, SQL LIMIT would cut the expanded join rows,
    # not the base rows. result_limit is set above (base-table subquery) for the SQL path;
    # it's also used for Python-level truncation after grouping in the GraphQL path.
    if has_lateral_ops_joins:
        if result_limit is None and "limit" in args:
            result_limit = int(args["limit"])
        # Outer LIMIT is meaningless after lateral expansion; use a fixed cap
        sql += " LIMIT 25"
        if "offset" in args:
            sql += f" OFFSET {collector.add(int(args['offset']))}"
    else:
        # Always parameterized, never interpolated. No compile-time default LIMIT:
        # the row cap is applied by governance (stage2 resolve_row_cap) so it is
        # role-aware — FULL_RESULTS roles get no default row limit, others get the
        # configured default_row_limit. (The lateral-ops base guard above is a
        # Cartesian-explosion guard, not the governance cap, and is kept.)
        if "limit" in args:
            sql += f" LIMIT {collector.add(int(args['limit']))}"
        if "offset" in args:
            sql += f" OFFSET {collector.add(int(args['offset']))}"

    # Collect native filter args (any arg not handled by SQL compilation above)
    _STANDARD_ARGS = {"where", "order_by", "limit", "offset", "distinct_on", "as_of", "sample"}
    api_args = {k: v for k, v in args.items() if k not in _STANDARD_ARGS}

    # Inject _nf_-prefixed WHERE conditions so the SQL/CQL preview shows the filter.
    # nf_extractor strips these before the engine execution; endpoint.py uses api_args for the REST call.
    if api_args:
        nf_conditions = []
        for k, v in api_args.items():
            col = f"_nf_{k}"
            quoted_col = _q(col)
            if isinstance(v, bool):
                lit = "TRUE" if v else "FALSE"
            elif isinstance(v, (int, float)):
                lit = str(v)
            else:
                lit = "'" + str(v).replace("'", "''") + "'"
            nf_conditions.append(f"{quoted_col} = {lit}")
        nf_where = " AND ".join(nf_conditions)
        if " WHERE " in sql:
            sql += f" AND {nf_where}"
        else:
            sql += f" WHERE {nf_where}"

    return CompiledQuery(
        sql=sql,
        params=collector.params,
        root_field=root_name,
        canonical_field=field_node.name.value,
        columns=columns,
        sources=sources,
        api_args=api_args,
        result_limit=result_limit,
        gql_remote_extra_selections=ctx.gql_remote_extra_selections,
    )


def _sql_literal(
    val: object,  # object-ok: truly-any payload — SQL literal accepts any Python scalar
) -> str:
    """Convert a Python value to a SQL literal for VALUES injection."""
    if val is None:
        return "NULL"
    if isinstance(val, bool):
        return "TRUE" if val else "FALSE"
    if isinstance(val, int):
        return str(val)
    if isinstance(val, float):
        return str(val)
    if isinstance(val, str):
        escaped = val.replace("'", "''")
        return f"'{escaped}'"
    return f"'{val!s}'"


def rewrite_hot_joins(  # REQ-230, REQ-232
    compiled: CompiledQuery, hot_manager: object
) -> (
    CompiledQuery
):  # object-ok: circular import boundary — HotTableManager imported inside function body
    """Rewrite JOINs targeting hot tables to use VALUES-based CTEs.

    When a LEFT JOIN target is a hot-cached table, replace the table reference
    with a CTE containing the cached rows as VALUES. This works cross-source
    since the data travels as constants in the query.
    """
    from provisa.cache.hot_tables import HotTableManager

    assert isinstance(hot_manager, HotTableManager)

    sql = compiled.sql
    ctes: list[str] = []

    # Match LEFT JOIN patterns: LEFT JOIN "schema"."table" "alias" ON ...
    # or LEFT JOIN "catalog"."schema"."table" "alias" ON ...
    join_pattern = _re.compile(
        r"LEFT JOIN\s+"
        r'(?:"[^"]+"\.)?'
        r'"[^"]+"\.'
        r'"([^"]+)"'  # table name in last segment
        r'\s+"([^"]+)"'  # alias
        r"\s+ON\s+(.+?)(?=\s+(?:LEFT JOIN|WHERE|ORDER BY|LIMIT|OFFSET)\b|\Z)",
        _re.IGNORECASE,
    )

    for match in reversed(list(join_pattern.finditer(sql))):
        table_name = match.group(1)
        alias = match.group(2)
        on_clause = match.group(3)

        if not hot_manager.is_hot(table_name):
            continue

        entry = hot_manager.get_entry(table_name)
        if entry is None or not entry.rows:
            continue

        # Build VALUES rows
        cte_name = f"_hot_{table_name}"
        col_names = entry.column_names

        value_rows = []
        for row in entry.rows:
            vals = [_sql_literal(row.get(c)) for c in col_names]
            value_rows.append(f"({', '.join(vals)})")

        col_defs = ", ".join(f'"{c}"' for c in col_names)
        col_suffix = f"({col_defs})" if col_defs else ""
        cte_sql = f"{cte_name}{col_suffix} AS (VALUES {', '.join(value_rows)})"
        ctes.append(cte_sql)

        # Replace the JOIN target with the CTE name
        new_join = f'LEFT JOIN "{cte_name}" "{alias}" ON {on_clause}'
        sql = sql[: match.start()] + new_join + sql[match.end() :]

    if ctes:
        new_ctes_sql = ", ".join(ctes)
        _with_re = _re.compile(r"^\s*WITH\s+", _re.IGNORECASE)
        if _with_re.match(sql):
            sql = _with_re.sub(f"WITH {new_ctes_sql}, ", sql)
        else:
            sql = f"WITH {new_ctes_sql} " + sql

    if sql != compiled.sql:
        return CompiledQuery(
            sql=sql,
            params=compiled.params,
            root_field=compiled.root_field,
            canonical_field=compiled.canonical_field,
            columns=compiled.columns,
            sources=compiled.sources,
        )
    return compiled


def _extract_node_selections(field_node: FieldNode) -> list:
    """Extract selections from edges.node in a connection field."""
    if not field_node.selection_set:
        return []
    for sel in field_node.selection_set.selections:
        if isinstance(sel, FieldNode) and sel.name.value == "edges":
            if sel.selection_set:
                for edge_sel in sel.selection_set.selections:
                    if isinstance(edge_sel, FieldNode) and edge_sel.name.value == "node":
                        if edge_sel.selection_set:
                            return list(edge_sel.selection_set.selections)
    return []


def _compile_connection_field(  # REQ-218
    field_node: FieldNode,
    ctx: CompilationContext,
    variables: dict | None,
    use_catalog: bool = False,
) -> CompiledQuery:
    """Compile a _connection root query field to SQL with cursor pagination."""
    from provisa.compiler.cursor import apply_cursor_pagination, extract_sort_columns, reverse_order

    root_name = field_node.alias.value if field_node.alias else field_node.name.value
    table = ctx.tables[field_node.name.value]
    collector = ParamCollector()
    sources: set[str] = {table.source_id}

    select_parts: list[str] = []
    columns: list[ColumnRef] = []
    for sel in _extract_node_selections(field_node):
        if not isinstance(sel, FieldNode):
            continue
        sel_name = sel.name.value
        phys_name = ctx.exposed_to_physical.get((table.table_id, sel_name), sel_name)
        select_parts.append(_q(phys_name))
        columns.append(ColumnRef(None, phys_name, sel_name, None))

    if not select_parts:
        select_parts.append("1")

    ref = _table_ref(table, use_catalog)
    args = {}
    if field_node.arguments:
        for arg in field_node.arguments:
            args[arg.name.value] = _extract_value(arg.value, variables)

    sort_columns = extract_sort_columns(args)
    for sc in sort_columns:
        if sc not in [c.field_name for c in columns]:
            select_parts.append(_q(sc))
            columns.append(ColumnRef(None, sc, sc, None))

    sql = f"SELECT {', '.join(select_parts)} FROM {ref}"

    where_parts: list[str] = []
    if "where" in args:
        _conn_vvals = ctx.virtual_columns.get(table.table_id)
        where_parts.append(
            _compile_where(
                args["where"], collector, None, _conn_vvals, table.table_id, ctx.exposed_to_physical
            )
        )

    cursor_where, effective_limit, is_backward = apply_cursor_pagination(
        args,
        sort_columns,
        collector,
        None,
    )
    if cursor_where:
        where_parts.append(cursor_where)
    if where_parts:
        sql += f" WHERE {' AND '.join(where_parts)}"

    if "order_by" in args:
        order_by_val = args["order_by"]
        if isinstance(order_by_val, dict):
            order_by_val = [order_by_val]
        order_sql = _compile_order_by(order_by_val, None, table.table_id, ctx.exposed_to_physical)
        if is_backward:
            order_sql = reverse_order(order_sql)
        sql += f" ORDER BY {order_sql}"
    else:
        direction = "DESC" if is_backward else "ASC"
        sql += f' ORDER BY "id" {direction}'

    page_size = args.get("first") or args.get("last")
    if effective_limit is not None:
        sql += f" LIMIT {collector.add(int(effective_limit))}"

    return CompiledQuery(
        sql=sql,
        params=collector.params,
        root_field=root_name,
        canonical_field=field_node.name.value,
        columns=columns,
        sources=sources,
        is_connection=True,
        is_backward=is_backward,
        sort_columns=sort_columns,
        page_size=int(page_size) if page_size is not None else None,
        has_cursor=("after" in args or "before" in args),
    )


def compile_query(  # REQ-007, REQ-009, REQ-010, REQ-011, REQ-262, REQ-263, REQ-266, REQ-300
    document: DocumentNode,
    ctx: CompilationContext,
    variables: dict | None = None,
    use_catalog: bool = False,
    flat: bool = False,
) -> list[CompiledQuery]:
    """Compile a validated GraphQL document to SQL queries.

    Args:
        document: Validated GraphQL DocumentNode.
        ctx: Compilation context mapping GraphQL names to physical metadata.
        variables: Optional GraphQL variable values.
        use_catalog: If True, emit catalog-qualified table names (for the engine).

    Returns one CompiledQuery per root query field in the document.
    """
    results: list[CompiledQuery] = []

    for definition in document.definitions:
        if not isinstance(definition, OperationDefinitionNode):
            continue
        # Merge variable defaults (from operation definition) under provided values.
        # Defaults apply only when the variable is absent from the caller's dict.
        effective_variables: dict | None = variables
        if definition.variable_definitions:
            defaults: dict = {}
            for vd in definition.variable_definitions:
                if isinstance(vd, VariableDefinitionNode) and vd.default_value is not None:
                    var_name = vd.variable.name.value
                    if variables is None or var_name not in variables:
                        defaults[var_name] = _extract_value(vd.default_value, None)
            if defaults:
                effective_variables = {**(variables or {}), **defaults}
        variables = effective_variables
        for sel in definition.selection_set.selections:
            if isinstance(sel, FieldNode):
                field_name = sel.name.value
                if field_name not in ctx.tables:
                    raise ValueError(f"Unknown root query field: {field_name!r}")
                with _tracer.start_as_current_span("compiler.compile_query") as span:
                    from provisa.compiler.aggregates import (
                        _compile_aggregate_field,
                        _compile_group_by_field,
                    )

                    span.set_attribute("graphql.field", field_name)
                    if field_name.endswith("_aggregate") or field_name.endswith("Aggregate"):
                        compiled = _compile_aggregate_field(
                            sel,
                            ctx,
                            variables,
                            use_catalog,
                        )
                    elif field_name.endswith("_group_by") or field_name.endswith("GroupBy"):
                        compiled = _compile_group_by_field(
                            sel,
                            ctx,
                            variables,
                            use_catalog,
                        )
                    elif field_name.endswith("_connection"):
                        compiled = _compile_connection_field(
                            sel,
                            ctx,
                            variables,
                            use_catalog,
                        )
                    else:
                        compiled = _compile_root_field(
                            sel,
                            ctx,
                            variables,
                            use_catalog,
                            flat=flat,
                        )
                    span.set_attribute("db.statement", compiled.sql[:1000])
                # Track source tables for warm-table promotion (REQ-AD5)
                table_meta = ctx.tables.get(field_name)
                if table_meta:
                    fqn = (
                        f'"{table_meta.catalog_name}"'
                        f'."{table_meta.schema_name}"'
                        f'."{table_meta.table_name}"'
                    )
                    query_counter.increment(fqn)
                results.append(compiled)

    return results
