# Copyright (c) 2026 Kenneth Stott
# Canary: a06089bd-b453-4daa-8692-7fcbd909b7b1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Aggregate / group-by / connection-node SQL compilation (REQ-196–199, REQ-213).

Compiles _aggregate / _group_by GraphQL root fields and the nodes subquery.
Extracted from sql_gen.py. sql_gen helpers are reached via the sql_gen module
object (_sg) to avoid a load-time import cycle; leaf deps imported directly.
"""

# complexity-gate: allow-cc=47 reason="_compile_group_by_field relocated verbatim from sql_gen.py; its high CC is the GraphQL group-by field-shape dispatch (per-arg where/order/limit + agg-func assembly); per-branch split is separately-tracked debt"

from __future__ import annotations

from graphql import FieldNode  # noqa: F401

from provisa.compiler import sql_gen as _sg
from provisa.compiler.params import ParamCollector  # noqa: F401
from provisa.compiler.sql_rewrite import (
    _join_column_expr,
    _q,
    _sql_str_literal,
    _table_ref,
)
from provisa.compiler.sql_types import (
    ColumnRef,
    CompilationContext,
    CompiledQuery,
    JoinMeta,
    TableMeta,
)


def _collect_requested_agg_funcs(
    field_node: FieldNode,
) -> tuple[bool, dict[str, list[str]], bool]:
    """Parse the aggregate selection set to find which functions are requested.

    Returns: (has_count, cols_by_func, has_nodes) where cols_by_func maps each aggregate
    function name (sum/avg/stddev/variance/min/max) to its selected columns.
    """
    has_count = False
    cols_by_func: dict[str, list[str]] = {
        "sum": [],
        "avg": [],
        "stddev": [],
        "variance": [],
        "min": [],
        "max": [],
    }
    has_nodes = False

    def _result() -> tuple[bool, dict[str, list[str]], bool]:
        return has_count, cols_by_func, has_nodes

    if not field_node.selection_set:
        return _result()

    for sel in field_node.selection_set.selections:
        if not isinstance(sel, FieldNode):
            continue
        name = sel.name.value
        if name == "nodes":
            has_nodes = True
        elif name == "aggregate" and sel.selection_set:
            for agg_sel in sel.selection_set.selections:
                if not isinstance(agg_sel, FieldNode):
                    continue
                agg_name = agg_sel.name.value
                if agg_name == "count":
                    has_count = True
                elif agg_name in cols_by_func and agg_sel.selection_set:
                    cols_by_func[agg_name] = [
                        s.name.value
                        for s in agg_sel.selection_set.selections
                        if isinstance(s, FieldNode)
                    ]

    return _result()


def _collect_agg_aliases(
    field_node: FieldNode,
) -> tuple[str, dict[str, str], dict[str, dict[str, str]]]:
    """Return (agg_key, func_aliases, col_aliases) from the aggregate selection set."""
    agg_key = "aggregate"
    func_aliases: dict[str, str] = {}
    col_aliases: dict[str, dict[str, str]] = {}
    if not field_node.selection_set:
        return agg_key, func_aliases, col_aliases
    for sel in field_node.selection_set.selections:
        if not isinstance(sel, FieldNode) or sel.name.value != "aggregate":
            continue
        if sel.alias:
            agg_key = sel.alias.value
        if not sel.selection_set:
            continue
        for agg_sel in sel.selection_set.selections:
            if not isinstance(agg_sel, FieldNode):
                continue
            func = agg_sel.name.value
            if agg_sel.alias:
                func_aliases[func] = agg_sel.alias.value
            if agg_sel.selection_set:
                col_aliases[func] = {}
                for col_sel in agg_sel.selection_set.selections:
                    if isinstance(col_sel, FieldNode) and col_sel.alias:
                        col_aliases[func][col_sel.name.value] = col_sel.alias.value
    return agg_key, func_aliases, col_aliases


def _build_agg_func_parts(
    sql_func: str,
    func_name: str,
    cols: list[str],
    agg_key: str,
    func_aliases: dict[str, str],
    col_aliases: dict[str, dict[str, str]],
    table_id: int,
    exposed_to_physical: dict[tuple[int, str], str],
) -> tuple[list[str], list[ColumnRef]]:
    """Build SELECT parts and ColumnRefs for one aggregate function (SUM/AVG/MIN/MAX)."""
    select_parts: list[str] = []
    columns: list[ColumnRef] = []
    fn_key = func_aliases.get(func_name, func_name)
    for col_name in cols:
        phys = exposed_to_physical.get((table_id, col_name), col_name)
        field_name = col_aliases.get(func_name, {}).get(col_name, col_name)
        expr = f"{sql_func}({_q(phys)})"
        if field_name != col_name:
            expr += f" AS {_q(field_name)}"
        select_parts.append(expr)
        columns.append(
            ColumnRef(
                alias=None, column=phys, field_name=field_name, nested_in=f"{agg_key}.{fn_key}"
            )
        )
    return select_parts, columns


def _resolve_join_src_tgt(
    join_meta: JoinMeta,
    root_alias: str,
    join_alias: str,
    ctx: CompilationContext,
    parent_table: TableMeta,
) -> tuple[str, str]:
    """Resolve (source_expr, target_expr) for a relationship join. Mirrors _compile_root_field."""
    if join_meta.source_expr is not None:
        src_expr = join_meta.source_expr.replace("{alias}", _q(root_alias))
    elif join_meta.source_constant is not None:
        src_expr = (
            _sql_str_literal(join_meta.source_constant)
            if isinstance(join_meta.source_constant, str)
            else str(join_meta.source_constant)
        )
    elif join_meta.source_column in _sg._VIRTUAL_COLS:
        _svc = (ctx.virtual_columns.get(parent_table.table_id) or {}).get(
            join_meta.source_column, ""
        )
        src_expr = _sql_str_literal(_svc)
    elif join_meta.source_json_key:
        src_expr = (
            f"CAST({_q(root_alias)}.{_q(join_meta.source_column)} AS JSON)"
            f"->>'{join_meta.source_json_key}'"
        )
    else:
        src_expr = _join_column_expr(
            root_alias,
            join_meta.source_column,
            join_meta.source_column_type,
            join_meta.target_column_type,
        )
    if join_meta.target_expr is not None:
        tgt_expr = join_meta.target_expr.replace("{alias}", _q(join_alias))
    elif join_meta.target_column in _sg._VIRTUAL_COLS:
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
    return src_expr, tgt_expr


def _build_nodes_subquery(
    field_node: FieldNode,
    ref: str,
    args: dict,
    ctx: CompilationContext,
    table: TableMeta,
    variables: dict | None,
    use_catalog: bool,
    by_cols: list[str] | None = None,
    phys_by_cols: list[str] | None = None,
) -> tuple[str | None, list[ColumnRef] | None, list]:
    """Build the nodes sub-query SQL, columns, and params (or Nones when not requested).

    Scalar fields are emitted as plain columns; relationship fields are emitted as
    correlated JSON subqueries (same shape as _compile_root_field). When relationships
    are present, the nodes table is aliased so the subqueries can correlate to it.
    by_cols/phys_by_cols append group-by join-key columns (nested_in="__join_key__").
    """
    assert field_node.selection_set is not None
    nodes_sel: FieldNode | None = None
    for sel in field_node.selection_set.selections:
        if isinstance(sel, FieldNode) and sel.name.value == "nodes" and sel.selection_set:
            nodes_sel = sel
            break
    if nodes_sel is None or nodes_sel.selection_set is None:
        return None, None, []

    has_rel = any(
        isinstance(s, FieldNode) and (table.type_name, s.name.value) in ctx.joins
        for s in nodes_sel.selection_set.selections
    )
    root_alias: str | None = "t0" if has_rel else None
    alias_counter = 1
    sources: set[str] = {table.source_id}
    _vvals = ctx.virtual_columns.get(table.table_id)

    nodes_select_parts: list[str] = []
    nodes_cols: list[ColumnRef] = []

    def _scalar_sql(phys: str) -> str:
        return f"{_q(root_alias)}.{_q(phys)}" if root_alias else _q(phys)

    for node_sel in nodes_sel.selection_set.selections:
        if not isinstance(node_sel, FieldNode):
            continue
        sel_name = node_sel.name.value
        join_key = (table.type_name, sel_name)
        if join_key in ctx.joins and node_sel.selection_set:
            assert root_alias is not None
            join_meta = ctx.joins[join_key]
            join_alias = f"t{alias_counter}"
            alias_counter += 1
            sources.add(join_meta.target.source_id)
            src_expr, tgt_expr = _resolve_join_src_tgt(
                join_meta, root_alias, join_alias, ctx, table
            )
            _agg_limit = _sg._explicit_limit(node_sel, variables) or join_meta.default_limit
            _from_clause = f"{_table_ref(join_meta.target, use_catalog)} {_q(join_alias)}"
            _where_expr = f"{tgt_expr} = {src_expr}"
            _rel_key = node_sel.alias.value if node_sel.alias else sel_name
            _pv = (
                join_meta.child_src_val
                if join_meta.child_src_val is not None
                else (src_expr if join_meta.source_column_type != "integer" else None)
            )
            json_expr, alias_counter = _sg._build_rel_json_expr(
                node_sel.selection_set.selections,
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
            nodes_select_parts.append(f"{json_expr} AS {_q(_rel_key)}")
            nodes_cols.append(
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
            phys = ctx.exposed_to_physical.get((table.table_id, sel_name), sel_name)
            nodes_select_parts.append(_scalar_sql(phys))
            nodes_cols.append(
                ColumnRef(alias=root_alias, column=phys, field_name=sel_name, nested_in=None)
            )

    if by_cols and phys_by_cols:
        for col, phys in zip(by_cols, phys_by_cols):
            nodes_select_parts.append(_scalar_sql(phys))
            nodes_cols.append(
                ColumnRef(alias=root_alias, column=phys, field_name=col, nested_in="__join_key__")
            )

    if not nodes_select_parts:
        return None, None, []

    from_sql = f"{ref} {_q(root_alias)}" if root_alias else ref
    nodes_sql = f"SELECT {', '.join(nodes_select_parts)} FROM {from_sql}"
    nodes_params: list = []
    if "where" in args:
        nodes_collector = ParamCollector()
        nodes_where_sql = _sg._compile_where(
            args["where"],
            nodes_collector,
            root_alias,
            _vvals,
            table.table_id,
            ctx.exposed_to_physical,
        )
        nodes_sql += f" WHERE {nodes_where_sql}"
        nodes_params = nodes_collector.params
    return nodes_sql, nodes_cols, nodes_params


def _compile_having(
    having_obj: dict,
    collector: ParamCollector,
    table_id: int | None,
    exposed_to_physical: dict,
) -> str:
    """Compile a HavingExp input object to a SQL HAVING clause fragment.

    Maps aggregate function expressions:
      count: {gt: 3}         → COUNT(*) > $N
      sum: {amount: {gte: 1000}} → SUM("amount") >= $N
    """
    _SQL_FUNC = {
        "sum": "SUM",
        "avg": "AVG",
        "stddev": "STDDEV",
        "variance": "VARIANCE",
        "min": "MIN",
        "max": "MAX",
    }
    parts: list[str] = []
    for key, value in having_obj.items():
        if key == "count":
            parts.extend(_sg._compile_column_filter("COUNT(*)", value, collector))
        elif key in _SQL_FUNC and isinstance(value, dict):
            sql_func = _SQL_FUNC[key]
            for col_name, col_filter in value.items():
                phys = (
                    exposed_to_physical.get((table_id, col_name), col_name)
                    if table_id is not None
                    else col_name
                )
                expr = f"{sql_func}({_q(phys)})"
                parts.extend(_sg._compile_column_filter(expr, col_filter, collector))
    return " AND ".join(parts) if parts else "TRUE"


def _compile_group_by_field(  # REQ-196, REQ-197, REQ-213
    field_node: FieldNode,
    ctx: CompilationContext,
    variables: dict | None,
    use_catalog: bool = False,
) -> CompiledQuery:
    """Compile a _group_by root query field to SQL (REQ-654, REQ-655)."""
    root_name = field_node.alias.value if field_node.alias else field_node.name.value
    table = ctx.tables[field_node.name.value]
    collector = ParamCollector()
    sources: set[str] = {table.source_id}

    args: dict = {}
    if field_node.arguments:
        for arg in field_node.arguments:
            args[arg.name.value] = _sg._extract_value(arg.value, variables)

    by_cols: list[str] = args.get("by") or []
    if isinstance(by_cols, str):
        by_cols = [by_cols]
    phys_by_cols = [ctx.exposed_to_physical.get((table.table_id, col), col) for col in by_cols]

    # Parse aggregates sub-selection for requested functions and optional FILTER WHERE
    agg_filter_where: dict | None = None
    has_count = False
    cols_by_func: dict[str, list[str]] = {
        k: [] for k in ("sum", "avg", "stddev", "variance", "min", "max")
    }

    if field_node.selection_set:
        for row_sel in field_node.selection_set.selections:
            if not isinstance(row_sel, FieldNode):
                continue
            if row_sel.name.value == "aggregate":
                if row_sel.arguments:
                    for agg_arg in row_sel.arguments:
                        if agg_arg.name.value == "where":
                            agg_filter_where = _sg._extract_value(agg_arg.value, variables) or None  # type: ignore[assignment]
                if row_sel.selection_set:
                    for agg_sel in row_sel.selection_set.selections:
                        if not isinstance(agg_sel, FieldNode):
                            continue
                        agg_name = agg_sel.name.value
                        if agg_name == "count":
                            has_count = True
                        elif agg_name in cols_by_func and agg_sel.selection_set:
                            cols_by_func[agg_name] = [
                                s.name.value
                                for s in agg_sel.selection_set.selections
                                if isinstance(s, FieldNode)
                            ]

    # Build FILTER (WHERE ...) SQL fragment from aggregates.where arg (REQ-655)
    filter_sql = ""
    if agg_filter_where:
        _vvals = ctx.virtual_columns.get(table.table_id)
        filter_where_sql = _sg._compile_where(
            agg_filter_where, collector, None, _vvals, table.table_id, ctx.exposed_to_physical
        )
        filter_sql = f" FILTER (WHERE {filter_where_sql})"

    select_parts: list[str] = []
    columns: list[ColumnRef] = []

    for col, phys in zip(by_cols, phys_by_cols):
        select_parts.append(_q(phys))
        columns.append(ColumnRef(alias=None, column=phys, field_name=col, nested_in="groupKey"))

    if has_count:
        select_parts.append(f"COUNT(*){filter_sql}")
        columns.append(
            ColumnRef(alias=None, column="count", field_name="count", nested_in="aggregate")
        )

    for func_name, sql_func in (
        ("sum", "SUM"),
        ("avg", "AVG"),
        ("stddev", "STDDEV"),
        ("variance", "VARIANCE"),
        ("min", "MIN"),
        ("max", "MAX"),
    ):
        for col_name in cols_by_func[func_name]:
            phys = ctx.exposed_to_physical.get((table.table_id, col_name), col_name)
            select_parts.append(f"{sql_func}({_q(phys)}){filter_sql}")
            columns.append(
                ColumnRef(
                    alias=None,
                    column=phys,
                    field_name=col_name,
                    nested_in=f"aggregate.{func_name}",
                )
            )

    if not select_parts:
        select_parts = [_q(phys_by_cols[0])] if phys_by_cols else ["1"]

    ref = _table_ref(table, use_catalog)
    sql = f"SELECT {', '.join(select_parts)} FROM {ref}"

    _vvals = ctx.virtual_columns.get(table.table_id)
    if "where" in args and not agg_filter_where:
        where_sql = _sg._compile_where(
            args["where"], collector, None, _vvals, table.table_id, ctx.exposed_to_physical
        )
        sql += f" WHERE {where_sql}"
    elif "where" in args and agg_filter_where:
        # FILTER params already consumed above; need a fresh sub-collector for WHERE
        where_collector = ParamCollector()
        where_sql = _sg._compile_where(
            args["where"], where_collector, None, _vvals, table.table_id, ctx.exposed_to_physical
        )
        # Re-number params: append where params after filter params
        _filter_count = len(collector.params)
        renumbered = where_sql
        for i in range(len(where_collector.params), 0, -1):
            renumbered = renumbered.replace(f"${i}", f"${i + _filter_count}")
        for p in where_collector.params:
            collector._params.append(p)
        sql += f" WHERE {renumbered}"

    if phys_by_cols:
        sql += f" GROUP BY {', '.join(_q(c) for c in phys_by_cols)}"

    # REQ-655: HAVING clause
    if "having" in args:
        having_sql = _compile_having(
            args["having"], collector, table.table_id, ctx.exposed_to_physical
        )
        if having_sql and having_sql != "TRUE":
            sql += f" HAVING {having_sql}"

    if "order_by" in args:
        order_by_val = args["order_by"]
        if isinstance(order_by_val, dict):
            order_by_val = [order_by_val]
        ob_sql = _sg._compile_order_by(order_by_val, None, table.table_id, ctx.exposed_to_physical)
        if ob_sql:
            sql += f" ORDER BY {ob_sql}"

    if "limit" in args:
        sql += f" LIMIT {collector.add(int(args['limit']))}"
    if "offset" in args:
        sql += f" OFFSET {collector.add(int(args['offset']))}"

    # Build nodes subquery: plain SELECT with same WHERE, no GROUP BY.
    # Group-by join key columns are appended last with nested_in="__join_key__".
    nodes_sql, nodes_columns, nodes_params = _build_nodes_subquery(
        field_node,
        ref,
        args,
        ctx,
        table,
        variables,
        use_catalog,
        by_cols=by_cols,
        phys_by_cols=phys_by_cols,
    )

    return CompiledQuery(
        sql=sql,
        params=collector.params,
        root_field=root_name,
        canonical_field=field_node.name.value,
        columns=columns,
        sources=sources,
        is_group_by=True,
        group_by_columns=list(by_cols),
        nodes_sql=nodes_sql,
        nodes_columns=nodes_columns,
        nodes_params=nodes_params,
    )


def _compile_aggregate_field(  # REQ-196, REQ-197, REQ-198, REQ-199
    field_node: FieldNode,
    ctx: CompilationContext,
    variables: dict | None,
    use_catalog: bool = False,
) -> CompiledQuery:
    """Compile an _aggregate root query field to SQL."""
    root_name = field_node.alias.value if field_node.alias else field_node.name.value
    table = ctx.tables[field_node.name.value]
    collector = ParamCollector()
    sources: set[str] = {table.source_id}

    has_count, cols_by_func, has_nodes = _collect_requested_agg_funcs(field_node)

    agg_key, func_aliases, col_aliases = _collect_agg_aliases(field_node)

    # Build SELECT parts for aggregate functions
    select_parts: list[str] = []
    columns: list[ColumnRef] = []

    if has_count:
        select_parts.append("COUNT(*)")
        columns.append(ColumnRef(alias=None, column="count", field_name="count", nested_in=agg_key))

    for func_name, sql_func in (
        ("sum", "SUM"),
        ("avg", "AVG"),
        ("stddev", "STDDEV"),
        ("variance", "VARIANCE"),
        ("min", "MIN"),
        ("max", "MAX"),
    ):
        cols = cols_by_func[func_name]
        sp, cr = _build_agg_func_parts(
            sql_func,
            func_name,
            cols,
            agg_key,
            func_aliases,
            col_aliases,
            table.table_id,
            ctx.exposed_to_physical,
        )
        select_parts.extend(sp)
        columns.extend(cr)

    if not select_parts:
        select_parts.append("1")

    ref = _table_ref(table, use_catalog)
    sql = f"SELECT {', '.join(select_parts)} FROM {ref}"

    # Process arguments (where)
    args = {}
    if field_node.arguments:
        for arg in field_node.arguments:
            args[arg.name.value] = _sg._extract_value(arg.value, variables)

    _agg_vvals = ctx.virtual_columns.get(table.table_id)
    if "where" in args:
        where_sql = _sg._compile_where(
            args["where"], collector, None, _agg_vvals, table.table_id, ctx.exposed_to_physical
        )
        sql += f" WHERE {where_sql}"

    # Build nodes SQL: plain SELECT with same WHERE, no aggregate functions
    nodes_sql: str | None = None
    nodes_columns: list[ColumnRef] | None = None
    nodes_params: list = []
    if has_nodes:
        nodes_sql, nodes_columns, nodes_params = _build_nodes_subquery(
            field_node, ref, args, ctx, table, variables, use_catalog
        )

    return CompiledQuery(
        sql=sql,
        params=collector.params,
        root_field=root_name,
        canonical_field=field_node.name.value,
        columns=columns,
        sources=sources,
        nodes_sql=nodes_sql,
        nodes_columns=nodes_columns,
        nodes_params=nodes_params,
        agg_alias=agg_key,
    )
