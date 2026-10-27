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
Extracted from sql_gen.py. WHERE/ORDER BY helpers come from sql_where (_sw) and
nested-JSON builders from sql_selection (_ssel); leaf deps imported directly.
"""

# complexity-gate: allow-cc=47 reason="_compile_group_by_field relocated verbatim from sql_gen.py; its high CC is the GraphQL group-by field-shape dispatch (per-arg where/order/limit + agg-func assembly); per-branch split is separately-tracked debt"

from __future__ import annotations

from graphql import FieldNode  # noqa: F401

from provisa.compiler import sql_where as _sw
from provisa.compiler import sql_selection as _ssel
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
    StatementSources,
    TableMeta,
)


def _collect_requested_agg_funcs(
    field_node: FieldNode,
) -> tuple[bool, dict[str, list[str]], list[str], bool]:
    """Parse the aggregate selection set to find which functions are requested.

    Returns: (has_count, cols_by_func, metric_names, has_nodes) where cols_by_func maps
    each aggregate function name (sum/avg/stddev/variance/min/max) to its selected columns
    and metric_names lists the metrics block's selected metrics (REQ-1319).
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
    metric_names: list[str] = []
    has_nodes = False

    def _result() -> tuple[bool, dict[str, list[str]], list[str], bool]:
        return has_count, cols_by_func, metric_names, has_nodes

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
                elif agg_name == "metrics" and agg_sel.selection_set:  # REQ-1319
                    metric_names = [
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
    elif join_meta.source_column in _sw._VIRTUAL_COLS:
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
    elif join_meta.target_column in _sw._VIRTUAL_COLS:
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


#: An _aggregate / _group_by field's arguments that are not a table's parameters (REQ-1986).
_AGGREGATE_ARGS = frozenset({"where"})
_GROUP_BY_ARGS = frozenset({"by", "where", "order_by", "limit", "offset", "distinct_on", "having"})


def _nodes_column(
    field_node: FieldNode,
    ctx: CompilationContext,
    table: TableMeta,
    variables: dict | None,
    use_catalog: bool,
) -> tuple[str, ColumnRef, str | None, StatementSources] | None:
    """An _aggregate / _group_by field's ``nodes`` as one column of its own statement: the rows it
    aggregates (a group's, under group-by), each a JSON object of its selected fields --
    ``json_agg(json_object(...)) AS nodes`` -- with each relationship field the correlated JSON
    subquery a root field's is. Returns (the column's SQL, its ColumnRef, the alias the table is
    read under, the tables it reads), or None when ``nodes`` is not selected. One statement: the
    rows are never read again beside it, nor joined back to the groups (REQ-196)."""
    from provisa.compiler.sql_selection import _empty_list_if_none

    assert field_node.selection_set is not None
    nodes_sel: FieldNode | None = None
    for sel in field_node.selection_set.selections:
        if isinstance(sel, FieldNode) and sel.name.value == "nodes" and sel.selection_set:
            nodes_sel = sel
            break
    if nodes_sel is None or nodes_sel.selection_set is None:
        return None

    has_rel = any(
        isinstance(s, FieldNode) and (table.type_name, s.name.value) in ctx.joins
        for s in nodes_sel.selection_set.selections
    )
    root_alias: str | None = "t0" if has_rel else None
    alias_counter = 1
    sources = StatementSources(table)

    def _scalar_sql(phys: str) -> str:
        return f"{_q(root_alias)}.{_q(phys)}" if root_alias else _q(phys)

    pairs: list[str] = []
    for node_sel in nodes_sel.selection_set.selections:
        if not isinstance(node_sel, FieldNode):
            continue
        sel_name = node_sel.name.value
        key = node_sel.alias.value if node_sel.alias else sel_name
        join_key = (table.type_name, sel_name)
        if join_key in ctx.joins and node_sel.selection_set:
            assert root_alias is not None
            join_meta = ctx.joins[join_key]
            join_alias = f"t{alias_counter}"
            alias_counter += 1
            sources.add_table(join_meta.target)
            src_expr, tgt_expr = _resolve_join_src_tgt(
                join_meta, root_alias, join_alias, ctx, table
            )
            _agg_limit = _sw._explicit_limit(node_sel, variables) or join_meta.default_limit
            _pv = (
                join_meta.child_src_val
                if join_meta.child_src_val is not None
                else (src_expr if join_meta.source_column_type != "integer" else None)
            )
            json_expr, alias_counter = _ssel._build_rel_json_expr(
                node_sel.selection_set.selections,
                ctx,
                join_meta.target.type_name,
                join_meta.target,
                join_alias,
                f"{_table_ref(join_meta.target, use_catalog)} {_q(join_alias)}",
                f"{tgt_expr} = {src_expr}",
                join_meta.cardinality,
                _agg_limit,
                use_catalog,
                alias_counter,
                sources,
                variables,
                parent_src_val=_pv,
            )
            pairs.append(f"KEY '{key}' VALUE {json_expr}")
        else:
            phys = ctx.exposed_to_physical.get((table.table_id, sel_name), sel_name)
            pairs.append(f"KEY '{key}' VALUE {_scalar_sql(phys)}")
    if not pairs:
        return None
    nodes_key = nodes_sel.alias.value if nodes_sel.alias else "nodes"
    objects = f"json_agg(json_object({', '.join(pairs)}))"
    sql = f"{_empty_list_if_none(objects)} AS {_q(nodes_key)}"
    column = ColumnRef(
        alias=None,
        column=nodes_key,
        field_name=nodes_key,
        nested_in=None,
        cardinality="one-to-many",
        is_agg=True,
    )
    return sql, column, root_alias, sources


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
            parts.extend(_sw._compile_column_filter("COUNT(*)", value, collector))
        elif key in _SQL_FUNC and isinstance(value, dict):
            sql_func = _SQL_FUNC[key]
            for col_name, col_filter in value.items():
                phys = (
                    exposed_to_physical.get((table_id, col_name), col_name)
                    if table_id is not None
                    else col_name
                )
                expr = f"{sql_func}({_q(phys)})"
                parts.extend(_sw._compile_column_filter(expr, col_filter, collector))
    return " AND ".join(parts) if parts else "TRUE"


_ORDER_FUNCS = {
    "sum": "SUM",
    "avg": "AVG",
    "stddev": "STDDEV",
    "variance": "VARIANCE",
    "min": "MIN",
    "max": "MAX",
}


def _order_direction(direction: str) -> str:
    sql_dir = _sw._DIRECTION_SQL.get(direction)
    if sql_dir is None:
        raise ValueError(f"Unknown order direction: {direction!r}")
    return sql_dir


def _compile_group_by_order(  # REQ-2007
    order_by: list[dict],
    by_cols: list[str],
    table_id: int,
    exposed_to_physical: dict,
) -> str:
    """A group-by's ORDER BY, from its ``order_by`` entries in order: ``{aggregate: {count: dir}}``
    or ``{aggregate: {func: {column: dir}}}`` orders by that aggregate, ``{groupKey: {key: dir}}``
    by a group key. A key the statement does not group by is refused, naming it."""
    parts: list[str] = []
    for entry in order_by:
        for kind, spec in entry.items():
            if kind == "aggregate":
                for func, value in spec.items():
                    if func == "count":
                        parts.append(f"COUNT(*) {_order_direction(value)}")
                        continue
                    for col_name, direction in value.items():
                        phys = exposed_to_physical.get((table_id, col_name), col_name)
                        parts.append(
                            f"{_ORDER_FUNCS[func]}({_q(phys)}) {_order_direction(direction)}"
                        )
            elif kind == "groupKey":
                for col_name, direction in spec.items():
                    if col_name not in by_cols:
                        raise ValueError(
                            f"order_by groupKey {col_name!r} is not a group key of this "
                            f"statement; it groups by {', '.join(by_cols)}"
                        )
                    phys = exposed_to_physical.get((table_id, col_name), col_name)
                    parts.append(f"{_q(phys)} {_order_direction(direction)}")
            else:
                raise ValueError(f"Unknown group-by order_by entry {kind!r}")
    return ", ".join(parts)


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
    sources = StatementSources(table)

    args: dict = {}
    if field_node.arguments:
        for arg in field_node.arguments:
            args[arg.name.value] = _sw._extract_value(arg.value, variables)

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
    metric_names: list[str] = []  # REQ-1319

    if field_node.selection_set:
        for row_sel in field_node.selection_set.selections:
            if not isinstance(row_sel, FieldNode):
                continue
            if row_sel.name.value == "aggregate":
                if row_sel.arguments:
                    for agg_arg in row_sel.arguments:
                        if agg_arg.name.value == "where":
                            agg_filter_where = _sw._extract_value(agg_arg.value, variables) or None  # type: ignore[assignment]
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
                        elif agg_name == "metrics" and agg_sel.selection_set:  # REQ-1319
                            metric_names = [
                                s.name.value
                                for s in agg_sel.selection_set.selections
                                if isinstance(s, FieldNode)
                            ]

    # Build FILTER (WHERE ...) SQL fragment from aggregates.where arg (REQ-655)
    filter_sql = ""
    if agg_filter_where:
        _vvals = ctx.virtual_columns.get(table.table_id)
        filter_where_sql = _sw._compile_where(
            agg_filter_where,
            collector,
            None,
            _vvals,
            table.table_id,
            ctx.exposed_to_physical,
            column_types=ctx.aggregate_columns,
        )
        filter_sql = f" FILTER (WHERE {filter_where_sql})"

    select_parts: list[str] = []
    columns: list[ColumnRef] = []
    _, _, col_aliases = _collect_agg_aliases(field_node)

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
                    # A selected column's alias names its result, as on the aggregate root.
                    field_name=col_aliases.get(func_name, {}).get(col_name, col_name),
                    nested_in=f"aggregate.{func_name}",
                )
            )

    # REQ-1319: metrics under group-by — the inlined aggregate expression groups per key.
    # A metric is a compound expression, so a per-function FILTER (WHERE ...) cannot wrap
    # it: requesting both is a hard error, never a silently-unfiltered result.
    if metric_names and filter_sql:
        raise ValueError("metrics fields do not support the aggregate where/FILTER argument")
    for m_name in metric_names:
        m_expr = ctx.metric_exprs.get((table.table_id, m_name))
        if m_expr is None:
            raise ValueError(f"metric {m_name!r} is not registered for table {table.table_name!r}")
        select_parts.append(m_expr)
        columns.append(
            ColumnRef(
                alias=None,
                column=m_name,
                field_name=m_name,
                nested_in="aggregate.metrics",
            )
        )

    # Each group's rows, a column of the group's own row.
    nodes = _nodes_column(field_node, ctx, table, variables, use_catalog)
    if nodes is not None:
        select_parts.append(nodes[0])
        columns.append(nodes[1])
        sources.merge(nodes[3])

    if not select_parts:
        select_parts = [_q(phys_by_cols[0])] if phys_by_cols else ["1"]

    ref = _table_ref(table, use_catalog)
    read_as = f"{ref} {_q(nodes[2])}" if nodes is not None and nodes[2] else ref
    sql = f"SELECT {', '.join(select_parts)} FROM {read_as}"

    _vvals = ctx.virtual_columns.get(table.table_id)
    # REQ-1986, REQ-1991: the table's parameters, as on its base field.
    api_args = _sw.native_args(
        args, _GROUP_BY_ARGS, ctx.native_filter_columns.get(table.table_id, {})
    )
    nf_conditions = _sw.native_arg_conditions(api_args)
    if "where" in args and not agg_filter_where:
        where_sql = _sw._compile_where(
            args["where"],
            collector,
            None,
            _vvals,
            table.table_id,
            ctx.exposed_to_physical,
            column_types=ctx.aggregate_columns,
        )
        sql += f" WHERE {where_sql}"
    elif "where" in args and agg_filter_where:
        # FILTER params already consumed above; need a fresh sub-collector for WHERE
        where_collector = ParamCollector()
        where_sql = _sw._compile_where(
            args["where"],
            where_collector,
            None,
            _vvals,
            table.table_id,
            ctx.exposed_to_physical,
            column_types=ctx.aggregate_columns,
        )
        # Re-number params: append where params after filter params
        _filter_count = len(collector.params)
        renumbered = where_sql
        for i in range(len(where_collector.params), 0, -1):
            renumbered = renumbered.replace(f"${i}", f"${i + _filter_count}")
        for p in where_collector.params:
            collector._params.append(p)
        sql += f" WHERE {renumbered}"
    if nf_conditions:
        sql += f" {'AND' if 'where' in args else 'WHERE'} {' AND '.join(nf_conditions)}"

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
        ob_sql = _compile_group_by_order(
            order_by_val, by_cols, table.table_id, ctx.exposed_to_physical
        )
        if ob_sql:
            sql += f" ORDER BY {ob_sql}"

    if "limit" in args:
        sql += f" LIMIT {collector.add(int(args['limit']))}"
    if "offset" in args:
        sql += f" OFFSET {collector.add(int(args['offset']))}"

    return CompiledQuery(
        sql=sql,
        params=collector.params,
        root_field=root_name,
        canonical_field=field_node.name.value,
        columns=columns,
        sources=set(sources),
        table_ids=frozenset(sources.table_ids),
        is_group_by=True,
        group_by_columns=list(by_cols),
        api_args=api_args,
        has_nodes=nodes is not None,
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
    sources = StatementSources(table)

    has_count, cols_by_func, metric_names, has_nodes = _collect_requested_agg_funcs(field_node)

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

    # REQ-1319: metrics block — each selected metric inlines its precompiled physical
    # aggregate expression as another select item, so a metric lowers into the same
    # aggregate SQL as the sum/avg fields (one pipeline, one governance path).
    _metrics_key = func_aliases.get("metrics", "metrics")
    for m_name in metric_names:
        m_expr = ctx.metric_exprs.get((table.table_id, m_name))
        if m_expr is None:
            raise ValueError(f"metric {m_name!r} is not registered for table {table.table_name!r}")
        m_field = col_aliases.get("metrics", {}).get(m_name, m_name)
        select_parts.append(f"{m_expr} AS {_q(m_field)}" if m_field != m_name else m_expr)
        columns.append(
            ColumnRef(
                alias=None,
                column=m_name,
                field_name=m_field,
                nested_in=f"{agg_key}.{_metrics_key}",
            )
        )

    # The rows it aggregates, a column of its one row.
    nodes = _nodes_column(field_node, ctx, table, variables, use_catalog) if has_nodes else None
    if nodes is not None:
        select_parts.append(nodes[0])
        columns.append(nodes[1])
        sources.merge(nodes[3])

    if not select_parts:
        select_parts.append("1")

    ref = _table_ref(table, use_catalog)
    read_as = f"{ref} {_q(nodes[2])}" if nodes is not None and nodes[2] else ref
    sql = f"SELECT {', '.join(select_parts)} FROM {read_as}"

    # Process arguments (where)
    args = {}
    if field_node.arguments:
        for arg in field_node.arguments:
            args[arg.name.value] = _sw._extract_value(arg.value, variables)

    _agg_vvals = ctx.virtual_columns.get(table.table_id)
    where_parts = []
    if "where" in args:
        where_parts.append(
            _sw._compile_where(
                args["where"],
                collector,
                None,
                _agg_vvals,
                table.table_id,
                ctx.exposed_to_physical,
                column_types=ctx.aggregate_columns,
            )
        )
    # REQ-1986, REQ-1991: the table's parameters, as on its base field.
    api_args = _sw.native_args(
        args, _AGGREGATE_ARGS, ctx.native_filter_columns.get(table.table_id, {})
    )
    where_parts += _sw.native_arg_conditions(api_args)
    if where_parts:
        sql += f" WHERE {' AND '.join(where_parts)}"

    return CompiledQuery(
        sql=sql,
        params=collector.params,
        root_field=root_name,
        canonical_field=field_node.name.value,
        columns=columns,
        sources=set(sources),
        table_ids=frozenset(sources.table_ids),
        has_nodes=nodes is not None,
        agg_alias=agg_key,
        api_args=api_args,
    )
