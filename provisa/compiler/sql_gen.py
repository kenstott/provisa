# Copyright (c) 2026 Kenneth Stott
# Canary: 88835731-3381-43e1-b017-0aff20e5f4bc
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
# complexity-gate: allow-cc=85 allow-ble=1 allow-magic=1 reason="_compile_root_field is the GraphQL root-field compiler — an inherently large dispatch over field/argument kinds (aggregate, sample, relationship, pagination, …), pre-existing and untouched by the REQ-913 AST change (which only converted rewrite_hot_joins off regex). The one blind-except is the best-effort PROVISA_DEFAULT_ROW_LIMIT env read (falls back to the documented default limit); the one magic literal is the 100 upper bound of a sample percentage. Separate refactor tracked for the root-field compiler."

from __future__ import annotations


from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Protocol
from provisa.otel_compat import get_tracer as _get_tracer
from provisa.otel_compat import stage as _stage

if TYPE_CHECKING:
    from provisa.cache.values_cte import HotRows

    pass

from graphql import (
    DocumentNode,
    FieldNode,
    OperationDefinitionNode,
    VariableDefinitionNode,
)

from provisa.compiler.params import ParamCollector
from provisa.core.source_registry import TIME_TRAVEL_SOURCES

from provisa.compiler.sql_types import (
    ColumnRef,
    CompilationContext,
    CompiledQuery,
    JoinMeta,  # noqa: F401 — re-exported; many modules import JoinMeta from sql_gen
    StatementSources,
    TableMeta,  # noqa: F401 — re-exported; many modules import TableMeta from sql_gen
)
from provisa.compiler.sql_rewrite import (
    _q,
    _sql_str_literal,
    _table_ref,
)
from provisa.compiler.sql_where import (
    RelationshipConditions,
    join_exprs,
    where_names_relationship,
    native_arg_conditions,
    native_args,
    _VIRTUAL_COLS,
    _compile_order_by,
    _compile_where,
    _explicit_limit,
    _extract_value,
    _has_joins,
    _below_takes_lateral_args,
    _has_lateral_force_args,
)
from provisa.compiler.sql_selection import (
    _build_gql_selection,
    _build_rel_json_expr,
    _collect_nested_columns,
    _lateral_join,
)

_tracer = _get_tracer(__name__)


# Hard cap on rows returned when the caller supplies no explicit LIMIT — REQ-1678: read from
# core.limits, which the API layer publishes at config load, never from api.app.
from provisa.core.limits import default_row_limit as _get_default_row_limit  # noqa: E402


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
    sources = StatementSources(table)

    # A where stating a relationship's condition correlates an EXISTS with the rows (REQ-2136).
    use_aliases = _has_joins(field_node, ctx, table.type_name) or any(
        arg.name.value == "where"
        and where_names_relationship(_extract_value(arg.value, variables), ctx, table.type_name)
        for arg in field_node.arguments or ()
    )
    root_alias: str | None = "t0" if use_aliases else None
    alias_counter = 1
    has_lateral_ops_joins = False

    # Collect SELECT columns and JOINs
    select_parts: list[str] = []
    columns: list[ColumnRef] = []
    join_clauses: list[str] = []
    # REQ-430: a ``similar`` search ranks the rows by this score, and _similarity reads it.
    similar_sql = _similarity_expr(field_node, variables, ctx, table, root_alias, collector)

    assert field_node.selection_set is not None
    for sel in field_node.selection_set.selections:
        if not isinstance(sel, FieldNode):
            continue
        if sel.name.value == "_similarity":
            if similar_sql is None:
                raise ValueError(
                    f"_similarity is the score of a similar search; give {root_name} a similar "
                    "argument"
                )
            response_key = sel.alias.value if sel.alias else sel.name.value
            select_parts.append(f"{similar_sql} AS {_q(response_key)}")
            columns.append(
                ColumnRef(
                    alias=None,
                    column=response_key,
                    field_name=response_key,
                    nested_in=None,
                )
            )
            continue

        sel_name = sel.name.value
        if sel_name == "__typename":
            # The object's GraphQL type, not a column of its table: emitted as its value.
            response_key = sel.alias.value if sel.alias else sel_name
            select_parts.append(f"{_sql_str_literal(table.type_name)} AS {_q(response_key)}")
            columns.append(
                ColumnRef(
                    alias=root_alias if use_aliases else None,
                    column=response_key,
                    field_name=response_key,
                    nested_in=None,
                )
            )
            continue
        join_key = (table.type_name, sel_name)

        if join_key in ctx.joins:
            # Relationship field → JOIN
            assert root_alias is not None  # joins only exist when use_aliases=True
            join_meta = ctx.joins[join_key]
            join_alias = f"t{alias_counter}"
            alias_counter += 1
            sources.add_table(join_meta.target)

            src_expr, tgt_expr = join_exprs(join_meta, root_alias, join_alias, ctx, table.table_id)
            _below = _below_takes_lateral_args(sel, ctx, join_meta.target.type_name)
            _use_agg = not flat and not _has_lateral_force_args(sel) and not _below
            if (
                join_meta.default_limit is not None
                or _has_lateral_force_args(sel)
                # A to-many field whose descendants are joined is joined as they are.
                or (_below and join_meta.cardinality == "one-to-many")
            ) and not _use_agg:
                # A lateral join repeats each root row once per joined row, so the root's own
                # WHERE, ORDER BY and LIMIT are applied to the root rows before it.
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
                        RelationshipConditions(
                            ctx, join_meta.target.type_name, use_catalog, sources
                        ),
                    )
                )
                if sel.selection_set:
                    alias_counter, _child_lateral = _collect_nested_columns(
                        sel.selection_set.selections,
                        join_alias,
                        join_meta.target.type_name,
                        join_meta.target,
                        # The field's response key: the path its rows are answered under.
                        sel.alias.value if sel.alias else sel_name,
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
                        # The field's response key: the path its rows are answered under.
                        sel.alias.value if sel.alias else sel_name,
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

    result_limit: int | None = None

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

    # Native filter args (any arg not handled by SQL compilation): each a _nf_-prefixed condition
    # of the root's WHERE, so the SQL/CQL preview shows the filter. nf_extractor strips them before
    # the engine runs the statement; endpoint.py uses api_args for the REST call.
    api_args = native_args(args, _STANDARD_ARGS, ctx.native_filter_columns.get(table.table_id, {}))

    # WHERE
    where_parts: list[str] = []
    if "where" in args:
        _vvals = ctx.virtual_columns.get(table.table_id)
        where_parts.append(
            _compile_where(
                args["where"],
                collector,
                root_alias,
                _vvals,
                table.table_id,
                ctx.exposed_to_physical,
                RelationshipConditions(ctx, table.type_name, use_catalog, sources),
            )
        )
    where_parts += native_arg_conditions(api_args)
    where_sql = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""

    # ORDER BY -- a similar search first: the rows most like the query lead (REQ-430).
    order_parts = [f"{similar_sql} DESC"] if similar_sql is not None else []
    if "order_by" in args:
        order_by_val = args["order_by"]
        if isinstance(order_by_val, dict):
            order_by_val = [order_by_val]
        order_parts.append(
            _compile_order_by(order_by_val, root_alias, table.table_id, ctx.exposed_to_physical)
        )
    order_sql = f" ORDER BY {', '.join(order_parts)}" if order_parts else ""

    if has_lateral_ops_joins:
        # A lateral join repeats each root row once per joined row: the root rows are chosen
        # first -- filtered, ordered and limited in a subquery -- and only those are expanded.
        # With no limit asked, the default row limit caps them, so one lateral scan runs per
        # row it keeps, not per row of the table. result_limit truncates the grouped rows.
        base_limit = int(args["limit"]) if "limit" in args else _get_default_row_limit()
        result_limit = base_limit if "limit" in args else None
        base = f"SELECT * FROM {from_clause}{where_sql}{order_sql} LIMIT {base_limit}"
        if "offset" in args:
            base += f" OFFSET {collector.add(int(args['offset']))}"
        if use_aliases:
            assert root_alias is not None
            from_clause = f"({base}) {_q(root_alias)}"
        else:
            from_clause = f"({base})"
        where_sql = ""

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

    # The root rows' order holds over the expanded rows too.
    sql += f"{where_sql}{order_sql}"

    # LIMIT / OFFSET: with a lateral join they chose the root rows in the subquery above.
    if not has_lateral_ops_joins:
        # Always parameterized, never interpolated. No compile-time default LIMIT:
        # the row cap is applied by governance (stage2 resolve_row_cap) so it is
        # role-aware — FULL_RESULTS roles get no default row limit, others get the
        # configured default_row_limit. (The lateral-ops base guard above is a
        # Cartesian-explosion guard, not the governance cap, and is kept.)
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
        api_args=api_args,
        result_limit=result_limit,
        gql_remote_extra_selections=ctx.gql_remote_extra_selections,
    )


class HotTableSource(Protocol):  # REQ-1678
    """What the hot-join rewrite needs from the cache: the manager satisfies it structurally, so
    the compiler never imports the cache manager (which pulls the file-source and DuckDB stack)."""

    def entries_for(self, table_ids: Iterable[int]) -> "Mapping[str, HotRows]": ...


def rewrite_hot_joins(  # REQ-230, REQ-232
    compiled: CompiledQuery, hot_manager: HotTableSource
) -> CompiledQuery:
    """Rewrite references to hot-cached tables to use VALUES-based CTEs.

    When the query reads a hot-cached table, replace the table reference with a CTE containing
    the cached rows as VALUES. This works cross-source since the data travels as constants in the
    query. The hot tables are those among the tables the query reads (``compiled.table_ids``),
    each under the name the SQL carries for it.

    REQ-913: structural, AST-only. The set of hot tables is read from the parsed
    tree and the rewrite is delegated to ``build_values_ctes_sql``, which renames
    the table node and attaches the CTE on the AST — no regex over the SQL text
    derives the JOIN structure, alias binding, or WITH injection point.
    """
    import sqlglot
    import sqlglot.expressions as exp

    from provisa.cache.values_cte import build_values_ctes_sql

    hot = hot_manager.entries_for(compiled.table_ids)
    tree = sqlglot.parse_one(compiled.sql, read="postgres")
    hot_names: list[str] = []
    for tbl in tree.find_all(exp.Table):
        if tbl.name in hot and tbl.name not in hot_names:
            hot_names.append(tbl.name)

    sql = build_values_ctes_sql(compiled.sql, {name: hot[name] for name in hot_names})

    if sql != compiled.sql:
        return CompiledQuery(
            sql=sql,
            params=compiled.params,
            root_field=compiled.root_field,
            canonical_field=compiled.canonical_field,
            columns=compiled.columns,
            sources=compiled.sources,
            table_ids=compiled.table_ids,
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
    sources = StatementSources(table)

    select_parts: list[str] = []
    columns: list[ColumnRef] = []
    for sel in _extract_node_selections(field_node):
        if not isinstance(sel, FieldNode):
            continue
        sel_name = sel.name.value
        if sel_name == "__typename":  # the node's GraphQL type, not a column
            response_key = sel.alias.value if sel.alias else sel_name
            select_parts.append(f"{_sql_str_literal(table.type_name)} AS {_q(response_key)}")
            columns.append(ColumnRef(None, response_key, response_key, None))
            continue
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
        sources=set(sources),
        table_ids=frozenset(sources.table_ids),
        is_connection=True,
        is_backward=is_backward,
        sort_columns=sort_columns,
        page_size=int(page_size) if page_size is not None else None,
        has_cursor=("after" in args or "before" in args),
    )


class UnknownRootField(ValueError):
    """A query names a root field the role's compilation context does not have.

    The statement is at fault, not the server: the role is not served a table by that name (it
    was never registered for it, or was removed since the client learned the schema). Every
    surface answers it as a client error naming the field — over HTTP a 400 with code
    ``data.unknown_root_field`` (api/app.py) — never as an unhandled exception. A ``ValueError``,
    so a surface that already answers the compiler's refusals answers this one."""

    code = "data.unknown_root_field"

    def __init__(self, field_name: str) -> None:
        self.field_name = field_name
        self.params = {"field": field_name}
        super().__init__(f"Unknown root query field: {field_name!r}")


#: A root field's arguments that are not a table's parameters (REQ-1986).
_STANDARD_ARGS = frozenset(
    {"where", "order_by", "limit", "offset", "distinct_on", "as_of", "sample", "similar"}
)


def _similarity_expr(
    field_node: FieldNode,
    variables: dict | None,
    ctx: CompilationContext,
    table: TableMeta,
    root_alias: str | None,
    collector: ParamCollector,
) -> str | None:  # REQ-430
    """The field's ``similar: {column, text | vector}`` as ``cosine_similarity(<column>, $N)``,
    the query a bound value; None when the field has no such argument. Governance completes the
    call and the pipeline makes the value a vector of the column's model."""
    arg = next((a for a in field_node.arguments or [] if a.name.value == "similar"), None)
    if arg is None:
        return None
    spec = _extract_value(arg.value, variables)
    assert isinstance(spec, dict)
    text, vector = spec.get("text"), spec.get("vector")
    if (text is None) == (vector is None):
        raise ValueError("similar takes a text or a vector -- exactly one of them")
    column = str(spec["column"])
    physical = ctx.exposed_to_physical.get((table.table_id, column), column)
    sql_name = ctx.physical_to_sql.get((table.table_id, physical), physical)
    ref = f"{_q(root_alias)}.{_q(sql_name)}" if root_alias else _q(sql_name)
    query = collector.add(text if text is not None else list(vector))  # pyright: ignore[reportArgumentType]
    return f"cosine_similarity({ref}, {query})"


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
                    raise UnknownRootField(field_name)
                with _stage(_tracer, "compiler.compile_query", name="compile") as span:
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
                results.append(compiled)

    return results
