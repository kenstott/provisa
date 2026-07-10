# Copyright (c) 2026 Kenneth Stott
# Canary: a06089bd-b453-4daa-8692-7fcbd909b7b1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Lateral-join, aggregate-subquery, and nested-JSON selection builders for sql_gen.

Depends on sql_where (WHERE/ORDER BY + field-arg helpers) and the sql_rewrite/
sql_types leaves; never calls back into sql_gen's root-field compiler.
"""

from __future__ import annotations


from graphql import (
    FieldNode,
)

from provisa.compiler.params import ParamCollector
from provisa.compiler.sql_types import (
    ColumnRef,
    CompilationContext,
    JoinMeta,
    TableMeta,
)
from provisa.compiler.sql_rewrite import (
    _join_column_expr,
    _join_column_expr_for,
    _q,
    _sql_str_literal,
    _table_ref,
)

from provisa.compiler.sql_where import (
    _VIRTUAL_COLS,
    _compile_order_by,
    _compile_where,
    _explicit_limit,
    _extract_non_negative_int,
    _field_args,
    _has_lateral_force_args,
)


def _lateral_join(
    field_node: FieldNode,
    join_meta: JoinMeta,
    join_alias: str,
    src_expr: str,
    collector: ParamCollector,
    variables: dict | None,
    use_catalog: bool,
    exposed_to_physical: dict | None = None,
) -> str:
    args = _field_args(field_node, variables)
    _tid = join_meta.target.table_id
    _e2p = exposed_to_physical or {}
    if join_meta.target_expr is not None:
        _lat_tgt_expr = join_meta.target_expr.replace("{alias}", _q(join_alias))
    else:
        _lat_tgt_expr = _join_column_expr_for(
            None,
            join_meta.target_column,
            join_meta.target_column_type,
            join_meta.source_column_type,
        )
    sql = (
        f"LEFT JOIN LATERAL (SELECT * FROM {_table_ref(join_meta.target, use_catalog)}"
        f" WHERE {_lat_tgt_expr} = {src_expr}"
    )
    if "where" in args:
        where_sql = _compile_where(
            args["where"],
            collector,
            None,
            None,
            _tid,
            exposed_to_physical,
        )
        sql += f" AND ({where_sql})"
    if "distinct_on" in args:
        distinct_cols = args["distinct_on"]
        if isinstance(distinct_cols, str):
            distinct_cols = [distinct_cols]
        distinct_prefix = ", ".join(_q(_e2p.get((_tid, c), c)) for c in distinct_cols)
        sql = sql.replace("SELECT *", f"SELECT DISTINCT ON ({distinct_prefix}) *", 1)
    if "order_by" in args:
        order_by_val = args["order_by"]
        if isinstance(order_by_val, dict):
            order_by_val = [order_by_val]
        sql += f" ORDER BY {_compile_order_by(order_by_val, None, _tid, exposed_to_physical)}"
    limit_value = args.get("limit", join_meta.default_limit)
    if limit_value is not None:
        limit_value = _extract_non_negative_int(limit_value, "limit")
        sql += f" LIMIT {collector.add(limit_value)}"
    if "offset" in args:
        offset_value = _extract_non_negative_int(args["offset"], "offset")
        sql += f" OFFSET {collector.add(offset_value)}"
    return f"{sql}) {_q(join_alias)} ON TRUE"


def _emit_agg_subqueries(
    selections,
    ctx: CompilationContext,
    type_name: str,
    table_meta: TableMeta,
    from_clause: str,
    where_expr: str,
    extra_joins: str,
    current_alias: str,
    nesting_path: str,
    cardinality: str | None,
    agg_limit: int | None,
    use_catalog: bool,
    alias_counter: int,
    select_parts: list[str],
    columns: list[ColumnRef],
    sources: set[str],
    variables: dict | None = None,
) -> int:
    """Emit correlated ARRAY_AGG subqueries for all scalars at any depth.

    For scalars: emits one ARRAY_AGG correlated subquery per leaf field.
    For sub-relationships: extends the JOIN chain and recurses.
    """
    for sel in selections:
        if not isinstance(sel, FieldNode):
            continue
        name = sel.name.value
        key = sel.alias.value if sel.alias else name
        join_key = (type_name, name)

        if join_key in ctx.joins:
            if not sel.selection_set:
                continue
            sub_join_meta = ctx.joins[join_key]
            sub_alias = f"t{alias_counter}"
            alias_counter += 1
            sources.add(sub_join_meta.target.source_id)

            if sub_join_meta.source_constant is not None:
                sub_src = (
                    _sql_str_literal(sub_join_meta.source_constant)
                    if isinstance(sub_join_meta.source_constant, str)
                    else str(sub_join_meta.source_constant)
                )
            elif sub_join_meta.source_expr is not None:
                sub_src = sub_join_meta.source_expr.replace("{alias}", _q(current_alias))
            elif sub_join_meta.source_json_key:
                sub_src = (
                    f"CAST({_q(current_alias)}.{_q(sub_join_meta.source_column)} AS JSON)"
                    f">>'{sub_join_meta.source_json_key}'"
                )
            else:
                sub_src = _join_column_expr(
                    current_alias,
                    sub_join_meta.source_column,
                    sub_join_meta.source_column_type,
                    sub_join_meta.target_column_type,
                )
            if sub_join_meta.target_expr is not None:
                sub_tgt = sub_join_meta.target_expr.replace("{alias}", _q(sub_alias))
            elif sub_join_meta.target_column in _VIRTUAL_COLS:
                _tvc = (ctx.virtual_columns.get(sub_join_meta.target.table_id) or {}).get(
                    sub_join_meta.target_column, ""
                )
                sub_tgt = _sql_str_literal(_tvc)
            else:
                sub_tgt = _join_column_expr(
                    sub_alias,
                    sub_join_meta.target_column,
                    sub_join_meta.target_column_type,
                    sub_join_meta.source_column_type,
                )
            _on_cond = f"{sub_tgt} = {sub_src}"
            new_join = (
                f"JOIN {_table_ref(sub_join_meta.target, use_catalog)} {_q(sub_alias)}"
                f" ON {_on_cond}"
            )
            new_extra = f"{extra_joins} {new_join}".strip() if extra_joins else new_join
            sub_limit = _explicit_limit(sel, variables) or sub_join_meta.default_limit
            alias_counter = _emit_agg_subqueries(
                sel.selection_set.selections,
                ctx,
                sub_join_meta.target.type_name,
                sub_join_meta.target,
                from_clause,
                where_expr,
                new_extra,
                sub_alias,
                f"{nesting_path}.{key}",
                sub_join_meta.cardinality,
                sub_limit,
                use_catalog,
                alias_counter,
                select_parts,
                columns,
                sources,
                variables,
            )
        else:
            phys_col = ctx.exposed_to_physical.get((table_meta.table_id, name), name)
            sql_col = ctx.physical_to_sql.get((table_meta.table_id, phys_col), phys_col)
            col_alias = nesting_path.replace(".", "__") + "__" + key
            if extra_joins and agg_limit is not None:
                select_parts.append(
                    f"(SELECT ARRAY_AGG({_q(current_alias)}.{_q(sql_col)})"
                    f" FROM (SELECT {_q(current_alias)}.{_q(sql_col)}"
                    f" FROM {from_clause} {extra_joins}"
                    f" WHERE {where_expr}"
                    f" LIMIT {agg_limit}))"
                    f" AS {_q(col_alias)}"
                )
            elif extra_joins:
                select_parts.append(
                    f"(SELECT ARRAY_AGG({_q(current_alias)}.{_q(sql_col)})"
                    f" FROM {from_clause} {extra_joins}"
                    f" WHERE {where_expr})"
                    f" AS {_q(col_alias)}"
                )
            elif agg_limit is not None:
                select_parts.append(
                    f"(SELECT ARRAY_AGG({_q(sql_col)})"
                    f" FROM (SELECT {_q(sql_col)}"
                    f" FROM {from_clause}"
                    f" WHERE {where_expr}"
                    f" LIMIT {agg_limit}))"
                    f" AS {_q(col_alias)}"
                )
            else:
                select_parts.append(
                    f"(SELECT ARRAY_AGG({_q(current_alias)}.{_q(sql_col)})"
                    f" FROM {from_clause}"
                    f" WHERE {where_expr})"
                    f" AS {_q(col_alias)}"
                )
            columns.append(
                ColumnRef(
                    alias=current_alias,
                    column=sql_col,
                    field_name=key,
                    nested_in=nesting_path,
                    cardinality=cardinality,
                    is_agg=True,
                )
            )
    return alias_counter


def _extract_json_blob_kv(sels, blob_base: str) -> list[str]:
    """Recursively build KEY/VALUE pairs by extracting fields from a JSON blob column."""
    pairs: list[str] = []
    for ss in sels:
        if not isinstance(ss, FieldNode):
            continue
        sn = ss.name.value
        sk = ss.alias.value if ss.alias else sn
        if ss.selection_set:
            sub_pairs = _extract_json_blob_kv(ss.selection_set.selections, f"{blob_base}->'{sn}'")
            if sub_pairs:
                pairs.append(f"KEY '{sk}' VALUE json_object({', '.join(sub_pairs)})")
        else:
            pairs.append(f"KEY '{sk}' VALUE {blob_base}->>'{sn}'")
    return pairs


def _build_rel_json_kv(
    selections,
    ctx: CompilationContext,
    type_name: str,
    table_meta: TableMeta,
    table_alias: str,
    use_catalog: bool,
    alias_counter: int,
    sources: set[str],
    variables: dict | None,
    parent_src_val: str | None = None,
) -> tuple[list[str], int]:
    """Build KEY/VALUE pairs for json_object(KEY k VALUE v, ...) for a relationship.

    Returns (kv_pairs_list, alias_counter) where each element is
    "KEY 'key' VALUE expr" — suitable for joining with commas inside json_object.
    Nested relationships produce correlated subqueries at value positions.
    Uses SQL-standard json_object syntax so sqlglot transpiles correctly to the engine.
    """
    kv_pairs: list[str] = []
    for sel in selections:
        if not isinstance(sel, FieldNode):
            continue
        name = sel.name.value
        key = sel.alias.value if sel.alias else name
        join_key = (type_name, name)

        if join_key in ctx.joins:
            if not sel.selection_set:
                continue
            sub_join_meta = ctx.joins[join_key]
            sub_alias = f"t{alias_counter}"
            alias_counter += 1
            sources.add(sub_join_meta.target.source_id)

            if sub_join_meta.source_expr is not None:
                if parent_src_val is not None:
                    # the engine rejects doubly-nested correlated subqueries. When the parent
                    # join resolved to a constant (e.g. 'pet-store.pets'), use that value
                    # here so the child subquery's WHERE clause contains no outer reference.
                    sub_src = parent_src_val
                else:
                    sub_src = sub_join_meta.source_expr.replace("{alias}", _q(table_alias))
            elif sub_join_meta.source_constant is not None:
                sub_src = (
                    _sql_str_literal(sub_join_meta.source_constant)
                    if isinstance(sub_join_meta.source_constant, str)
                    else str(sub_join_meta.source_constant)
                )
            elif sub_join_meta.source_json_key:
                sub_src = (
                    f"CAST({_q(table_alias)}.{_q(sub_join_meta.source_column)} AS JSON)"
                    f">>'{sub_join_meta.source_json_key}'"
                )
            else:
                sub_src = _join_column_expr(
                    table_alias,
                    sub_join_meta.source_column,
                    sub_join_meta.source_column_type,
                    sub_join_meta.target_column_type,
                )
            if sub_join_meta.target_expr is not None:
                sub_tgt = sub_join_meta.target_expr.replace("{alias}", _q(sub_alias))
            elif sub_join_meta.target_column in _VIRTUAL_COLS:
                _tvc = (ctx.virtual_columns.get(sub_join_meta.target.table_id) or {}).get(
                    sub_join_meta.target_column, ""
                )
                sub_tgt = _sql_str_literal(_tvc)
            else:
                sub_tgt = _join_column_expr(
                    sub_alias,
                    sub_join_meta.target_column,
                    sub_join_meta.target_column_type,
                    sub_join_meta.source_column_type,
                )
            sub_from = f"{_table_ref(sub_join_meta.target, use_catalog)} {_q(sub_alias)}"
            sub_where = f"{sub_tgt} = {sub_src}"
            sub_limit = _explicit_limit(sel, variables) or sub_join_meta.default_limit
            # Prefer child_src_val (explicit varchar constant) over sub_src. Fall back to
            # sub_src for non-integer joins; block propagation for integer joins to avoid
            # type mismatches in child varchar source_expr comparisons.
            _next_parent_src = (
                sub_join_meta.child_src_val
                if sub_join_meta.child_src_val is not None
                else (sub_src if sub_join_meta.source_column_type != "integer" else None)
            )
            sub_expr, alias_counter = _build_rel_json_expr(
                sel.selection_set.selections,
                ctx,
                sub_join_meta.target.type_name,
                sub_join_meta.target,
                sub_alias,
                sub_from,
                sub_where,
                sub_join_meta.cardinality,
                sub_limit,
                use_catalog,
                alias_counter,
                sources,
                variables,
                parent_src_val=_next_parent_src,
            )
            kv_pairs.append(f"KEY '{key}' VALUE {sub_expr}")
        else:
            if sel.selection_set and (table_meta.table_id, name) in ctx.gql_json_columns:
                blob_base = f"{_q(table_alias)}.{_q(name)}"
                sub_kv = _extract_json_blob_kv(sel.selection_set.selections, blob_base)
                if sub_kv:
                    kv_pairs.append(f"KEY '{key}' VALUE json_object({', '.join(sub_kv)})")
            else:
                phys_col = ctx.exposed_to_physical.get((table_meta.table_id, name), name)
                sql_col = ctx.physical_to_sql.get((table_meta.table_id, phys_col), phys_col)
                kv_pairs.append(f"KEY '{key}' VALUE {_q(table_alias)}.{_q(sql_col)}")

    return kv_pairs, alias_counter


def _build_rel_json_expr(
    selections,
    ctx: CompilationContext,
    type_name: str,
    table_meta: TableMeta,
    table_alias: str,
    from_clause: str,
    where_expr: str,
    cardinality: str | None,
    agg_limit: int | None,
    use_catalog: bool,
    alias_counter: int,
    sources: set[str],
    variables: dict | None = None,
    parent_src_val: str | None = None,
) -> tuple[str, int]:
    """Build one correlated JSON subquery for a relationship.

    many-to-one  → (SELECT json_object(...) FROM ... WHERE ... LIMIT 1)
    one-to-many  → (SELECT json_agg(json_object(...)) FROM ... WHERE ...)
    one-to-many with agg_limit →
        (SELECT json_agg(_t) FROM (SELECT json_object(...) AS _t FROM ... WHERE ... LIMIT n) _sub)
    Returns (sql_expr, alias_counter).
    """
    kv_pairs, alias_counter = _build_rel_json_kv(
        selections,
        ctx,
        type_name,
        table_meta,
        table_alias,
        use_catalog,
        alias_counter,
        sources,
        variables,
        parent_src_val=parent_src_val,
    )
    jbo = f"json_object({', '.join(kv_pairs)})"

    if cardinality == "many-to-one":
        expr = f"(SELECT {jbo} FROM {from_clause} WHERE {where_expr} LIMIT 1)"
    elif agg_limit is not None:
        expr = (
            f"(SELECT json_agg(_t)"
            f" FROM (SELECT {jbo} AS _t"
            f" FROM {from_clause}"
            f" WHERE {where_expr}"
            f" LIMIT {agg_limit}) _sub)"
        )
    else:
        expr = f"(SELECT json_agg({jbo}) FROM {from_clause} WHERE {where_expr})"

    return expr, alias_counter


def _build_gql_selection(field_name: str, selection_set) -> str:
    """Serialize a GQL field + its selection_set back to a GQL selection string."""

    def _sels(ss) -> str:
        parts = []
        for sel in ss.selections:
            if not isinstance(sel, FieldNode):
                continue
            sn = sel.name.value
            if sel.selection_set:
                parts.append(f"{sn} {{ {_sels(sel.selection_set)} }}")
            else:
                parts.append(sn)
        return " ".join(parts)

    return f"{field_name} {{ {_sels(selection_set)} }}"


# complexity-gate: allow-cc=32 reason="relocated verbatim from sql_gen.py; CC is the per-selection nested-relationship dispatch (column vs join vs lateral vs agg-subquery); decomposition is separately-tracked debt"
def _collect_nested_columns(
    selections,
    parent_alias: str,
    parent_type_name: str,
    parent_table: TableMeta,
    nesting_path: str,
    ctx: CompilationContext,
    select_parts: list[str],
    columns: list[ColumnRef],
    join_clauses: list[str],
    sources: set[str],
    alias_counter: int,
    use_catalog: bool,
    collector: ParamCollector,
    variables: dict | None,
    cardinality: str | None = None,
    flat: bool = False,
) -> tuple[int, bool]:
    """Recursively collect columns and JOINs from nested selections."""
    has_lateral = False
    for nested_sel in selections:
        if not isinstance(nested_sel, FieldNode):
            continue
        nested_name = nested_sel.name.value
        nested_join_key = (parent_type_name, nested_name)

        if nested_join_key in ctx.joins:
            # This nested field is itself a relationship → add another JOIN
            nested_join_meta = ctx.joins[nested_join_key]
            nested_alias = f"t{alias_counter}"
            alias_counter += 1
            sources.add(nested_join_meta.target.source_id)

            if nested_join_meta.source_constant is not None:
                src_expr = (
                    _sql_str_literal(nested_join_meta.source_constant)
                    if isinstance(nested_join_meta.source_constant, str)
                    else str(nested_join_meta.source_constant)
                )
            elif nested_join_meta.source_column in _VIRTUAL_COLS:
                _svc = (ctx.virtual_columns.get(parent_table.table_id) or {}).get(
                    nested_join_meta.source_column, ""
                )
                src_expr = _sql_str_literal(_svc)
            elif nested_join_meta.source_json_key:
                src_expr = f"CAST({_q(parent_alias)}.{_q(nested_join_meta.source_column)} AS JSON)->>'{nested_join_meta.source_json_key}'"
            else:
                src_expr = _join_column_expr(
                    parent_alias,
                    nested_join_meta.source_column,
                    nested_join_meta.source_column_type,
                    nested_join_meta.target_column_type,
                )
            if nested_join_meta.target_expr is not None:
                tgt_expr = nested_join_meta.target_expr.replace("{alias}", _q(nested_alias))
            elif nested_join_meta.target_column in _VIRTUAL_COLS:
                _tvc = (ctx.virtual_columns.get(nested_join_meta.target.table_id) or {}).get(
                    nested_join_meta.target_column, ""
                )
                tgt_expr = _sql_str_literal(_tvc)
            else:
                tgt_expr = _join_column_expr(
                    nested_alias,
                    nested_join_meta.target_column,
                    nested_join_meta.target_column_type,
                    nested_join_meta.source_column_type,
                )
            nested_key = nested_sel.alias.value if nested_sel.alias else nested_name
            _use_agg = not flat and not _has_lateral_force_args(nested_sel)
            if (
                nested_join_meta.default_limit is not None or _has_lateral_force_args(nested_sel)
            ) and not _use_agg:
                if nested_join_meta.default_limit is not None:
                    has_lateral = True
                join_clauses.append(
                    _lateral_join(
                        nested_sel,
                        nested_join_meta,
                        nested_alias,
                        src_expr,
                        collector,
                        variables,
                        use_catalog,
                        ctx.exposed_to_physical,
                    )
                )
            elif _use_agg and nested_sel.selection_set:
                _agg_limit = (
                    _explicit_limit(nested_sel, variables) or nested_join_meta.default_limit
                )
                _from_clause = (
                    f"{_table_ref(nested_join_meta.target, use_catalog)} {_q(nested_alias)}"
                )
                _where_expr = f"{tgt_expr} = {src_expr}"
                alias_counter = _emit_agg_subqueries(
                    nested_sel.selection_set.selections,
                    ctx,
                    nested_join_meta.target.type_name,
                    nested_join_meta.target,
                    _from_clause,
                    _where_expr,
                    "",
                    nested_alias,
                    f"{nesting_path}.{nested_key}",
                    nested_join_meta.cardinality,
                    _agg_limit,
                    use_catalog,
                    alias_counter,
                    select_parts,
                    columns,
                    sources,
                    variables,
                )
            else:
                join_clauses.append(
                    f"LEFT JOIN {_table_ref(nested_join_meta.target, use_catalog)}"
                    f" {_q(nested_alias)}"
                    f" ON {src_expr} = {tgt_expr}"
                )

            sub_path = f"{nesting_path}.{nested_name}"
            if nested_sel.selection_set and not _use_agg:
                alias_counter, _child_lateral = _collect_nested_columns(
                    nested_sel.selection_set.selections,
                    nested_alias,
                    nested_join_meta.target.type_name,
                    nested_join_meta.target,
                    sub_path,
                    ctx,
                    select_parts,
                    columns,
                    join_clauses,
                    sources,
                    alias_counter,
                    use_catalog,
                    collector,
                    variables,
                    cardinality=nested_join_meta.cardinality,
                    flat=flat,
                )
                has_lateral |= _child_lateral
        else:
            # Undeclared OBJECT field on a graphql_remote table — hydrate from remote endpoint
            if (
                nested_sel.selection_set
                and parent_table.source_type == "graphql_remote"
                and (parent_table.table_id, nested_name) not in ctx.gql_json_columns
            ):
                _gql_sel = _build_gql_selection(nested_name, nested_sel.selection_set)
                ctx.gql_json_columns.add((parent_table.table_id, nested_name))
                ctx.gql_remote_extra_selections.setdefault(parent_table.table_name, {})[
                    nested_name
                ] = _gql_sel
            # GQL OBJECT column stored as JSON — expand sub-selections recursively via -> / ->>
            if (
                nested_sel.selection_set
                and (parent_table.table_id, nested_name) in ctx.gql_json_columns
            ):

                def _emit_json_cols(
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
                            _emit_json_cols(
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
                                    alias=parent_alias,
                                    column=col_alias,
                                    field_name=sk,
                                    nested_in=nesting,
                                    cardinality=cardinality,
                                )
                            )

                _emit_json_cols(
                    nested_sel.selection_set.selections,
                    f"{_q(parent_alias)}.{_q(nested_name)}",
                    nested_name,
                    f"{nesting_path}.{nested_name}",
                )
                continue
            # Scalar column from the parent join
            nested_response_key = nested_sel.alias.value if nested_sel.alias else nested_name
            nested_phys = ctx.exposed_to_physical.get(
                (parent_table.table_id, nested_name), nested_name
            )
            nested_sql = ctx.physical_to_sql.get((parent_table.table_id, nested_phys), nested_phys)
            if nested_phys in _VIRTUAL_COLS:
                _nvc = (ctx.virtual_columns.get(parent_table.table_id) or {}).get(nested_phys, "")
                col_expr = _sql_str_literal(_nvc)
            else:
                col_expr = f"{_q(parent_alias)}.{_q(nested_sql)}"
            if nested_sel.alias:
                select_parts.append(f"{col_expr} AS {_q(nested_response_key)}")
            else:
                select_parts.append(col_expr)
            columns.append(
                ColumnRef(
                    alias=parent_alias,
                    column=nested_sql,
                    field_name=nested_response_key,
                    nested_in=nesting_path,
                    cardinality=cardinality,
                )
            )
    return alias_counter, has_lateral
