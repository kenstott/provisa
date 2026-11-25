# Copyright (c) 2026 Kenneth Stott
# Canary: 904fdd9e-fb95-4daf-adf0-29e56059fec4
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
    StatementSources,
    TableMeta,
)
from provisa.compiler.sql_rewrite import (
    _join_column_expr,
    _join_column_expr_for,
    _q,
    _sql_str_literal,
    _table_ref,
    travel_ref,
)

from provisa.compiler.sql_where import (
    _VIRTUAL_COLS,
    _compile_order_by,
    _compile_where,
    _explicit_limit,
    _extract_non_negative_int,
    _field_args,
    _below_takes_lateral_args,
    _has_lateral_force_args,
    _joins_row_level_to_one,
    JunctionHop,
    RelationshipConditions,
    join_exprs,
    junction_hop,
    where_names_relationship,
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
    relationships: RelationshipConditions | None = None,
    column_types: dict[int, list[tuple[str, str]]] | None = None,
    hop: JunctionHop | None = None,
) -> str:
    """``relationships`` are the target's: a where stating a relationship's condition is compiled
    to EXISTS correlated with the target's rows, read here under the join's alias (REQ-2136).
    ``column_types`` types the where's comparison values (``sql_where._compile_where``).
    ``hop`` is a junction-backed relationship's two hops (REQ-1586): the target's rows are those
    its junction rows correlated with the source name, read with the junction beside them, so
    every column of the target is read under the join's alias."""
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
    # A where naming a relationship correlates with the target's rows.
    related = (
        relationships is not None
        and "where" in args
        and where_names_relationship(args["where"], relationships.ctx, relationships.type_name)
    )
    # Beside a junction, a bare column could be the junction's: the target's are qualified.
    qualifier = join_alias if hop is not None else None
    # REQ-1163: a target read at its own time keeps it under the join's alias. The rows are always
    # read under it: the star is expanded by its table's alias, and a table with no alias of its
    # own expands under a physical name the statement's FROM no longer holds once it is renamed.
    target = travel_ref(_table_ref(join_meta.target, use_catalog), field_node, variables)
    distinct = ""
    if "distinct_on" in args:
        distinct_cols = args["distinct_on"]
        if isinstance(distinct_cols, str):
            distinct_cols = [distinct_cols]
        distinct_prefix = ", ".join(
            (f"{_q(qualifier)}." if qualifier else "") + _q(_e2p.get((_tid, c), c))
            for c in distinct_cols
        )
        distinct = f"DISTINCT ON ({distinct_prefix}) "
    if hop is not None:
        sql = (
            f"LEFT JOIN LATERAL (SELECT {distinct}{_q(join_alias)}.* FROM "
            f"{hop.from_clause(f'{target} {_q(join_alias)}')} WHERE {hop.to_source}"
        )
    else:
        sql = (
            f"LEFT JOIN LATERAL (SELECT {distinct}* FROM {target} {_q(join_alias)} "
            f"WHERE {_lat_tgt_expr} = {src_expr}"
        )
    if "where" in args:
        where_sql = _compile_where(
            args["where"],
            collector,
            join_alias if related else qualifier,
            None,
            _tid,
            exposed_to_physical,
            relationships if related else None,
            column_types,
        )
        sql += f" AND ({where_sql})"
    if "order_by" in args:
        order_by_val = args["order_by"]
        if isinstance(order_by_val, dict):
            order_by_val = [order_by_val]
        sql += f" ORDER BY {_compile_order_by(order_by_val, qualifier, _tid, exposed_to_physical)}"
    limit_value = args.get("limit", join_meta.default_limit)
    if limit_value is not None:
        limit_value = _extract_non_negative_int(limit_value, "limit")
        sql += f" LIMIT {collector.add(limit_value)}"
    if "offset" in args:
        offset_value = _extract_non_negative_int(args["offset"], "offset")
        sql += f" OFFSET {collector.add(offset_value)}"
    return f"{sql}) {_q(join_alias)} ON TRUE"


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
    sources: StatementSources,
    variables: dict | None,
    joins: list[str],
    json_text: set[str] | None = None,
    key_prefix: str = "",
) -> tuple[list[str], int]:
    """Build KEY/VALUE pairs for json_object(KEY k VALUE v, ...) for a relationship.

    Returns (kv_pairs_list, alias_counter) where each element is
    "KEY 'key' VALUE expr" — suitable for joining with commas inside json_object.
    A nested relationship is its own grouped read (``rel_read``), left-joined to
    ``table_alias``'s rows by appending to ``joins``; its value is the read's column.
    Uses SQL-standard json_object syntax so sqlglot transpiles correctly to the engine.
    ``json_text`` collects the key paths (under ``key_prefix``) whose value is JSON an engine
    may embed as text: a nested relationship, a declared object -- each always an object, an
    array or null, so a string there is the engine's text of one (REQ-1867, ColumnRef.json_text).
    A json or jsonb column is not among them: the engine embeds it as the JSON it is, a JSON
    string included.
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
            sources.add_table(sub_join_meta.target)
            sub_src, sub_tgt = join_exprs(
                sub_join_meta, table_alias, sub_alias, ctx, table_meta.table_id
            )
            sub_value, sub_join, alias_counter = rel_read(
                sel,
                sub_join_meta,
                ctx,
                sub_alias,
                sub_src,
                sub_tgt,
                junction_hop(sub_join_meta, table_alias, sub_alias, use_catalog, sources),
                use_catalog,
                alias_counter,
                sources,
                variables,
                json_text=json_text,
                key_prefix=f"{key_prefix}{key}.",
            )
            joins.append(sub_join)
            if json_text is not None:
                json_text.add(f"{key_prefix}{key}")
            kv_pairs.append(f"KEY '{key}' VALUE {sub_value}")
        else:
            if sel.selection_set and (table_meta.table_id, name) in ctx.gql_json_columns:
                blob_base = f"{_q(table_alias)}.{_q(name)}"
                sub_kv = _extract_json_blob_kv(sel.selection_set.selections, blob_base)
                if sub_kv:
                    kv_pairs.append(f"KEY '{key}' VALUE json_object({', '.join(sub_kv)})")
                    if json_text is not None:
                        json_text.add(f"{key_prefix}{key}")
            elif name == "__typename":  # the object's GraphQL type, not a column
                kv_pairs.append(f"KEY '{key}' VALUE {_sql_str_literal(type_name)}")
            else:
                phys_col = ctx.exposed_to_physical.get((table_meta.table_id, name), name)
                sql_col = ctx.physical_to_sql.get((table_meta.table_id, phys_col), phys_col)
                kv_pairs.append(f"KEY '{key}' VALUE {_q(table_alias)}.{_q(sql_col)}")

    return kv_pairs, alias_counter


#: A list field with no rows is [], never null: json_agg of no rows is NULL (#286). Each engine's
#: transpile reads this literal as an empty JSON array (Trino: transpile.py).
EMPTY_JSON_LIST = "CAST('[]' AS JSON)"

#: The columns of a relationship's grouped read (``rel_read``): its join key, one per key column
#: (_key0, _key1, ...), and the field's JSON value.
READ_KEY = "_key"
READ_VALUE = "_value"
#: A row's place among its key's rows, in a read that keeps the head of each key's rows, and the
#: derived table of those numbered rows.
READ_ROW = "_row"
READ_ROWS = "_rows"


def _empty_list_if_none(subquery: str) -> str:
    return f"COALESCE({subquery}, {EMPTY_JSON_LIST})"


def _when_joined(expr: str, joined: str | None) -> str:
    """``expr`` -- a value a joined row does not hold itself (a list nested in it, a literal) --
    null when no row joined: ``joined`` is its join's condition, null when the row is absent.
    The encoder answers a to-many row of nulls as no element, a to-one as null (J1218)."""
    return expr if joined is None else f"CASE WHEN {joined} THEN {expr} END"


def rel_read(
    sel: FieldNode,
    join_meta: JoinMeta,
    ctx: CompilationContext,
    target_alias: str,
    src_expr: str,
    tgt_expr: str,
    hop: JunctionHop | None,
    use_catalog: bool,
    alias_counter: int,
    sources: StatementSources,
    variables: dict | None,
    json_text: set[str] | None = None,
    key_prefix: str = "",
) -> tuple[str, str, int]:
    """A relationship field as one grouped read of its target, joined back by key: every engine
    reads it so, none correlates a subquery with the outer row (#362).

    The read holds one row per join key -- the key and the field's JSON value -- so the LEFT
    JOIN that brings it to the source's rows repeats none of them, and a source row with no
    match reads NULL, as the subquery it replaces did:

    many-to-one, one-to-one → the head of each key's matched rows (ROW_NUMBER, as LIMIT 1
        took one), its json_object.
    one-to-many → json_agg(json_object(...)) of each key's rows; with a list cap (an explicit
        limit or the relationship's default), the first ``n`` of each key's rows (ROW_NUMBER),
        as LIMIT n took them. A key with no rows is no row of the read: the field is [].

    The governed WHERE of the target moves with the read, as it governs every table reference.
    A junction-backed relationship (REQ-1586) reads its target beside its junction, keyed by the
    junction's source columns. Nested relationships are reads of their own, joined inside this
    one. Returns (the field's value, the LEFT JOIN that reads it, alias_counter)."""
    assert sel.selection_set is not None
    target_from = (
        f"{travel_ref(_table_ref(join_meta.target, use_catalog), sel, variables)}"
        f" {_q(target_alias)}"
    )
    if hop is not None:
        target_from = hop.from_clause(target_from)
        keys = list(hop.source_keys)
    else:
        keys = [(tgt_expr, src_expr)]
    inner_joins: list[str] = []
    kv_pairs, alias_counter = _build_rel_json_kv(
        sel.selection_set.selections,
        ctx,
        join_meta.target.type_name,
        join_meta.target,
        target_alias,
        use_catalog,
        alias_counter,
        sources,
        variables,
        inner_joins,
        json_text=json_text,
        key_prefix=key_prefix,
    )
    jbo = f"json_object({', '.join(kv_pairs)})"
    rows_from = " ".join([target_from, *inner_joins])
    key_names = [_q(f"{READ_KEY}{i}") for i in range(len(keys))]
    keyed = ", ".join(f"{inner} AS {name}" for (inner, _), name in zip(keys, key_names))
    partition = ", ".join(inner for inner, _ in keys)
    value, row = _q(READ_VALUE), _q(READ_ROW)
    numbered = (
        f"SELECT {keyed}, {jbo} AS {value}, ROW_NUMBER() OVER (PARTITION BY {partition}) AS {row}"
        f" FROM {rows_from}"
    )
    rows = _q(READ_ROWS)
    cardinality = join_meta.cardinality
    if cardinality in ("many-to-one", "one-to-one"):
        read = f"SELECT {', '.join(key_names)}, {value} FROM ({numbered}) {rows} WHERE {row} = 1"
    elif cardinality == "one-to-many":
        limit = _explicit_limit(sel, variables) or join_meta.default_limit
        if limit is None:
            read = (
                f"SELECT {keyed}, json_agg({jbo}) AS {value} FROM {rows_from} GROUP BY {partition}"
            )
        else:
            read = (
                f"SELECT {', '.join(key_names)}, json_agg({rows}.{value}) AS {value}"
                f" FROM ({numbered}) {rows} WHERE {row} <= {limit}"
                f" GROUP BY {', '.join(key_names)}"
            )
    else:
        raise ValueError(f"unhandled relationship cardinality {cardinality!r}")
    read_alias = _q(f"{target_alias}_read")
    on = " AND ".join(f"{read_alias}.{name} = {outer}" for (_, outer), name in zip(keys, key_names))
    field_value = f"{read_alias}.{value}"
    if cardinality == "one-to-many":
        field_value = _empty_list_if_none(field_value)
    return field_value, f"LEFT JOIN ({read}) {read_alias} ON {on}", alias_counter


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
    sources: StatementSources,
    alias_counter: int,
    use_catalog: bool,
    collector: ParamCollector,
    variables: dict | None,
    cardinality: str | None = None,
    flat: bool = False,
    joined: str | None = None,
) -> tuple[int, bool]:
    """Recursively collect columns and JOINs from nested selections. ``joined`` is the condition
    of the join reading ``parent_alias`` (``_when_joined``); None for the root's own rows."""
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
            sources.add_table(nested_join_meta.target)

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
            # REQ-1586: a junction-backed relationship reaches its rows through its junction.
            hop = junction_hop(nested_join_meta, parent_alias, nested_alias, use_catalog, sources)
            nested_joined = hop.joined if hop is not None else f"{tgt_expr} = {src_expr}"
            nested_key = nested_sel.alias.value if nested_sel.alias else nested_name
            _below = _below_takes_lateral_args(nested_sel, ctx, nested_join_meta.target.type_name)
            _use_agg = (
                not flat
                and not _has_lateral_force_args(nested_sel)
                and not _below
                and not _joins_row_level_to_one(nested_sel, nested_join_meta, ctx)
            )
            if (
                nested_join_meta.default_limit is not None
                or _has_lateral_force_args(nested_sel)
                # A to-many field whose descendants are joined is joined as they are.
                or (_below and nested_join_meta.cardinality == "one-to-many")
            ) and not _use_agg:
                # Each lateral join repeats its parent's rows: the root is limited before it.
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
                        RelationshipConditions(
                            ctx, nested_join_meta.target.type_name, use_catalog, sources
                        ),
                        ctx.aggregate_columns,
                        hop,
                    )
                )
            elif _use_agg and nested_sel.selection_set:
                # One JSON value for the field, as at the root: a grouped read nesting its own
                # relationships at any depth (#286, #362).
                _json_text: set[str] = set()
                json_expr, read_join, alias_counter = rel_read(
                    nested_sel,
                    nested_join_meta,
                    ctx,
                    nested_alias,
                    src_expr,
                    tgt_expr,
                    hop,
                    use_catalog,
                    alias_counter,
                    sources,
                    variables,
                    json_text=_json_text,
                )
                join_clauses.append(read_join)
                json_alias = f"{nesting_path}.{nested_key}".replace(".", "__")
                # A to-one's read is null with its row; a list is [] for none, never null.
                if nested_join_meta.cardinality == "one-to-many":
                    json_expr = _when_joined(json_expr, joined)
                select_parts.append(f"{json_expr} AS {_q(json_alias)}")
                columns.append(
                    ColumnRef(
                        alias=nested_alias,
                        column=json_alias,
                        field_name=nested_key,
                        nested_in=nesting_path,
                        cardinality=cardinality,
                        json=True,
                        json_text=frozenset(_json_text),
                    )
                )
            else:
                nested_from = (
                    f"{travel_ref(_table_ref(nested_join_meta.target, use_catalog), nested_sel, variables)}"
                    f" {_q(nested_alias)}"
                )
                join_clauses.append(
                    f"LEFT JOIN {nested_from} ON {src_expr} = {tgt_expr}"
                    if hop is None
                    else hop.left_joins(nested_from)
                )

            sub_path = f"{nesting_path}.{nested_key}"  # its response key, as the path
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
                    joined=nested_joined,
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
            if nested_name == "__typename":  # the object's GraphQL type, not a column
                col_expr = _when_joined(_sql_str_literal(parent_type_name), joined)
                nested_sql = nested_response_key
            elif nested_phys in _VIRTUAL_COLS:
                _nvc = (ctx.virtual_columns.get(parent_table.table_id) or {}).get(nested_phys, "")
                col_expr = _when_joined(_sql_str_literal(_nvc), joined)
            else:
                col_expr = f"{_q(parent_alias)}.{_q(nested_sql)}"
            # Named by its response path (edges_out__target_term__id): the statement reads as the
            # answer it is, each level's columns told apart.
            path_alias = f"{nesting_path}.{nested_response_key}".replace(".", "__")
            select_parts.append(f"{col_expr} AS {_q(path_alias)}")
            columns.append(
                ColumnRef(
                    alias=parent_alias,
                    column=nested_sql,
                    field_name=nested_response_key,
                    nested_in=nesting_path,
                    cardinality=cardinality,
                    json=(parent_table.table_id, nested_phys.lower()) in ctx.json_columns,
                    integer=(parent_table.table_id, nested_phys.lower()) in ctx.integer_columns,
                )
            )
    return alias_counter, has_lateral
