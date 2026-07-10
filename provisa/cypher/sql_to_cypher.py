# Copyright (c) 2026 Kenneth Stott
# Canary: 1cf652fa-af09-49ac-a860-b3222b8a38ce
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Translate semantic SQL → Cypher (reverse of cypher_to_sql pipeline).

Entry point: semantic_sql_to_cypher(sql, label_map, ctx) -> str | None

Only handles SELECT statements with MATCH-translatable FROM/JOIN structures.
Returns None when the SQL cannot be represented as a Cypher pattern query.
"""

from __future__ import annotations

import re
import string

import sqlglot
import sqlglot.expressions as exp

from provisa.cypher.label_map import CypherLabelMap, RelationshipMapping
from provisa.cypher.sql_to_cypher_helpers import (
    _resolve_label,
    _rel_type_from_on,
    _src_alias_from_on,
    _sql_to_cypher_expr,
)
from provisa.cypher.sql_to_cypher_agg import (
    _process_array_agg_subqueries,
    _process_json_subqueries,
)

# Requirements: REQ-345, REQ-347, REQ-351, REQ-355


def semantic_sql_to_cypher(  # REQ-345, REQ-347, REQ-351, REQ-355
    semantic_sql: str,
    label_map: CypherLabelMap,
    ctx: object,  # object-ok: circular-import boundary — CompilationContext lives in provisa.compiler
    override_limit: int | None = None,
    params: list | None = None,
    flat: bool = False,
    node_only: bool = False,
) -> str | None:
    """Convert semantic SQL to an equivalent Cypher query.

    Each node gets two labels: the type label (e.g. User) and the domain label
    (e.g. SalesData), so callers can MATCH all nodes in a domain with
    MATCH (n:SalesData).

    Args:
        semantic_sql:   Semantic SQL using domain.field_name table references.
        label_map:      CypherLabelMap built from the same CompilationContext.
        ctx:            CompilationContext (used to map domain.field_name → label).
        override_limit: When set, replaces the SQL LIMIT (used when sql_gen emits a
                        safety-cap LIMIT instead of the user-supplied value, e.g. when
                        lateral ops joins are present).

    Returns:
        Cypher string, or None if the SQL cannot be translated.
    """
    from provisa.compiler.naming import domain_to_sql_name

    try:
        tree = sqlglot.parse_one(semantic_sql, read="postgres")
    except Exception:
        return None

    if not isinstance(tree, exp.Select):
        return None

    domain_to_label = _build_domain_to_label(ctx, label_map, domain_to_sql_name)
    join_to_rel = _build_join_to_rel(label_map)

    # --- Resolve FROM clause ---
    from_clause = tree.args.get("from_")
    if from_clause is None:
        return None

    from_tbl = _unwrap_from_table(from_clause.this, tree)
    if from_tbl is None:
        return None

    base_label = _resolve_label(from_tbl, domain_to_label)
    if base_label is None:
        return None

    sql_base_alias = from_tbl.alias or from_tbl.name

    label_to_rel, label_to_many, src_tgt_to_rel = _build_label_to_rel(label_map)

    join_segments, skipped_aliases = _resolve_join_segments(
        tree,
        domain_to_label,
        join_to_rel,
        label_to_rel,
        label_to_many,
        sql_base_alias,
        params,
    )

    # Build short alias map: verbose SQL alias → a, b, c, …
    _letters = list(string.ascii_lowercase)
    all_sql_aliases = [sql_base_alias] + [seg[3] for seg in join_segments]
    alias_map: dict[str, str] = {
        sql_a: _letters[i] if i < len(_letters) else f"n{i}"
        for i, sql_a in enumerate(all_sql_aliases)
    }
    base_alias = alias_map[sql_base_alias]

    # Build label lookup: sql_alias → display label (needed for src node in OPTIONAL MATCH)
    alias_label: dict[str, str] = {sql_base_alias: base_label}
    for _, _, _, tgt_a, tgt_lbl, _, _ in join_segments:
        alias_label[tgt_a] = tgt_lbl

    def _prop_map_for_label(display_lbl: str) -> dict[str, str]:
        type_names = label_map.nodes_by_table.get(display_lbl, [])
        if not type_names:
            return {}
        nm = label_map.nodes.get(type_names[0])
        if nm is None:
            return {}
        return {sql_col: cypher_prop for cypher_prop, sql_col in nm.properties.items()}

    alias_prop_map: dict[str, dict[str, str]] = {
        sql_a: _prop_map_for_label(lbl) for sql_a, lbl in alias_label.items()
    }

    def _node(short: str, label: str) -> str:
        return f"({short}:{label})"

    def _remap(text: str) -> str:
        """Replace verbose SQL aliases with short Cypher aliases and sql_col names with cypher prop names."""
        for sql_a in sorted(alias_map, key=len, reverse=True):
            text = re.sub(rf"\b{re.escape(sql_a)}\b", alias_map[sql_a], text)
        prop_lookup: dict[str, dict[str, str]] = {
            alias_map[sql_a]: pm for sql_a, pm in alias_prop_map.items() if sql_a in alias_map
        }

        def _col_sub(m: re.Match) -> str:
            node_alias, sql_col = m.group(1), m.group(2)
            return f"{node_alias}.{prop_lookup.get(node_alias, {}).get(sql_col, sql_col)}"

        text = re.sub(r"(\b\w+)\.(\w+)\b", _col_sub, text)
        return text

    # --- Build MATCH pattern ---
    required_path = _node(base_alias, base_label)
    for is_optional, rel_type, _, tgt_sql_a, label, _, _ in join_segments:
        if not is_optional:
            rel_str = f"[:{rel_type}]" if rel_type else "[]"
            required_path += f"-{rel_str}->{_node(alias_map[tgt_sql_a], label)}"

    cypher_lines = [f"MATCH {required_path}"]

    # --- Pre-scan SELECT to know which properties are needed per alias ---
    select_exprs = tree.args.get("expressions") or []
    alias_needed_props = _build_alias_needed_props(select_exprs, alias_map, alias_prop_map)

    collected_aliases: dict[str, dict[str, str]] = {}
    _emit_optional_matches(
        join_segments,
        alias_map,
        alias_label,
        base_alias,
        base_label,
        alias_needed_props,
        collected_aliases,
        cypher_lines,
        flat,
        node_only,
        _node,
    )

    array_agg_return: dict[str, str | list[str]] = {}
    _agg_alias_counter = len(alias_map)
    _agg_seen: dict[str, str] = {}
    _agg_seen_label: dict[str, str] = {}

    _agg_alias_counter = _process_array_agg_subqueries(
        select_exprs,
        domain_to_label,
        label_to_rel,
        src_tgt_to_rel,
        alias_map,
        alias_label,
        base_alias,
        base_label,
        sql_base_alias,
        flat,
        _agg_alias_counter,
        _agg_seen,
        _agg_seen_label,
        array_agg_return,
        cypher_lines,
        _letters,
        _prop_map_for_label,
        _node,
    )

    _process_json_subqueries(
        select_exprs,
        domain_to_label,
        label_to_rel,
        src_tgt_to_rel,
        alias_map,
        alias_label,
        base_alias,
        base_label,
        sql_base_alias,
        flat,
        _agg_alias_counter,
        _agg_seen,
        _agg_seen_label,
        array_agg_return,
        cypher_lines,
        _letters,
        _prop_map_for_label,
        _node,
    )

    # --- WHERE ---
    where_expr = tree.args.get("where")
    if where_expr:
        where_sql = _remap(_sql_to_cypher_expr(where_expr.this.sql(dialect="postgres")))
        cypher_lines.append(f"WHERE {where_sql}")

    # --- RETURN ---
    default_sql_alias = sql_base_alias if not join_segments else None
    for _expr in select_exprs:
        if isinstance(_expr, exp.Column) and _expr.table in skipped_aliases:
            return None

    if node_only:
        node_aliases = _build_node_aliases(
            base_alias,
            join_segments,
            alias_map,
            _agg_seen,
        )
        cypher_lines.append(f"RETURN {', '.join(node_aliases)}")
    else:
        return_items = _build_return(
            select_exprs,
            default_sql_alias,
            alias_map,
            alias_prop_map,
            collected_aliases,
            array_agg_return,
            alias_label=alias_label,
            flat_labels=flat,
        )
        cypher_lines.append(f"RETURN {', '.join(return_items)}" if return_items else "RETURN *")

    if not node_only:
        _append_order_by(
            tree,
            alias_map,
            alias_prop_map,
            default_sql_alias,
            cypher_lines,
            _remap,
        )

    _append_skip_limit(
        tree,
        override_limit,
        node_only,
        node_aliases if node_only else [],  # type: ignore[possibly-undefined]
        cypher_lines,
    )

    return "\n".join(cypher_lines)


# --- Extracted helpers for semantic_sql_to_cypher ---


def _build_domain_to_label(
    ctx: object,  # object-ok: circular-import boundary — CompilationContext lives in provisa.compiler
    label_map: CypherLabelMap,
    domain_to_sql_name,
) -> dict[tuple[str, str], str]:
    """Build reverse lookup: (sql_domain, field_name) → node display label."""
    domain_to_label: dict[tuple[str, str], str] = {}
    for _fn, table_meta in ctx.tables.items():  # type: ignore[attr-defined]  # pyright: ignore[reportUnusedVariable]
        type_name = table_meta.type_name
        if type_name not in label_map.nodes:
            continue
        nm = label_map.nodes[type_name]
        sql_domain = domain_to_sql_name(table_meta.domain_id)
        field_key = (
            table_meta.field_name.split("__", 1)[1]
            if "__" in table_meta.field_name
            else table_meta.field_name
        )
        lbl = label_map.display_label(nm)
        domain_to_label[(sql_domain, field_key)] = lbl
        domain_to_label[("", field_key)] = lbl

    for _tn, nm in label_map.nodes.items():  # pyright: ignore[reportUnusedVariable]
        lbl = label_map.display_label(nm)
        _sql_dom = domain_to_sql_name(nm.domain_id) if nm.domain_id else ""
        _tbl = nm.sql_table_name
        domain_to_label.setdefault((_sql_dom, _tbl), lbl)
        domain_to_label.setdefault(("", _tbl), lbl)

    return domain_to_label


def _build_join_to_rel(
    label_map: CypherLabelMap,
) -> dict[tuple[str, str, str], RelationshipMapping]:
    """Build lookup: (src_col, tgt_col, tgt_display_label) → RelationshipMapping."""
    join_to_rel: dict[tuple[str, str, str], RelationshipMapping] = {}
    for rel in label_map.relationships.values():
        tgt_nm = label_map.nodes.get(rel.target_label)
        tgt_display = label_map.display_label(tgt_nm) if tgt_nm is not None else rel.target_label
        join_to_rel[(rel.join_source_column, rel.join_target_column, tgt_display)] = rel
    return join_to_rel


def _build_label_to_rel(
    label_map: CypherLabelMap,
) -> tuple[dict[str, str | None], dict[str, bool], dict[tuple[str, str], str]]:
    """Build label → rel_type, label → many, and (src_label, tgt_label) → rel_type lookups."""
    label_to_rel: dict[str, str | None] = {}
    label_to_many: dict[str, bool] = {}
    src_tgt_to_rel: dict[tuple[str, str], str] = {}
    for rel in label_map.relationships.values():
        src_nm = label_map.nodes.get(rel.source_label)
        src_display = label_map.display_label(src_nm) if src_nm is not None else rel.source_label
        tgt_nm = label_map.nodes.get(rel.target_label)
        tgt_display = label_map.display_label(tgt_nm) if tgt_nm is not None else rel.target_label
        label_to_rel[tgt_display] = rel.rel_type
        label_to_many[tgt_display] = rel.many
        src_tgt_to_rel[(src_display, tgt_display)] = rel.rel_type
    return label_to_rel, label_to_many, src_tgt_to_rel


def _unwrap_from_table(
    from_tbl: exp.Expression,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    tree: exp.Select,
) -> exp.Table | None:
    """Unwrap a FROM clause node into an exp.Table, handling limit-subquery wrappers."""
    if isinstance(from_tbl, exp.Table):
        return from_tbl
    if isinstance(from_tbl, exp.Subquery):
        inner = from_tbl.this
        inner_from = inner.args.get("from_") if isinstance(inner, exp.Select) else None
        inner_exprs = inner.args.get("expressions") or [] if isinstance(inner, exp.Select) else []
        is_star = len(inner_exprs) == 1 and isinstance(inner_exprs[0], exp.Star)
        if inner_from and is_star and isinstance(inner_from.this, exp.Table):
            inner_tbl = inner_from.this
            if from_tbl.alias:
                inner_tbl.set("alias", exp.TableAlias(this=exp.to_identifier(from_tbl.alias)))
            inner_limit = inner.args.get("limit")
            if inner_limit and not tree.args.get("limit"):
                tree.set("limit", inner_limit)
            return inner_tbl
    return None


def _resolve_lateral_inner_lim(
    inner_select: exp.Select,
    params: list | None,
) -> int | None:
    """Extract LIMIT value from a lateral subquery SELECT, resolving parameters if needed."""
    inner_lim_node = inner_select.args.get("limit")
    if inner_lim_node is None:
        return None
    _lim_expr = getattr(inner_lim_node, "expression", None)
    if isinstance(_lim_expr, exp.Literal):
        return int(_lim_expr.sql())
    if isinstance(_lim_expr, exp.Parameter) and params:
        _idx = int(_lim_expr.name) - 1
        return int(params[_idx])
    return None


def _resolve_join_segments(
    tree: exp.Select,
    domain_to_label: dict[tuple[str, str], str],
    join_to_rel: dict[tuple[str, str, str], RelationshipMapping],
    label_to_rel: dict[str, str | None],
    label_to_many: dict[str, bool],
    sql_base_alias: str,
    params: list | None,
) -> tuple[list[tuple[bool, str | None, str, str, str, int | None, bool]], set[str]]:
    """Resolve JOIN clauses into join_segments and collect skipped aliases."""
    joins = tree.args.get("joins") or []
    join_segments: list[tuple[bool, str | None, str, str, str, int | None, bool]] = []
    skipped_aliases: set[str] = set()

    for join in joins:
        join_tbl = join.this
        if not isinstance(join_tbl, exp.Table):
            _process_lateral_join(
                join_tbl,
                domain_to_label,
                label_to_rel,
                label_to_many,
                sql_base_alias,
                params,
                join_segments,
                skipped_aliases,
            )
            continue

        tgt_label = _resolve_label(join_tbl, domain_to_label)
        if tgt_label is None:
            skipped_aliases.add(join_tbl.alias or join_tbl.name)
            continue

        tgt_sql_alias = join_tbl.alias or join_tbl.name
        on_expr = join.args.get("on")
        rel_type = _rel_type_from_on(on_expr, join_to_rel, tgt_label) or label_to_rel.get(tgt_label)
        src_sql_alias = _src_alias_from_on(on_expr, tgt_sql_alias, sql_base_alias)
        is_optional = (join.side or "").upper() == "LEFT"
        join_segments.append(
            (
                is_optional,
                rel_type,
                src_sql_alias,
                tgt_sql_alias,
                tgt_label,
                None,
                label_to_many.get(tgt_label, False),
            )
        )

    return join_segments, skipped_aliases


def _process_lateral_join(
    join_tbl: exp.Expression,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    domain_to_label: dict[tuple[str, str], str],
    label_to_rel: dict[str, str | None],
    label_to_many: dict[str, bool],
    sql_base_alias: str,
    params: list | None,
    join_segments: list[tuple[bool, str | None, str, str, str, int | None, bool]],
    skipped_aliases: set[str],
) -> None:
    """Try to unwrap a LATERAL join and append a segment, or add to skipped_aliases."""
    lateral_alias = join_tbl.alias if hasattr(join_tbl, "alias") else None
    inner_tbl = None
    subquery_node = join_tbl.this if isinstance(join_tbl, exp.Lateral) else join_tbl
    inner_select: exp.Select | None = None
    if isinstance(subquery_node, exp.Subquery):
        inner_select = subquery_node.this if isinstance(subquery_node.this, exp.Select) else None
        if inner_select is not None:
            inner_from = inner_select.args.get("from_")
            if inner_from and isinstance(inner_from.this, exp.Table):
                inner_tbl = inner_from.this

    if inner_tbl is not None and lateral_alias and inner_select is not None:
        tgt_label = _resolve_label(inner_tbl, domain_to_label)
        if tgt_label is not None:
            rel_type = label_to_rel.get(tgt_label)
            inner_lim = _resolve_lateral_inner_lim(inner_select, params)
            inner_where = inner_select.args.get("where")
            lateral_src_alias = _src_alias_from_on(inner_where, lateral_alias, sql_base_alias)
            join_segments.append(
                (
                    True,
                    rel_type,
                    lateral_src_alias,
                    lateral_alias,
                    tgt_label,
                    inner_lim,
                    label_to_many.get(tgt_label, False),
                )
            )
            return

    if lateral_alias:
        skipped_aliases.add(lateral_alias)


def _build_alias_needed_props(
    select_exprs: list[exp.Expression],  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    alias_map: dict[str, str],
    alias_prop_map: dict[str, dict[str, str]],
) -> dict[str, list[tuple[str, str]]]:
    """Pre-scan SELECT to determine which properties are needed per short alias."""
    alias_needed_props: dict[str, list[tuple[str, str]]] = {}
    for _expr in select_exprs:
        if isinstance(_expr, exp.Column) and _expr.table:
            _tbl_short = alias_map.get(_expr.table, _expr.table)
            _sql_col = _expr.name
            _cypher_prop = alias_prop_map.get(_expr.table, {}).get(_sql_col, _sql_col)
            alias_needed_props.setdefault(_tbl_short, []).append((_sql_col, _cypher_prop))
    return alias_needed_props


def _emit_optional_matches(
    join_segments: list[tuple[bool, str | None, str, str, str, int | None, bool]],
    alias_map: dict[str, str],
    alias_label: dict[str, str],
    base_alias: str,
    base_label: str,
    alias_needed_props: dict[str, list[tuple[str, str]]],
    collected_aliases: dict[str, dict[str, str]],
    cypher_lines: list[str],
    flat: bool,
    node_only: bool,
    node_fn,
) -> None:
    """Emit OPTIONAL MATCH or CALL {} blocks for optional join segments."""
    for is_optional, rel_type, src_sql_a, tgt_sql_a, label, inner_lim, is_many in join_segments:
        if not is_optional:
            continue
        rel_str = f"[:{rel_type}]" if rel_type else "[]"
        src_short = alias_map.get(src_sql_a, base_alias)
        src_lbl = alias_label.get(src_sql_a, base_label)
        tgt_short = alias_map[tgt_sql_a]
        match_line = (
            f"OPTIONAL MATCH {node_fn(src_short, src_lbl)}-{rel_str}->{node_fn(tgt_short, label)}"
        )
        if not flat and not node_only and (inner_lim is not None or is_many):
            props = alias_needed_props.get(tgt_short, [])
            if props:
                prop_map: dict[str, str] = {}
                return_parts = []
                for _, _cypher_prop in props:
                    prop_list_var = f"{tgt_short}_{_cypher_prop}_list"
                    prop_map[_cypher_prop] = prop_list_var
                    slice_suffix = f"[..{inner_lim}]" if inner_lim is not None else ""
                    return_parts.append(
                        f"collect({tgt_short}.{_cypher_prop}){slice_suffix} AS {prop_list_var}"
                    )
                collected_aliases[tgt_short] = prop_map
                cypher_lines.append(
                    f"CALL {{\n"
                    f"  WITH {src_short}\n"
                    f"  {match_line}\n"
                    f"  RETURN {', '.join(return_parts)}\n"
                    f"}}"
                )
            else:
                cypher_lines.append(match_line)
        else:
            cypher_lines.append(match_line)


def _build_node_aliases(
    base_alias: str,
    join_segments: list[tuple[bool, str | None, str, str, str, int | None, bool]],
    alias_map: dict[str, str],
    agg_seen: dict[str, str],
) -> list[str]:
    """Build ordered unique list of node aliases for node_only RETURN."""
    node_aliases: list[str] = [base_alias]
    for _, _, _, tgt_sql_a, _, _, _ in join_segments:
        short = alias_map.get(tgt_sql_a)
        if short and short not in node_aliases:
            node_aliases.append(short)
    for short in agg_seen.values():
        if short not in node_aliases:
            node_aliases.append(short)
    return node_aliases


def _append_order_by(
    tree: exp.Select,
    alias_map: dict[str, str],
    alias_prop_map: dict[str, dict[str, str]],
    default_sql_alias: str | None,
    cypher_lines: list[str],
    remap_fn,
) -> None:
    """Append ORDER BY clause to cypher_lines if present."""
    order = tree.args.get("order")
    if not order:
        return
    order_items = []
    for o in order.expressions:
        col_expr = o.this
        if isinstance(col_expr, exp.Column) and not col_expr.table and default_sql_alias:
            prop = alias_prop_map.get(default_sql_alias, {}).get(col_expr.name, col_expr.name)
            col_sql = f"{alias_map[default_sql_alias]}.{prop}"
        else:
            col_sql = remap_fn(_sql_to_cypher_expr(col_expr.sql(dialect="postgres")))
        direction = " DESC" if o.args.get("desc") else ""
        order_items.append(f"{col_sql}{direction}")
    cypher_lines.append(f"ORDER BY {', '.join(order_items)}")


def _resolve_limit_expr(lim_node: exp.Expression) -> str:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    lim_expr = getattr(lim_node, "expression", None)
    if isinstance(lim_expr, exp.Parameter):
        return "25"
    return lim_expr.sql() if lim_expr is not None else "25"


def _append_skip_limit(
    tree: exp.Select,
    override_limit: int | None,
    node_only: bool,
    node_aliases: list[str],
    cypher_lines: list[str],
) -> None:
    """Append SKIP and LIMIT clauses to cypher_lines."""
    offset = tree.args.get("offset")
    limit = tree.args.get("limit")
    if offset:
        cypher_lines.append(f"SKIP {offset.expression.sql()}")

    if node_only:
        cypher_lines.append(
            "LIMIT 25"
            if len(node_aliases) > 1
            else (
                f"LIMIT {override_limit}"
                if override_limit is not None
                else (f"LIMIT {_resolve_limit_expr(limit)}" if limit else "LIMIT 25")
            )
        )
    elif override_limit is not None:
        cypher_lines.append(f"LIMIT {override_limit}")
    elif limit:
        cypher_lines.append(f"LIMIT {_resolve_limit_expr(limit)}")


def _build_return(
    select_exprs: list[exp.Expression],  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    default_sql_alias: str | None = None,
    alias_map: dict[str, str] | None = None,
    alias_prop_map: dict[str, dict[str, str]] | None = None,
    collected_aliases: dict[str, dict[str, str]] | None = None,
    array_agg_return: dict[str, str | list[str]] | None = None,
    alias_label: dict[str, str] | None = None,
    flat_labels: bool = False,
) -> list[str]:
    """Convert SELECT expressions to RETURN items.

    For aliases in collected_aliases (short_alias → {cypher_prop → prop_list_var}),
    each property column is returned as a direct list reference:
      prop_list_var AS short_alias_cypher_prop
    This avoids cartesian products when multiple multi-valued traversals are present.
    For aliases in array_agg_return (output_alias → collect(short.prop) or list[str]),
    emit the collect() form or expand the flat list directly.
    When flat_labels=True, root Column items are aliased as label__prop.
    """
    am = alias_map or {}
    apm = alias_prop_map or {}
    ca = collected_aliases or {}
    aar = array_agg_return or {}
    al = alias_label or {}

    def _short(sql_tbl: str) -> str:
        return am.get(sql_tbl, sql_tbl)

    def _prop(sql_tbl: str, sql_col: str) -> str:
        return apm.get(sql_tbl, {}).get(sql_col, sql_col)

    items: list[str] = []
    for expr in select_exprs:
        if isinstance(expr, exp.Star):
            return ["*"]
        if isinstance(expr, exp.Column):
            raw_tbl = expr.table or default_sql_alias or ""
            tbl = _short(raw_tbl)
            col = expr.name or "*"
            if col == "*":
                items.append(tbl if tbl else "*")
            elif tbl in ca:
                cypher_prop = _prop(raw_tbl, col)
                prop_list_var = ca[tbl].get(cypher_prop)
                if prop_list_var:
                    items.append(f"{prop_list_var} AS {tbl}_{cypher_prop}")
            else:
                cypher_prop = _prop(raw_tbl, col)
                if flat_labels and al:
                    lbl = al.get(raw_tbl)
                    lbl_prefix = lbl.lower() if lbl else tbl
                    items.append(
                        f"{tbl}.{cypher_prop} AS {lbl_prefix}__{cypher_prop}"
                        if tbl
                        else cypher_prop
                    )
                else:
                    items.append(f"{tbl}.{cypher_prop}" if tbl else cypher_prop)
        elif isinstance(expr, exp.Alias):
            if expr.alias in aar:
                val = aar[expr.alias]
                if isinstance(val, list):
                    items.extend(val)
                else:
                    items.append(f"{val} AS {expr.alias}")
            else:
                raw = _sql_to_cypher_expr(expr.this.sql(dialect="postgres"))
                for sql_a in sorted(am, key=len, reverse=True):
                    raw = re.sub(rf"\b{re.escape(sql_a)}\b", am[sql_a], raw)
                items.append(f"{raw} AS {expr.alias}")
        else:
            raw = _sql_to_cypher_expr(expr.sql(dialect="postgres"))
            for sql_a in sorted(am, key=len, reverse=True):
                raw = re.sub(rf"\b{re.escape(sql_a)}\b", am[sql_a], raw)
            items.append(raw)
    return items or ["*"]


def _offset_aliases(cypher: str, offset: int) -> tuple[str, int]:
    """Rename single-letter node aliases to start at the given letter offset.

    Aliases are discovered from MATCH pattern definitions (e.g. ``(a:Label)``),
    then every word-boundary occurrence is substituted.  Returns the rewritten
    Cypher and the next available offset.
    """
    letters = list(string.ascii_lowercase)
    defined = sorted(set(re.findall(r"\(([a-z])(?:[:\s)])", cypher)))
    rename = {
        old: letters[offset + i] if (offset + i) < len(letters) else f"n{offset + i}"
        for i, old in enumerate(defined)
    }
    result = cypher
    for old in sorted(rename, key=len, reverse=True):
        result = re.sub(rf"\b{re.escape(old)}\b", rename[old], result)
    return result, offset + len(defined)


def combine_cypher_queries(cyphers: list[str]) -> str:  # REQ-571
    """Combine independent per-root Cypher queries into a single CALL {} query.

    Each query is wrapped in a CALL {} subquery; the outer RETURN collects all
    projected items so callers get a single unified query.  Aliases are
    offset per subquery so no two roots share the same alias letter.
    """
    if len(cyphers) == 1:
        return cyphers[0]

    wrapped: list[str] = []
    all_return_items: list[str] = []
    alias_offset = 0

    for cypher in cyphers:
        renamed, alias_offset = _offset_aliases(cypher, alias_offset)
        lines = renamed.strip().splitlines()
        # Extract RETURN items to re-expose them in the outer RETURN
        for line in lines:
            stripped = line.strip()
            if re.match(r"RETURN\s+", stripped, re.IGNORECASE):
                items_str = re.sub(r"^RETURN\s+", "", stripped, flags=re.IGNORECASE)
                if items_str.strip() != "*":
                    all_return_items.extend(item.strip() for item in items_str.split(","))
                break
        indented = "\n".join(f"  {line}" for line in lines)
        wrapped.append(f"CALL {{\n{indented}\n}}")

    combined = "\n".join(wrapped)
    if all_return_items:
        combined += f"\nRETURN {', '.join(all_return_items)}"
    else:
        combined += "\nRETURN *"

    return combined
