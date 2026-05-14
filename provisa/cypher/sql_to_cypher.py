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



def semantic_sql_to_cypher(
    semantic_sql: str,
    label_map: CypherLabelMap,
    ctx: object,
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

    # Build reverse lookups: (sql_domain, field_name) → node label
    domain_to_label: dict[tuple[str, str], str] = {}
    for _fn, table_meta in ctx.tables.items():  # type: ignore[attr-defined]
        type_name = table_meta.type_name
        if type_name not in label_map.nodes:
            continue
        nm = label_map.nodes[type_name]
        sql_domain = domain_to_sql_name(table_meta.domain_id)
        # Strip domain prefix from field_name — same logic as _semantic_table_ref:
        # "sa__orders" → "orders" so lookups match the parsed semantic SQL table name.
        field_key = (
            table_meta.field_name.split("__", 1)[1]
            if "__" in table_meta.field_name
            else table_meta.field_name
        )
        lbl = label_map.display_label(nm)
        domain_to_label[(sql_domain, field_key)] = lbl
        domain_to_label[("", field_key)] = lbl

    # Also cover traversal-only nodes (in label_map.nodes but not in ctx.tables)
    for _tn, nm in label_map.nodes.items():
        lbl = label_map.display_label(nm)
        _sql_dom = domain_to_sql_name(nm.domain_id) if nm.domain_id else ""
        _tbl = nm.sql_table_name
        domain_to_label.setdefault((_sql_dom, _tbl), lbl)
        domain_to_label.setdefault(("", _tbl), lbl)

    # Build reverse lookup for relationships: (src_col, tgt_col) → RelationshipMapping
    join_to_rel: dict[tuple[str, str], RelationshipMapping] = {}
    for rel in label_map.relationships.values():
        join_to_rel[(rel.join_source_column, rel.join_target_column)] = rel
        join_to_rel[(rel.join_target_column, rel.join_source_column)] = rel

    # --- Resolve FROM clause ---
    from_clause = tree.args.get("from_")
    if from_clause is None:
        return None

    # sqlglot stores the Table directly (with alias embedded) in from_.this
    from_tbl = from_clause.this
    if not isinstance(from_tbl, exp.Table):
        # Unwrap simple limit subquery: (SELECT * FROM table LIMIT N) alias
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
                from_tbl = inner_tbl
        if not isinstance(from_tbl, exp.Table):
            return None  # non-unwrappable subquery in FROM

    base_label = _resolve_label(from_tbl, domain_to_label)
    if base_label is None:
        return None

    sql_base_alias = from_tbl.alias or from_tbl.name

    # --- Resolve JOINs → relationship segments ---
    joins = tree.args.get("joins") or []
    # Each entry: (is_optional, rel_type | None, src_sql_alias, tgt_sql_alias, tgt_label, inner_limit | None, many)
    join_segments: list[tuple[bool, str | None, str, str, str, int | None, bool]] = []

    # Build label → rel_type lookup for use with LATERAL joins (no ON condition to parse).
    # Key by display_label (e.g. "RegisteredTables"), not type_name (e.g. "Meta_RegisteredTables"),
    # because _resolve_label returns display_label and must match here.
    label_to_rel: dict[str, str | None] = {}
    label_to_many: dict[str, bool] = {}
    for rel in label_map.relationships.values():
        tgt_nm = label_map.nodes.get(rel.target_label)
        tgt_display = label_map.display_label(tgt_nm) if tgt_nm is not None else rel.target_label
        label_to_rel[tgt_display] = rel.rel_type
        label_to_many[tgt_display] = rel.many

    skipped_aliases: set[str] = set()
    for join in joins:
        join_tbl = join.this
        if not isinstance(join_tbl, exp.Table):
            # Try to unwrap LATERAL (SELECT * FROM actual_tbl WHERE ...) subquery
            lateral_alias = join_tbl.alias if hasattr(join_tbl, "alias") else None
            inner_tbl = None
            # Unwrap Lateral(Subquery(Select(...))) or bare Subquery(Select(...))
            subquery_node = (
                join_tbl.this if isinstance(join_tbl, exp.Lateral) else join_tbl
            )
            if isinstance(subquery_node, exp.Subquery):
                inner_select = subquery_node.this
                if isinstance(inner_select, exp.Select):
                    inner_from = inner_select.args.get("from_")
                    if inner_from and isinstance(inner_from.this, exp.Table):
                        inner_tbl = inner_from.this
            if inner_tbl is not None and lateral_alias:
                tgt_label = _resolve_label(inner_tbl, domain_to_label)
                if tgt_label is not None:
                    rel_type = label_to_rel.get(tgt_label)
                    inner_lim_node = inner_select.args.get("limit") if isinstance(inner_select, exp.Select) else None
                    inner_lim: int | None = None
                    if inner_lim_node is not None:
                        _lim_expr = getattr(inner_lim_node, "expression", None)
                        if isinstance(_lim_expr, exp.Literal):
                            inner_lim = int(_lim_expr.sql())
                        elif isinstance(_lim_expr, exp.Parameter) and params:
                            # sql_gen emits LIMIT $N (parameterized); Parameter.name is "1", "2", ...
                            _idx = int(_lim_expr.name) - 1
                            inner_lim = int(params[_idx])
                    # Determine source alias from the lateral subquery's WHERE clause.
                    # The WHERE condition ties the lateral to its source via a column equality
                    # (e.g. _meta_alias.table_name = lateral_alias.table_name).  The table
                    # qualifier that is NOT the lateral alias is the actual source.
                    inner_where = inner_select.args.get("where") if isinstance(inner_select, exp.Select) else None
                    lateral_src_alias = _src_alias_from_on(inner_where, lateral_alias, sql_base_alias)
                    join_segments.append((True, rel_type, lateral_src_alias, lateral_alias, tgt_label, inner_lim, label_to_many.get(tgt_label, False)))
                    continue
            if lateral_alias:
                skipped_aliases.add(lateral_alias)
            continue  # non-unwrappable or non-graph LATERAL — skip

        tgt_label = _resolve_label(join_tbl, domain_to_label)
        if tgt_label is None:
            skipped_aliases.add(join_tbl.alias or join_tbl.name)
            continue  # meta/ops table not in graph schema — skip

        tgt_sql_alias = join_tbl.alias or join_tbl.name

        on_expr = join.args.get("on")
        rel_type = _rel_type_from_on(on_expr, join_to_rel) or label_to_rel.get(tgt_label)
        # Determine source alias from ON condition table references
        src_sql_alias = _src_alias_from_on(on_expr, tgt_sql_alias, sql_base_alias)
        is_optional = (join.side or "").upper() == "LEFT"
        join_segments.append((is_optional, rel_type, src_sql_alias, tgt_sql_alias, tgt_label, None, label_to_many.get(tgt_label, False)))

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
    for _is_opt, _rt, _src, tgt_a, tgt_lbl, _il, _many in join_segments:
        alias_label[tgt_a] = tgt_lbl

    # Build sql_alias → {sql_col: cypher_prop} from NodeMapping.properties (inverted)
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
            text = re.sub(rf'\b{re.escape(sql_a)}\b', alias_map[sql_a], text)
        # Rewrite short_alias.sql_col → short_alias.cypherProp using prop map
        prop_lookup: dict[str, dict[str, str]] = {
            alias_map[sql_a]: pm for sql_a, pm in alias_prop_map.items() if sql_a in alias_map
        }
        def _col_sub(m: re.Match) -> str:
            node_alias, sql_col = m.group(1), m.group(2)
            return f"{node_alias}.{prop_lookup.get(node_alias, {}).get(sql_col, sql_col)}"
        text = re.sub(r'(\b\w+)\.(\w+)\b', _col_sub, text)
        return text

    # --- Build MATCH pattern ---
    required_path = _node(base_alias, base_label)
    for is_optional, rel_type, src_sql_a, tgt_sql_a, label, _il, _many in join_segments:
        if not is_optional:
            rel_str = f"[:{rel_type}]" if rel_type else "[]"
            required_path += f"-{rel_str}->{_node(alias_map[tgt_sql_a], label)}"

    cypher_lines = [f"MATCH {required_path}"]

    # --- Pre-scan SELECT to know which properties are needed per alias ---
    select_exprs = tree.args.get("expressions") or []
    # short_alias → [(sql_col, cypher_prop), ...]
    alias_needed_props: dict[str, list[tuple[str, str]]] = {}
    for _expr in select_exprs:
        if isinstance(_expr, exp.Column) and _expr.table:
            _tbl_short = alias_map.get(_expr.table, _expr.table)
            _sql_col = _expr.name
            _cypher_prop = alias_prop_map.get(_expr.table, {}).get(_sql_col, _sql_col)
            alias_needed_props.setdefault(_tbl_short, []).append((_sql_col, _cypher_prop))

    # short_alias → {cypher_prop → per-property list var name}
    # Used by _build_return to emit direct list references instead of list comprehensions.
    collected_aliases: dict[str, dict[str, str]] = {}

    for is_optional, rel_type, src_sql_a, tgt_sql_a, label, inner_lim, is_many in join_segments:
        if is_optional:
            rel_str = f"[:{rel_type}]" if rel_type else "[]"
            src_short = alias_map.get(src_sql_a, base_alias)
            src_lbl = alias_label.get(src_sql_a, base_label)
            tgt_short = alias_map[tgt_sql_a]
            match_line = (
                f"OPTIONAL MATCH {_node(src_short, src_lbl)}"
                f"-{rel_str}->{_node(tgt_short, label)}"
            )
            if not flat and not node_only and (inner_lim is not None or is_many):
                props = alias_needed_props.get(tgt_short, [])
                if props:
                    # Collect individual properties to avoid ARRAY_AGG(table_alias) (issue #49).
                    prop_map: dict[str, str] = {}
                    return_parts = []
                    for _sql_col, _cypher_prop in props:
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
                # If no properties selected from this alias, fall through to flat OPTIONAL MATCH.
                else:
                    cypher_lines.append(match_line)
            else:
                cypher_lines.append(match_line)

    # --- ARRAY_AGG subqueries → OPTIONAL MATCH + collect() ---
    # Non-flat one-to-many joins emit correlated ARRAY_AGG subqueries in the SQL SELECT list.
    # Translate each to an OPTIONAL MATCH on the inner table + collect() in RETURN.
    array_agg_return: dict[str, str] = {}  # output SQL alias → collect(short.prop) expr
    _agg_alias_counter = len(alias_map)
    _agg_seen: dict[str, str] = {}  # inner_sql_alias → assigned short alias
    _agg_seen_label: dict[str, str] = {}  # inner_sql_alias → display label

    for _expr in select_exprs:
        if not (isinstance(_expr, exp.Alias) and isinstance(_expr.this, exp.Subquery)):
            continue
        _inner = _expr.this.this
        if not isinstance(_inner, exp.Select):
            continue
        _agg_exprs = _inner.args.get("expressions") or []
        if len(_agg_exprs) != 1:
            continue
        _agg_node = _agg_exprs[0]
        if isinstance(_agg_node, exp.ArrayAgg):
            _agg_col_node = _agg_node.this
        elif isinstance(_agg_node, exp.Anonymous) and _agg_node.name.upper() == "ARRAY_AGG":
            _agg_cols = getattr(_agg_node, "expressions", [])
            _agg_col_node = _agg_cols[0] if _agg_cols else None
        else:
            continue
        if not isinstance(_agg_col_node, exp.Column):
            continue
        _inner_from = _inner.args.get("from_")
        if not _inner_from:
            continue
        # Handle both direct table and LIMIT-wrapped subquery:
        # ARRAY_AGG(col) FROM table WHERE ...
        # ARRAY_AGG(col) FROM (SELECT col FROM table WHERE ... LIMIT N)
        if isinstance(_inner_from.this, exp.Table):
            _inner_tbl = _inner_from.this
            _where_node = _inner.args.get("where")
        elif isinstance(_inner_from.this, exp.Subquery):
            _limit_select = _inner_from.this.this
            if not isinstance(_limit_select, exp.Select):
                continue
            _lf = _limit_select.args.get("from_")
            if not (_lf and isinstance(_lf.this, exp.Table)):
                continue
            _inner_tbl = _lf.this
            _where_node = _limit_select.args.get("where")
        else:
            continue
        _tgt_lbl = _resolve_label(_inner_tbl, domain_to_label)
        if _tgt_lbl is None:
            continue
        _inner_sql_alias = _inner_tbl.alias or _inner_tbl.name
        # Source side of join condition (WHERE inner_alias.col = src_alias.col)
        _src_sql = sql_base_alias
        if _where_node:
            for _eq in _where_node.find_all(exp.EQ):
                for _wc in (_eq.this, _eq.expression):
                    if isinstance(_wc, exp.Column) and _wc.table and _wc.table != _inner_sql_alias:
                        _src_sql = _wc.table
                        break

        # When aggregated column comes from a JOIN target inside the subquery
        # (e.g. ARRAY_AGG(t2.lastName) FROM assignments t1 JOIN employees t2 ...),
        # emit an OPTIONAL MATCH for the FROM table then chain to the JOIN target.
        _col_table = _agg_col_node.table
        if _col_table and _col_table != _inner_sql_alias:
            # Find the JOIN inside the subquery that has alias _col_table.
            _inner_joins = _inner.args.get("joins") or []
            _join_tbl_node = next(
                (
                    j.this
                    for j in _inner_joins
                    if isinstance(j.this, exp.Table)
                    and (j.this.alias or j.this.name) == _col_table
                ),
                None,
            )
            if _join_tbl_node is None:
                continue
            _eff_lbl = _resolve_label(_join_tbl_node, domain_to_label)
            if _eff_lbl is None:
                continue
            # Ensure the FROM-table OPTIONAL MATCH is emitted (assignments → …).
            if _inner_sql_alias not in _agg_seen:
                _arr_short = _letters[_agg_alias_counter] if _agg_alias_counter < len(_letters) else f"n{_agg_alias_counter}"
                _agg_alias_counter += 1
                _agg_seen[_inner_sql_alias] = _arr_short
                _src_short = alias_map.get(_src_sql, base_alias)
                _src_lbl = alias_label.get(_src_sql, base_label)
                _parent_rel_type = label_to_rel.get(_tgt_lbl)
                _parent_rel_str = f"[:{_parent_rel_type}]" if _parent_rel_type else "[]"
                cypher_lines.append(
                    f"OPTIONAL MATCH {_node(_src_short, _src_lbl)}-{_parent_rel_str}->{_node(_arr_short, _tgt_lbl)}"
                )
            # Emit the JOIN-target OPTIONAL MATCH chained from the FROM-table node.
            if _col_table not in _agg_seen:
                _join_short = _letters[_agg_alias_counter] if _agg_alias_counter < len(_letters) else f"n{_agg_alias_counter}"
                _agg_alias_counter += 1
                _agg_seen[_col_table] = _join_short
                _from_node_short = _agg_seen[_inner_sql_alias]
                _join_rel_type = label_to_rel.get(_eff_lbl)
                _join_rel_str = f"[:{_join_rel_type}]" if _join_rel_type else "[]"
                cypher_lines.append(
                    f"OPTIONAL MATCH {_node(_from_node_short, _tgt_lbl)}-{_join_rel_str}->{_node(_join_short, _eff_lbl)}"
                )
            _agg_sql_col = _agg_col_node.name
            _cypher_prop = _prop_map_for_label(_eff_lbl).get(_agg_sql_col, _agg_sql_col)
            _prop_ref = f"{_agg_seen[_col_table]}.{_cypher_prop}"
            array_agg_return[_expr.alias] = _prop_ref if flat else f"collect({_prop_ref})"
            continue

        _agg_sql_col = _agg_col_node.name
        _cypher_prop = _prop_map_for_label(_tgt_lbl).get(_agg_sql_col, _agg_sql_col)

        if _inner_sql_alias not in _agg_seen:
            _arr_short = _letters[_agg_alias_counter] if _agg_alias_counter < len(_letters) else f"n{_agg_alias_counter}"
            _agg_alias_counter += 1
            _agg_seen[_inner_sql_alias] = _arr_short
            _src_short = alias_map.get(_src_sql, base_alias)
            _src_lbl = alias_label.get(_src_sql, base_label)
            _agg_rel_type = label_to_rel.get(_tgt_lbl)
            _agg_rel_str = f"[:{_agg_rel_type}]" if _agg_rel_type else "[]"
            cypher_lines.append(
                f"OPTIONAL MATCH {_node(_src_short, _src_lbl)}-{_agg_rel_str}->{_node(_arr_short, _tgt_lbl)}"
            )

        _prop_ref = f"{_agg_seen[_inner_sql_alias]}.{_cypher_prop}"
        array_agg_return[_expr.alias] = _prop_ref if flat else f"collect({_prop_ref})"

    # --- json_agg(json_object(...)) / json_object(...) subqueries ---
    # _build_rel_json_expr emits one correlated subquery per relationship using
    # SQL-standard json_object(KEY k VALUE v, ...) which sqlglot parses as exp.JSONObject.
    # Walk each top-level Alias(Subquery) that contains json_agg or json_object
    # and emit OPTIONAL MATCH chains for every reachable table.
    for _expr in select_exprs:
        if not (isinstance(_expr, exp.Alias) and isinstance(_expr.this, exp.Subquery)):
            continue
        _outer_sel = _expr.this.this
        if not isinstance(_outer_sel, exp.Select):
            continue

        # Determine top-level output alias (e.g. "assignment")
        _top_alias = _expr.alias

        # Work queue: (parent_sql_alias, inner_select_node)
        # parent_sql_alias is the SQL table alias whose OPTIONAL MATCH we chain from.
        _queue: list[tuple[str, exp.Select]] = []

        # Detect json_agg(json_object) or json_agg(_t) [LIMIT-wrapped form]
        # or bare json_object (many-to-one, LIMIT 1).
        _outer_exprs = _outer_sel.args.get("expressions") or []
        if len(_outer_exprs) != 1:
            continue
        _outer_agg = _outer_exprs[0]

        # Resolve the actual SELECT that contains json_object
        _jbo_sel: exp.Select | None = None
        if isinstance(_outer_agg, exp.JSONArrayAgg):
            _inner_arg = _outer_agg.this
            if isinstance(_inner_arg, exp.JSONObject):
                # Direct form: json_agg(json_object(...))
                _jbo_sel = _outer_sel
            elif isinstance(_inner_arg, exp.Column) and _inner_arg.name == "_t":
                # LIMIT-wrapped: json_agg(_t) FROM (SELECT json_object(...) AS _t ...)
                _from_outer = _outer_sel.args.get("from_")
                if _from_outer and isinstance(_from_outer.this, exp.Subquery):
                    _inner_limited = _from_outer.this.this
                    if isinstance(_inner_limited, exp.Select):
                        _jbo_sel = _inner_limited
        elif isinstance(_outer_agg, exp.JSONObject):
            # Many-to-one (LIMIT 1): SELECT json_object(...)
            _jbo_sel = _outer_sel
        else:
            continue

        if _jbo_sel is None:
            continue

        _from_node = _jbo_sel.args.get("from_")
        if not (_from_node and isinstance(_from_node.this, exp.Table)):
            continue

        _queue.append((sql_base_alias, _jbo_sel))

        while _queue:
            _parent_sql_alias, _sel = _queue.pop(0)

            _sel_from = _sel.args.get("from_")
            if not (_sel_from and isinstance(_sel_from.this, exp.Table)):
                continue

            _tbl = _sel_from.this
            _tgt_lbl = _resolve_label(_tbl, domain_to_label)
            if _tgt_lbl is None:
                continue

            _inner_sql_alias = _tbl.alias or _tbl.name
            _where_nd = _sel.args.get("where")

            # Resolve source alias from WHERE condition
            _src_sql = _parent_sql_alias
            if _where_nd:
                for _eq in _where_nd.find_all(exp.EQ):
                    for _wc in (_eq.this, _eq.expression):
                        if isinstance(_wc, exp.Column) and _wc.table and _wc.table != _inner_sql_alias:
                            _src_sql = _wc.table
                            break

            if _inner_sql_alias not in _agg_seen:
                _arr_short = _letters[_agg_alias_counter] if _agg_alias_counter < len(_letters) else f"n{_agg_alias_counter}"
                _agg_alias_counter += 1
                _agg_seen[_inner_sql_alias] = _arr_short
                _agg_seen_label[_inner_sql_alias] = _tgt_lbl
                _src_short = alias_map.get(_src_sql) or _agg_seen.get(_src_sql, base_alias)
                _src_lbl = alias_label.get(_src_sql) or _agg_seen_label.get(_src_sql, base_label)
                _rel_type = label_to_rel.get(_tgt_lbl)
                _rel_str = f"[:{_rel_type}]" if _rel_type else "[]"
                cypher_lines.append(
                    f"OPTIONAL MATCH {_node(_src_short, _src_lbl)}-{_rel_str}->{_node(_arr_short, _tgt_lbl)}"
                )

            # Find nested subqueries inside json_object args (JSONKeyValue children)
            _sel_exprs = _sel.args.get("expressions") or []
            for _se in _sel_exprs:
                # Could be JSONObject or Alias(JSONObject)
                _jbo_node = _se if isinstance(_se, exp.JSONObject) else (
                    _se.this if isinstance(_se, exp.Alias) and isinstance(_se.this, exp.JSONObject) else None
                )
                if _jbo_node is None:
                    continue
                # Each child is a JSONKeyValue; check .expression (the value side) for Subquery
                for _kv in _jbo_node.expressions:
                    if not isinstance(_kv, exp.JSONKeyValue):
                        continue
                    _val = _kv.expression
                    # Unwrap Subquery → Select
                    _sub_sel: exp.Select | None = None
                    if isinstance(_val, exp.Subquery) and isinstance(_val.this, exp.Select):
                        _sub_sel = _val.this
                    if _sub_sel is None:
                        continue
                    # Determine the actual Select containing json_object
                    _sub_outer_exprs = _sub_sel.args.get("expressions") or []
                    if not _sub_outer_exprs:
                        continue
                    _sub_agg = _sub_outer_exprs[0]
                    if isinstance(_sub_agg, exp.JSONArrayAgg):
                        _sub_inner = _sub_agg.this
                        if isinstance(_sub_inner, exp.JSONObject):
                            _queue.append((_inner_sql_alias, _sub_sel))
                        elif isinstance(_sub_inner, exp.Column) and _sub_inner.name == "_t":
                            _sub_from_nd = _sub_sel.args.get("from_")
                            if _sub_from_nd and isinstance(_sub_from_nd.this, exp.Subquery):
                                _sub_limited = _sub_from_nd.this.this
                                if isinstance(_sub_limited, exp.Select):
                                    _queue.append((_inner_sql_alias, _sub_limited))
                    elif isinstance(_sub_agg, exp.JSONObject):
                        _queue.append((_inner_sql_alias, _sub_sel))

        # Build RETURN expression after queue is fully processed (all aliases populated).
        if _top_alias not in array_agg_return:
            # Extract the top-level JSONObject from _jbo_sel to build the map literal.
            _jbo_node_for_ret: exp.JSONObject | None = None
            for _se in (_jbo_sel.args.get("expressions") or []):
                if isinstance(_se, exp.JSONObject):
                    _jbo_node_for_ret = _se
                    break
                if isinstance(_se, exp.Alias) and isinstance(_se.this, exp.JSONObject):
                    _jbo_node_for_ret = _se.this
                    break
                # Direct form: json_agg(json_object(...)) — JSONArrayAgg wraps the JSONObject
                if isinstance(_se, exp.JSONArrayAgg) and isinstance(_se.this, exp.JSONObject):
                    _jbo_node_for_ret = _se.this
                    break
            # First table alias from _jbo_sel (fallback for flat mode)
            _jbo_from = _jbo_sel.args.get("from_")
            _jbo_tbl = _jbo_from.this if _jbo_from and isinstance(_jbo_from.this, exp.Table) else None
            _first_sql_alias = (_jbo_tbl.alias or _jbo_tbl.name) if _jbo_tbl else None

            if not flat and _jbo_node_for_ret is not None:
                _map_expr = _cypher_map_from_jbo(
                    _jbo_node_for_ret, _agg_seen, _agg_seen_label, _prop_map_for_label, flat
                )
                array_agg_return[_top_alias] = f"collect({_map_expr})"
            elif flat and _jbo_node_for_ret is not None:
                _flat_items = _flat_return_items_from_jbo(
                    _jbo_node_for_ret, _agg_seen, _agg_seen_label, _prop_map_for_label
                )
                array_agg_return[_top_alias] = _flat_items or _agg_seen.get(_first_sql_alias or "", base_alias)
            else:
                array_agg_return[_top_alias] = _agg_seen.get(_first_sql_alias or "", base_alias)

    # --- WHERE ---
    where_expr = tree.args.get("where")
    if where_expr:
        where_sql = _remap(_sql_to_cypher_expr(where_expr.this.sql(dialect="postgres")))
        cypher_lines.append(f"WHERE {where_sql}")

    # --- RETURN ---
    default_sql_alias = sql_base_alias if not join_segments else None
    # If any SELECT column references a skipped (non-graph) alias, Cypher can't express it
    for _expr in select_exprs:
        if isinstance(_expr, exp.Column) and _expr.table in skipped_aliases:
            return None

    if node_only:
        # Emit unique node aliases only (a, b, c …) — no property dotted paths.
        # Preserve insertion order: base alias first, then each join target in order.
        node_aliases: list[str] = [base_alias]
        for _, _, _, tgt_sql_a, _, _, _ in join_segments:
            short = alias_map.get(tgt_sql_a)
            if short and short not in node_aliases:
                node_aliases.append(short)
        # Also include any aliases added by ARRAY_AGG subquery processing.
        for short in _agg_seen.values():
            if short not in node_aliases:
                node_aliases.append(short)
        cypher_lines.append(f"RETURN {', '.join(node_aliases)}")
    else:
        return_items = _build_return(select_exprs, default_sql_alias, alias_map, alias_prop_map, collected_aliases, array_agg_return, alias_label=alias_label, flat_labels=flat)
        cypher_lines.append(f"RETURN {', '.join(return_items)}" if return_items else "RETURN *")

    # --- ORDER BY (skipped in node_only mode — node variables have no stable sort key) ---
    if not node_only:
        order = tree.args.get("order")
        if order:
            order_items = []
            for o in order.expressions:
                col_expr = o.this
                if isinstance(col_expr, exp.Column) and not col_expr.table and default_sql_alias:
                    prop = alias_prop_map.get(default_sql_alias, {}).get(col_expr.name, col_expr.name)
                    col_sql = f"{alias_map[default_sql_alias]}.{prop}"
                else:
                    col_sql = _remap(_sql_to_cypher_expr(col_expr.sql(dialect="postgres")))
                direction = " DESC" if o.args.get("desc") else ""
                order_items.append(f"{col_sql}{direction}")
            cypher_lines.append(f"ORDER BY {', '.join(order_items)}")

    # --- SKIP / LIMIT ---
    offset = tree.args.get("offset")
    limit = tree.args.get("limit")
    if offset:
        cypher_lines.append(f"SKIP {offset.expression.sql()}")

    def _resolve_limit_expr(lim_node: exp.Expression) -> str:
        lim_expr = getattr(lim_node, "expression", None)
        if isinstance(lim_expr, exp.Parameter):
            return "25"
        return lim_expr.sql() if lim_expr is not None else "25"

    if node_only:
        # Use Neo4j browser default when multiple nodes; original limit for single-node.
        cypher_lines.append("LIMIT 25" if len(node_aliases) > 1 else (
            f"LIMIT {override_limit}" if override_limit is not None else
            (f"LIMIT {_resolve_limit_expr(limit)}" if limit else "LIMIT 25")
        ))
    elif override_limit is not None:
        cypher_lines.append(f"LIMIT {override_limit}")
    elif limit:
        cypher_lines.append(f"LIMIT {_resolve_limit_expr(limit)}")

    return "\n".join(cypher_lines)


# --- Helpers ---

def _resolve_label(
    tbl: exp.Table,
    domain_to_label: dict[tuple[str, str], str],
) -> str | None:
    """Map a sqlglot Table node to a Cypher node label using the domain lookup."""
    db = tbl.db or ""
    name = tbl.name or ""
    return domain_to_label.get((db, name)) or domain_to_label.get(("", name))


def _rel_type_from_on(
    on_expr: exp.Expression | None,
    join_to_rel: dict[tuple[str, str], RelationshipMapping],
) -> str | None:
    """Extract Cypher relationship type from a JOIN ON condition."""
    if on_expr is None:
        return None
    for eq in on_expr.find_all(exp.EQ):
        left, right = eq.this, eq.expression
        if isinstance(left, exp.Column) and isinstance(right, exp.Column):
            lc = left.name
            rc = right.name
            rel = join_to_rel.get((lc, rc)) or join_to_rel.get((rc, lc))
            if rel:
                return rel.rel_type
    return None


def _src_alias_from_on(
    on_expr: exp.Expression | None,
    tgt_sql_alias: str,
    default_alias: str,
) -> str:
    """Return the source table alias from a JOIN ON condition.

    Looks for column references whose table qualifier is not the join target —
    that's the source side of the relationship.  Falls back to default_alias.
    """
    if on_expr is None:
        return default_alias
    for eq in on_expr.find_all(exp.EQ):
        for col in (eq.this, eq.expression):
            if isinstance(col, exp.Column) and col.table and col.table != tgt_sql_alias:
                return col.table
    return default_alias


def _flat_return_items_from_jbo(
    jbo_node: exp.JSONObject,
    agg_seen: dict[str, str],
    agg_seen_label: dict[str, str],
    prop_map_for_label,
) -> list[str]:
    """Build 'node.prop AS label__key' items for flat (non-aggregated) RETURN clause."""
    items: list[str] = []
    for kv in jbo_node.expressions:
        if not isinstance(kv, exp.JSONKeyValue):
            continue
        key_raw = kv.this.sql()
        key = key_raw.strip("'\"")
        val = kv.expression
        if isinstance(val, exp.Column):
            tbl = val.table or ""
            short = agg_seen.get(tbl, tbl)
            lbl = agg_seen_label.get(tbl)
            lbl_prefix = lbl.lower() if lbl else short
            # key is already the GQL/Cypher property name (set by _build_rel_json_kv)
            items.append(f"{short}.{key} AS {lbl_prefix}__{key}")
        elif isinstance(val, exp.Subquery) and isinstance(val.this, exp.Select):
            nested_sel = val.this
            nested_exprs = nested_sel.args.get("expressions") or []
            if not nested_exprs:
                continue
            nested_agg = nested_exprs[0]
            nested_jbo: exp.JSONObject | None = None
            if isinstance(nested_agg, exp.JSONObject):
                nested_jbo = nested_agg
            elif isinstance(nested_agg, exp.Alias) and isinstance(nested_agg.this, exp.JSONObject):
                nested_jbo = nested_agg.this
            elif isinstance(nested_agg, exp.JSONArrayAgg) and isinstance(nested_agg.this, exp.JSONObject):
                nested_jbo = nested_agg.this
            if nested_jbo is not None:
                items.extend(_flat_return_items_from_jbo(nested_jbo, agg_seen, agg_seen_label, prop_map_for_label))
    return items


def _cypher_map_from_jbo(
    jbo_node: exp.JSONObject,
    agg_seen: dict[str, str],
    agg_seen_label: dict[str, str],
    prop_map_for_label,
    flat: bool,
) -> str:
    pairs = []
    for kv in jbo_node.expressions:
        if not isinstance(kv, exp.JSONKeyValue):
            continue
        key_raw = kv.this.sql()
        key = key_raw.strip("'\"")
        val = kv.expression
        if isinstance(val, exp.Column):
            tbl = val.table or ""
            col = val.name
            short = agg_seen.get(tbl, tbl)
            lbl = agg_seen_label.get(tbl)
            pmap = prop_map_for_label(lbl) if lbl else {}
            cypher_prop = pmap.get(col, col)
            pairs.append(f"{key}: {short}.{cypher_prop}")
        elif isinstance(val, exp.Subquery) and isinstance(val.this, exp.Select):
            nested_sel = val.this
            nested_exprs = nested_sel.args.get("expressions") or []
            if not nested_exprs:
                continue
            nested_agg = nested_exprs[0]
            nested_jbo = None
            is_array = False
            if isinstance(nested_agg, exp.JSONObject):
                nested_jbo = nested_agg
            elif isinstance(nested_agg, exp.Alias) and isinstance(nested_agg.this, exp.JSONObject):
                nested_jbo = nested_agg.this
            elif isinstance(nested_agg, exp.JSONArrayAgg):
                is_array = True
                if isinstance(nested_agg.this, exp.JSONObject):
                    nested_jbo = nested_agg.this
            if nested_jbo is not None:
                nested_map = _cypher_map_from_jbo(nested_jbo, agg_seen, agg_seen_label, prop_map_for_label, flat)
                if is_array and not flat:
                    pairs.append(f"{key}: collect({nested_map})")
                else:
                    pairs.append(f"{key}: {nested_map}")
    return "{" + ", ".join(pairs) + "}"


def _build_return(
    select_exprs: list[exp.Expression],
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
                    items.append(f"{tbl}.{cypher_prop} AS {lbl_prefix}__{cypher_prop}" if tbl else cypher_prop)
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
                    raw = re.sub(rf'\b{re.escape(sql_a)}\b', am[sql_a], raw)
                items.append(f"{raw} AS {expr.alias}")
        else:
            raw = _sql_to_cypher_expr(expr.sql(dialect="postgres"))
            for sql_a in sorted(am, key=len, reverse=True):
                raw = re.sub(rf'\b{re.escape(sql_a)}\b', am[sql_a], raw)
            items.append(raw)
    return items or ["*"]


def _offset_aliases(cypher: str, offset: int) -> tuple[str, int]:
    """Rename single-letter node aliases to start at the given letter offset.

    Aliases are discovered from MATCH pattern definitions (e.g. ``(a:Label)``),
    then every word-boundary occurrence is substituted.  Returns the rewritten
    Cypher and the next available offset.
    """
    letters = list(string.ascii_lowercase)
    defined = sorted(set(re.findall(r'\(([a-z])(?:[:\s)])', cypher)))
    rename = {
        old: letters[offset + i] if (offset + i) < len(letters) else f"n{offset + i}"
        for i, old in enumerate(defined)
    }
    result = cypher
    for old in sorted(rename, key=len, reverse=True):
        result = re.sub(rf'\b{re.escape(old)}\b', rename[old], result)
    return result, offset + len(defined)


def combine_cypher_queries(cyphers: list[str]) -> str:
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
                    all_return_items.extend(
                        item.strip() for item in items_str.split(",")
                    )
                break
        indented = "\n".join(f"  {l}" for l in lines)
        wrapped.append(f"CALL {{\n{indented}\n}}")

    combined = "\n".join(wrapped)
    if all_return_items:
        combined += f"\nRETURN {', '.join(all_return_items)}"
    else:
        combined += "\nRETURN *"

    return combined


def _sql_to_cypher_expr(sql_expr: str) -> str:
    """Minimally rewrite a SQL expression fragment to Cypher syntax."""
    # Remove double-quote wrapping from identifiers (sqlglot emits them)
    result = re.sub(r'"(\w+)"', r'\1', sql_expr)
    result = result.replace(" ILIKE ", " =~ ")
    result = result.replace("TRUE", "true").replace("FALSE", "false")
    return result
