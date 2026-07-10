# Copyright (c) 2026 Kenneth Stott
# Canary: 1cf652fa-af09-49ac-a860-b3222b8a38ce
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""ARRAY_AGG / JSON correlated-subquery processing for SQL→Cypher.

Turns ARRAY_AGG and json_agg/json_object correlated subqueries in a SELECT into
OPTIONAL MATCH traversals plus collect()/map RETURN expressions. Called by
sql_to_cypher.semantic_sql_to_cypher; depends only on sql_to_cypher_helpers.
"""

from __future__ import annotations

import sqlglot.expressions as exp

from provisa.cypher.sql_to_cypher_helpers import _resolve_label


def _extract_array_agg_col_and_from(
    inner: exp.Select,
) -> tuple[exp.Column | None, exp.Table | None, exp.Expression | None]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """Return (agg_col_node, inner_tbl, where_node) from an ARRAY_AGG subquery SELECT."""
    _agg_exprs = inner.args.get("expressions") or []
    if len(_agg_exprs) != 1:
        return None, None, None
    _agg_node = _agg_exprs[0]
    if isinstance(_agg_node, exp.ArrayAgg):
        _agg_col_node = _agg_node.this
    elif isinstance(_agg_node, exp.Anonymous) and _agg_node.name.upper() == "ARRAY_AGG":
        _agg_cols = getattr(_agg_node, "expressions", [])
        _agg_col_node = _agg_cols[0] if _agg_cols else None
    else:
        return None, None, None
    if not isinstance(_agg_col_node, exp.Column):
        return None, None, None
    _inner_from = inner.args.get("from_")
    if not _inner_from:
        return None, None, None
    if isinstance(_inner_from.this, exp.Table):
        return _agg_col_node, _inner_from.this, inner.args.get("where")
    if isinstance(_inner_from.this, exp.Subquery):
        _limit_select = _inner_from.this.this
        if not isinstance(_limit_select, exp.Select):
            return None, None, None
        _lf = _limit_select.args.get("from_")
        if not (_lf and isinstance(_lf.this, exp.Table)):
            return None, None, None
        return _agg_col_node, _lf.this, _limit_select.args.get("where")
    return None, None, None


def _resolve_where_src_alias(
    where_node: exp.Expression | None,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    inner_sql_alias: str,
    default: str,
) -> str:
    """Find the source alias in a WHERE condition (the side that is NOT inner_sql_alias)."""
    if not where_node:
        return default
    for _eq in where_node.find_all(exp.EQ):
        for _wc in (_eq.this, _eq.expression):
            if isinstance(_wc, exp.Column) and _wc.table and _wc.table != inner_sql_alias:
                return _wc.table
    return default


def _next_short_alias(counter: int, letters: list[str]) -> str:
    return letters[counter] if counter < len(letters) else f"n{counter}"


def _process_array_agg_subqueries(
    select_exprs: list[exp.Expression],  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    domain_to_label: dict[tuple[str, str], str],
    label_to_rel: dict[str, str | None],
    src_tgt_to_rel: dict[tuple[str, str], str],
    alias_map: dict[str, str],
    alias_label: dict[str, str],
    base_alias: str,
    base_label: str,
    sql_base_alias: str,
    flat: bool,
    agg_alias_counter: int,
    agg_seen: dict[str, str],
    _agg_seen_label: dict[str, str],
    array_agg_return: dict[str, str | list[str]],
    cypher_lines: list[str],
    letters: list[str],
    prop_map_for_label,
    node_fn,
) -> int:
    """Process ARRAY_AGG correlated subqueries in SELECT; mutates cypher_lines and array_agg_return."""
    for _expr in select_exprs:
        if not (isinstance(_expr, exp.Alias) and isinstance(_expr.this, exp.Subquery)):
            continue
        _inner = _expr.this.this
        if not isinstance(_inner, exp.Select):
            continue
        _agg_col_node, _inner_tbl, _where_node = _extract_array_agg_col_and_from(_inner)
        if _agg_col_node is None or _inner_tbl is None:
            continue
        _tgt_lbl = _resolve_label(_inner_tbl, domain_to_label)
        if _tgt_lbl is None:
            continue
        _inner_sql_alias = _inner_tbl.alias or _inner_tbl.name
        _src_sql = _resolve_where_src_alias(_where_node, _inner_sql_alias, sql_base_alias)
        _col_table = _agg_col_node.table

        if _col_table and _col_table != _inner_sql_alias:
            agg_alias_counter = _process_array_agg_chained(
                _inner,
                _agg_col_node,
                _inner_tbl,
                _tgt_lbl,
                _inner_sql_alias,
                _col_table,
                _src_sql,
                domain_to_label,
                label_to_rel,
                src_tgt_to_rel,
                alias_map,
                alias_label,
                base_alias,
                base_label,
                flat,
                agg_alias_counter,
                agg_seen,
                cypher_lines,
                letters,
                prop_map_for_label,
                node_fn,
                array_agg_return,
                _expr,
            )
            continue

        _agg_sql_col = _agg_col_node.name
        _cypher_prop = prop_map_for_label(_tgt_lbl).get(_agg_sql_col, _agg_sql_col)

        if _inner_sql_alias not in agg_seen:
            _arr_short = _next_short_alias(agg_alias_counter, letters)
            agg_alias_counter += 1
            agg_seen[_inner_sql_alias] = _arr_short
            _src_short = alias_map.get(_src_sql, base_alias)
            _src_lbl = alias_label.get(_src_sql, base_label)
            _agg_rel_type = src_tgt_to_rel.get((_src_lbl, _tgt_lbl)) or label_to_rel.get(_tgt_lbl)
            _agg_rel_str = f"[:{_agg_rel_type}]" if _agg_rel_type else "[]"
            cypher_lines.append(
                f"OPTIONAL MATCH {node_fn(_src_short, _src_lbl)}-{_agg_rel_str}->{node_fn(_arr_short, _tgt_lbl)}"
            )

        _prop_ref = f"{agg_seen[_inner_sql_alias]}.{_cypher_prop}"
        array_agg_return[_expr.alias] = _prop_ref if flat else f"collect({_prop_ref})"

    return agg_alias_counter


def _process_array_agg_chained(
    inner: exp.Select,
    agg_col_node: exp.Column,
    _inner_tbl: exp.Table,
    tgt_lbl: str,
    inner_sql_alias: str,
    col_table: str,
    src_sql: str,
    domain_to_label: dict[tuple[str, str], str],
    label_to_rel: dict[str, str | None],
    src_tgt_to_rel: dict[tuple[str, str], str],
    alias_map: dict[str, str],
    alias_label: dict[str, str],
    base_alias: str,
    base_label: str,
    flat: bool,
    agg_alias_counter: int,
    agg_seen: dict[str, str],
    cypher_lines: list[str],
    letters: list[str],
    prop_map_for_label,
    node_fn,
    array_agg_return: dict[str, str | list[str]],
    expr: exp.Alias,
) -> int:
    """Handle ARRAY_AGG where the aggregated column comes from a JOIN inside the subquery."""
    _inner_joins = inner.args.get("joins") or []
    _join_tbl_node = next(
        (
            j.this
            for j in _inner_joins
            if isinstance(j.this, exp.Table) and (j.this.alias or j.this.name) == col_table
        ),
        None,
    )
    if _join_tbl_node is None:
        return agg_alias_counter
    _eff_lbl = _resolve_label(_join_tbl_node, domain_to_label)
    if _eff_lbl is None:
        return agg_alias_counter

    if inner_sql_alias not in agg_seen:
        _arr_short = _next_short_alias(agg_alias_counter, letters)
        agg_alias_counter += 1
        agg_seen[inner_sql_alias] = _arr_short
        _src_short = alias_map.get(src_sql, base_alias)
        _src_lbl = alias_label.get(src_sql, base_label)
        _parent_rel_type = src_tgt_to_rel.get((_src_lbl, tgt_lbl)) or label_to_rel.get(tgt_lbl)
        _parent_rel_str = f"[:{_parent_rel_type}]" if _parent_rel_type else "[]"
        cypher_lines.append(
            f"OPTIONAL MATCH {node_fn(_src_short, _src_lbl)}-{_parent_rel_str}->{node_fn(_arr_short, tgt_lbl)}"
        )

    if col_table not in agg_seen:
        _join_short = _next_short_alias(agg_alias_counter, letters)
        agg_alias_counter += 1
        agg_seen[col_table] = _join_short
        _from_node_short = agg_seen[inner_sql_alias]
        _join_rel_type = src_tgt_to_rel.get((tgt_lbl, _eff_lbl)) or label_to_rel.get(_eff_lbl)
        _join_rel_str = f"[:{_join_rel_type}]" if _join_rel_type else "[]"
        cypher_lines.append(
            f"OPTIONAL MATCH {node_fn(_from_node_short, tgt_lbl)}-{_join_rel_str}->{node_fn(_join_short, _eff_lbl)}"
        )

    _agg_sql_col = agg_col_node.name
    _cypher_prop = prop_map_for_label(_eff_lbl).get(_agg_sql_col, _agg_sql_col)
    _prop_ref = f"{agg_seen[col_table]}.{_cypher_prop}"
    array_agg_return[expr.alias] = _prop_ref if flat else f"collect({_prop_ref})"
    return agg_alias_counter


def _resolve_jbo_sel(outer_sel: exp.Select) -> exp.Select | None:
    """Resolve the actual SELECT containing json_object from a json_agg or json_object wrapper."""
    _outer_exprs = outer_sel.args.get("expressions") or []
    if len(_outer_exprs) != 1:
        return None
    _outer_agg = _outer_exprs[0]
    if isinstance(_outer_agg, exp.JSONArrayAgg):
        _inner_arg = _outer_agg.this
        if isinstance(_inner_arg, exp.JSONObject):
            return outer_sel
        if isinstance(_inner_arg, exp.Column) and _inner_arg.name == "_t":
            _from_outer = outer_sel.args.get("from_")
            if _from_outer and isinstance(_from_outer.this, exp.Subquery):
                _inner_limited = _from_outer.this.this
                if isinstance(_inner_limited, exp.Select):
                    return _inner_limited
    elif isinstance(_outer_agg, exp.JSONObject):
        return outer_sel
    return None


def _enqueue_jbo_nested_subqueries(
    sel: exp.Select,
    inner_sql_alias: str,
    queue: list[tuple[str, exp.Select]],
) -> None:
    """Find nested json_agg/json_object subqueries in a json_object and enqueue them."""
    _sel_exprs = sel.args.get("expressions") or []
    for _se in _sel_exprs:
        if isinstance(_se, exp.JSONObject):
            _jbo_node: exp.JSONObject | None = _se
        elif isinstance(_se, exp.Alias) and isinstance(_se.this, exp.JSONObject):
            _jbo_node = _se.this
        elif isinstance(_se, exp.JSONArrayAgg) and isinstance(_se.this, exp.JSONObject):
            _jbo_node = _se.this
        else:
            _jbo_node = None
        if _jbo_node is None:
            continue
        for _kv in _jbo_node.expressions:
            if not isinstance(_kv, exp.JSONKeyValue):
                continue
            _val = _kv.expression
            _sub_sel: exp.Select | None = None
            if isinstance(_val, exp.Subquery) and isinstance(_val.this, exp.Select):
                _sub_sel = _val.this
            if _sub_sel is None:
                continue
            _sub_outer_exprs = _sub_sel.args.get("expressions") or []
            if not _sub_outer_exprs:
                continue
            _sub_agg = _sub_outer_exprs[0]
            if isinstance(_sub_agg, exp.JSONArrayAgg):
                _sub_inner = _sub_agg.this
                if isinstance(_sub_inner, exp.JSONObject):
                    queue.append((inner_sql_alias, _sub_sel))
                elif isinstance(_sub_inner, exp.Column) and _sub_inner.name == "_t":
                    _sub_from_nd = _sub_sel.args.get("from_")
                    if _sub_from_nd and isinstance(_sub_from_nd.this, exp.Subquery):
                        _sub_limited = _sub_from_nd.this.this
                        if isinstance(_sub_limited, exp.Select):
                            queue.append((inner_sql_alias, _sub_limited))
            elif isinstance(_sub_agg, exp.JSONObject):
                queue.append((inner_sql_alias, _sub_sel))


def _extract_jbo_node_for_ret(jbo_sel: exp.Select) -> exp.JSONObject | None:
    """Extract the top-level JSONObject from a jbo_sel for building the RETURN map literal."""
    for _se in jbo_sel.args.get("expressions") or []:
        if isinstance(_se, exp.JSONObject):
            return _se
        if isinstance(_se, exp.Alias) and isinstance(_se.this, exp.JSONObject):
            return _se.this
        if isinstance(_se, exp.JSONArrayAgg) and isinstance(_se.this, exp.JSONObject):
            return _se.this
    return None


def _process_json_subqueries(
    select_exprs: list[exp.Expression],  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    domain_to_label: dict[tuple[str, str], str],
    label_to_rel: dict[str, str | None],
    src_tgt_to_rel: dict[tuple[str, str], str],
    alias_map: dict[str, str],
    alias_label: dict[str, str],
    base_alias: str,
    base_label: str,
    sql_base_alias: str,
    flat: bool,
    agg_alias_counter: int,
    agg_seen: dict[str, str],
    agg_seen_label: dict[str, str],
    array_agg_return: dict[str, str | list[str]],
    cypher_lines: list[str],
    letters: list[str],
    prop_map_for_label,
    node_fn,
) -> None:
    """Process json_agg/json_object correlated subqueries; mutates cypher_lines and array_agg_return."""
    for _expr in select_exprs:
        if not (isinstance(_expr, exp.Alias) and isinstance(_expr.this, exp.Subquery)):
            continue
        _outer_sel = _expr.this.this
        if not isinstance(_outer_sel, exp.Select):
            continue
        _top_alias = _expr.alias
        _jbo_sel = _resolve_jbo_sel(_outer_sel)
        if _jbo_sel is None:
            continue
        _from_node = _jbo_sel.args.get("from_")
        if not (_from_node and isinstance(_from_node.this, exp.Table)):
            continue

        _queue: list[tuple[str, exp.Select]] = [(sql_base_alias, _jbo_sel)]

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
            _src_sql = _resolve_where_src_alias(_where_nd, _inner_sql_alias, _parent_sql_alias)

            if _inner_sql_alias not in agg_seen:
                _arr_short = _next_short_alias(agg_alias_counter, letters)
                agg_alias_counter += 1
                agg_seen[_inner_sql_alias] = _arr_short
                agg_seen_label[_inner_sql_alias] = _tgt_lbl
                _src_short = alias_map.get(_src_sql) or agg_seen.get(_src_sql, base_alias)
                _src_lbl = alias_label.get(_src_sql) or agg_seen_label.get(_src_sql, base_label)
                _rel_type = src_tgt_to_rel.get((_src_lbl, _tgt_lbl)) or label_to_rel.get(_tgt_lbl)
                _rel_str = f"[:{_rel_type}]" if _rel_type else "[]"
                cypher_lines.append(
                    f"OPTIONAL MATCH {node_fn(_src_short, _src_lbl)}-{_rel_str}->{node_fn(_arr_short, _tgt_lbl)}"
                )

            _enqueue_jbo_nested_subqueries(_sel, _inner_sql_alias, _queue)

        if _top_alias not in array_agg_return:
            _jbo_node_for_ret = _extract_jbo_node_for_ret(_jbo_sel)
            _jbo_from = _jbo_sel.args.get("from_")
            _jbo_tbl = (
                _jbo_from.this if _jbo_from and isinstance(_jbo_from.this, exp.Table) else None
            )
            _first_sql_alias = (_jbo_tbl.alias or _jbo_tbl.name) if _jbo_tbl else None

            if not flat and _jbo_node_for_ret is not None:
                _map_expr = _cypher_map_from_jbo(
                    _jbo_node_for_ret, agg_seen, agg_seen_label, prop_map_for_label, flat
                )
                array_agg_return[_top_alias] = f"collect({_map_expr})"
            elif flat and _jbo_node_for_ret is not None:
                _flat_items = _flat_return_items_from_jbo(
                    _jbo_node_for_ret, agg_seen, agg_seen_label, prop_map_for_label
                )
                array_agg_return[_top_alias] = _flat_items or agg_seen.get(
                    _first_sql_alias or "", base_alias
                )
            else:
                array_agg_return[_top_alias] = agg_seen.get(_first_sql_alias or "", base_alias)


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
            elif isinstance(nested_agg, exp.JSONArrayAgg) and isinstance(
                nested_agg.this, exp.JSONObject
            ):
                nested_jbo = nested_agg.this
            if nested_jbo is not None:
                items.extend(
                    _flat_return_items_from_jbo(
                        nested_jbo, agg_seen, agg_seen_label, prop_map_for_label
                    )
                )
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
                nested_map = _cypher_map_from_jbo(
                    nested_jbo, agg_seen, agg_seen_label, prop_map_for_label, flat
                )
                if is_array and not flat:
                    pairs.append(f"{key}: collect({nested_map})")
                else:
                    pairs.append(f"{key}: {nested_map}")
    return "{" + ", ".join(pairs) + "}"
