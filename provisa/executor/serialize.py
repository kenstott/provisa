# Copyright (c) 2026 Kenneth Stott
# Canary: c88a327d-c038-4dec-a4c5-f292b2675f6f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Reconstruct nested GraphQL JSON from flat JOIN result rows (REQ-047).

many-to-one  -> single nested object
one-to-many  -> array of nested objects
Null propagation for nullable relationships.
"""

# Requirements: REQ-047, REQ-048, REQ-049, REQ-050, REQ-196, REQ-218

from __future__ import annotations

import json
from decimal import Decimal

from provisa.compiler.cursor import encode_cursor
from provisa.compiler.sql_gen import ColumnRef, CompiledQuery


def _recursive_json_convert(
    val: object,  # object-ok: arbitrary SQL column value — Decimal | date | str | int | dict | list | None
) -> object:
    """Recursively apply _convert_value to nested dicts/lists.

    Needed because json_format(...) embeds JSON arrays as VARCHAR strings inside
    outer JSON_OBJECT results — the top-level parse yields a dict whose values
    may themselves be JSON strings that need unpacking.
    """
    if isinstance(val, dict):
        return {k: _convert_value(v) for k, v in val.items()}
    if isinstance(val, list):
        return [_convert_value(v) for v in val]
    return val


def _convert_value(
    val: object,  # object-ok: arbitrary SQL column value — Decimal | date | str | int | dict | list | None
) -> object:
    """Convert database types to JSON-safe Python types."""
    if isinstance(val, Decimal):
        f = float(val)
        if f == int(f) and "." not in str(val):
            return int(f)
        return f
    if hasattr(val, "isoformat"):
        from datetime import date as _date
        from typing import cast as _cast

        return _cast(_date, val).isoformat()  # guarded by hasattr — any date/datetime-like type
    # Trino returns JSON columns as strings; parse so object sub-fields resolve correctly.
    # Recursively convert nested values so json_format(...)-wrapped arrays unpack correctly.
    if isinstance(val, str) and len(val) > 1 and val[0] in ("{", "["):
        try:
            parsed = json.loads(val)
            return _recursive_json_convert(parsed)
        except (json.JSONDecodeError, ValueError):
            pass
    return val


def _to_hashable(
    v: object,  # object-ok: arbitrary SQL column value — dict | list | scalar
) -> object:
    """Make a value safe to use as a dict key / tuple component."""
    if isinstance(v, (dict, list)):
        return json.dumps(v, sort_keys=True, default=str)
    return v


def _group_columns(
    columns: list[ColumnRef],
) -> tuple[list[tuple[int, ColumnRef]], dict[str, list[tuple[int, ColumnRef]]]]:
    """Split columns into root-level and nested groups keyed by nest path."""
    root_cols: list[tuple[int, ColumnRef]] = []
    nested_groups: dict[str, list[tuple[int, ColumnRef]]] = {}
    for i, col in enumerate(columns):
        if col.nested_in is None:
            root_cols.append((i, col))
        else:
            nested_groups.setdefault(col.nested_in, []).append((i, col))
    return root_cols, nested_groups


def _detect_path_sets(
    nested_groups: dict[str, list[tuple[int, ColumnRef]]],
) -> tuple[set[str], set[str], set[str]]:
    """Return (one_to_many_paths, agg_paths, absorbed_paths)."""
    one_to_many_paths: set[str] = set()
    for nest_path, nest_cols in nested_groups.items():
        for _, col in nest_cols:
            if col.cardinality == "one-to-many" or col.is_agg:
                one_to_many_paths.add(nest_path)
                break

    agg_paths: set[str] = {
        p for p in one_to_many_paths if any(col.is_agg for _, col in nested_groups[p])
    }
    absorbed_paths: set[str] = {
        cp for cp in one_to_many_paths if any(cp.startswith(p + ".") for p in agg_paths)
    }
    return one_to_many_paths, agg_paths, absorbed_paths


def _navigate_path(
    obj: dict,
    parts: list[str],
) -> tuple[dict, bool]:
    """Walk obj down parts[:-1], creating intermediate dicts. Returns (target, skip)."""
    target = obj
    for part in parts[:-1]:
        cur = target.get(part)
        if isinstance(cur, dict):
            target = cur
        elif part not in target:
            target[part] = {}
            target = target[part]
        else:
            return target, True
    return target, False


def _navigate_path_into_lists(
    obj: dict,
    parts: list[str],
) -> tuple[dict, bool]:
    """Walk obj down parts[:-1], entering the last list element when encountered."""
    target = obj
    for part in parts[:-1]:
        cur = target.get(part)
        if isinstance(cur, dict):
            target = cur
        elif isinstance(cur, list) and cur:
            target = cur[-1]
        else:
            return target, True
    return target, False


def _seed_nested_path(
    obj: dict,
    nest_path: str,
    nest_cols_for_path: list[tuple[int, ColumnRef]],
    one_to_many_paths: set[str],
    row: tuple,
) -> None:
    """Seed a single nested path into obj on first visit."""
    parts = nest_path.split(".")
    target, skip = _navigate_path(obj, parts)
    if skip:
        return
    leaf = parts[-1]
    if nest_path in one_to_many_paths:
        target[leaf] = []
    else:
        all_none = all(row[idx] is None for idx, _ in nest_cols_for_path)
        if all_none:
            target[leaf] = None
        else:
            target[leaf] = {
                col.field_name: _convert_value(row[idx]) for idx, col in nest_cols_for_path
            }


def _seed_all_nested_paths(
    obj: dict,
    nested_groups: dict[str, list[tuple[int, ColumnRef]]],
    one_to_many_paths: set[str],
    row: tuple,
) -> None:
    """Seed all nested paths in sorted order (parents before children)."""
    for nest_path in sorted(nested_groups.keys()):
        _seed_nested_path(
            obj,
            nest_path,
            nested_groups[nest_path],
            one_to_many_paths,
            row,
        )


def _zip_absorbed_children(
    arr: list,
    oto_path: str,
    child: dict,
    absorbed_paths: set[str],
    nested_groups: dict[str, list[tuple[int, ColumnRef]]],
    row: tuple,
    n: int,
) -> None:
    """Unzip ARRAY_AGG child and absorbed sub-paths, appending elements to arr."""
    prefix = oto_path + "."
    child_absorbed = [
        (
            cp,
            {col.field_name: _convert_value(row[idx]) for idx, col in nested_groups[cp]},
        )
        for cp in sorted(absorbed_paths)
        if cp.startswith(prefix)
    ]
    for j in range(n):
        elem = {k: (v[j] if isinstance(v, list) and j < len(v) else v) for k, v in child.items()}
        for cp, cp_data in child_absorbed:
            rel = cp[len(prefix) :]
            rel_parts = rel.split(".")
            sub_t: dict = elem
            for sub_part in rel_parts[:-1]:
                sub_t = sub_t.setdefault(sub_part, {})
            leaf = rel_parts[-1]
            cp_elem = {
                k: (v[j] if isinstance(v, list) and j < len(v) else v) for k, v in cp_data.items()
            }
            if all(v is None for v in cp_elem.values()):
                sub_t[leaf] = None
            else:
                sub_t.setdefault(leaf, []).append(cp_elem)
        arr.append(elem)


def _append_regular_child(
    arr: list,
    new_item: dict,
    oto_path: str,
    one_to_many_paths: set[str],
    absorbed_paths: set[str],
) -> None:
    """Seed deeper sub-paths on new_item then append to arr, skipping duplicates."""
    if new_item in arr:
        return
    prefix = oto_path + "."
    for deeper_path in sorted(
        p for p in one_to_many_paths if p.startswith(prefix) and p not in absorbed_paths
    ):
        rel_parts = deeper_path[len(prefix) :].split(".")
        sub_target = new_item
        for sub_part in rel_parts[:-1]:
            sub_target = sub_target.setdefault(sub_part, {})
        sub_target.setdefault(rel_parts[-1], [])
    arr.append(new_item)


def _accumulate_oto_path(
    oto_path: str,
    row: tuple,
    parent_obj: dict,
    is_first_visit: bool,
    one_to_many_paths: set[str],
    absorbed_paths: set[str],
    nested_groups: dict[str, list[tuple[int, ColumnRef]]],
) -> None:
    """Accumulate one row into the one-to-many array at oto_path in parent_obj."""
    nest_cols_for_path = nested_groups[oto_path]
    all_none = all(row[idx] is None for idx, _ in nest_cols_for_path)
    child: dict = {col.field_name: _convert_value(row[idx]) for idx, col in nest_cols_for_path}
    parts = oto_path.split(".")
    target, skip = _navigate_path_into_lists(parent_obj, parts)
    if skip:
        return
    arr = target.get(parts[-1])
    if not isinstance(arr, list):
        return
    if all_none:
        return
    has_oto_parent = any(oto_path.startswith(p + ".") for p in one_to_many_paths if p != oto_path)
    if any(isinstance(v, list) for v in child.values()):
        if not is_first_visit and not has_oto_parent:
            return
        n = next((len(v) for v in child.values() if isinstance(v, list)), 0)
        _zip_absorbed_children(arr, oto_path, child, absorbed_paths, nested_groups, row, n)
    else:
        _append_regular_child(arr, dict(child), oto_path, one_to_many_paths, absorbed_paths)


def _serialize_with_one_to_many(
    rows: list[tuple],
    root_cols: list[tuple[int, ColumnRef]],
    nested_groups: dict[str, list[tuple[int, ColumnRef]]],
    one_to_many_paths: set[str],
    absorbed_paths: set[str],
    result_limit: int | None,
    root_field: str,
) -> dict:
    """Serialize rows when at least one one-to-many path exists."""
    seen: dict[tuple, int] = {}
    result_rows: list[dict] = []

    for row in rows:
        root_key = tuple(_to_hashable(_convert_value(row[idx])) for idx, _ in root_cols)
        is_first_visit = root_key not in seen

        if is_first_visit:
            obj: dict = {}
            for idx, col in root_cols:
                obj[col.field_name] = _convert_value(row[idx])
            _seed_all_nested_paths(obj, nested_groups, one_to_many_paths, row)
            seen[root_key] = len(result_rows)
            result_rows.append(obj)

        parent_idx = seen[root_key]
        parent_obj = result_rows[parent_idx]
        for oto_path in sorted(one_to_many_paths):
            if oto_path in absorbed_paths:
                continue
            _accumulate_oto_path(
                oto_path,
                row,
                parent_obj,
                is_first_visit,
                one_to_many_paths,
                absorbed_paths,
                nested_groups,
            )

    if result_limit is not None:
        result_rows = result_rows[:result_limit]
    return {"data": {root_field: result_rows}}


def _build_nested_obj(
    obj: dict,
    nested_groups: dict[str, list[tuple[int, ColumnRef]]],
    row: tuple,
) -> None:
    """Populate nested relationship fields into obj for flat/many-to-one rows."""
    for nest_path, nest_cols in nested_groups.items():
        all_none = all(row[idx] is None for idx, _ in nest_cols)
        parts = nest_path.split(".")
        target: dict | None = obj
        skip = False
        for part in parts[:-1]:
            assert target is not None
            if part not in target:
                target[part] = {}
            target = target[part]
            if target is None:
                skip = True
                break
        if skip:
            continue
        assert target is not None
        leaf = parts[-1]
        if all_none:
            target[leaf] = None
        else:
            nested_obj: dict = {}
            for idx, col in nest_cols:
                nested_obj[col.field_name] = _convert_value(row[idx])
            target[leaf] = nested_obj


def _detect_truncated_paths(
    result_rows: list[dict],
    seen_root_keys: dict[tuple, int],
    nested_groups: dict[str, list[tuple[int, ColumnRef]]],
    row: tuple,
    root_key: tuple,
) -> set[str]:
    """Return set of nested paths that differ from the already-seen row."""
    truncated: set[str] = set()
    prev_obj = result_rows[seen_root_keys[root_key]]
    for nest_path, nest_cols in nested_groups.items():
        cur_vals = tuple(_convert_value(row[idx]) for idx, _ in nest_cols)
        parts = nest_path.split(".")
        target = prev_obj
        for part in parts[:-1]:
            target = target.get(part) or {}
        prev_val = target.get(parts[-1])
        prev_vals = tuple(prev_val.values()) if isinstance(prev_val, dict) else (prev_val,)
        if cur_vals != prev_vals:
            truncated.add(nest_path)
    return truncated


def _serialize_flat(
    rows: list[tuple],
    root_cols: list[tuple[int, ColumnRef]],
    nested_groups: dict[str, list[tuple[int, ColumnRef]]],
    result_limit: int | None,
    root_field: str,
) -> dict:
    """Serialize rows for flat or many-to-one (no one-to-many) case."""
    seen_root_keys: dict[tuple, int] = {}
    result_rows: list[dict] = []
    truncated_paths: set[str] = set()

    for row in rows:
        if root_cols:
            root_key = tuple(
                _to_hashable(_convert_value(row[idx])) for idx, col in root_cols if not col.is_agg
            )
        else:
            # group_by: all columns are nested (groupKey/aggregates) — every row is distinct
            root_key = tuple(_to_hashable(_convert_value(v)) for v in row)
        if root_key in seen_root_keys:
            truncated_paths |= _detect_truncated_paths(
                result_rows, seen_root_keys, nested_groups, row, root_key
            )
            continue

        obj: dict = {}
        for idx, col in root_cols:
            obj[col.field_name] = _convert_value(row[idx])
        _build_nested_obj(obj, nested_groups, row)
        seen_root_keys[root_key] = len(result_rows)
        result_rows.append(obj)

    if result_limit is not None:
        result_rows = result_rows[:result_limit]

    result: dict = {"data": {root_field: result_rows}}
    if truncated_paths:
        result["extensions"] = {
            "warnings": [
                {
                    "message": (
                        f"many-to-one relationship '{path}' returned multiple rows "
                        f"for the same parent; only the first value was used. "
                        f"Check cardinality — this relationship may need to be one-to-many."
                    ),
                    "path": path,
                }
                for path in sorted(truncated_paths)
            ]
        }
    return result


def serialize_rows(  # REQ-047, REQ-048, REQ-049, REQ-050
    rows: list[tuple],
    columns: list[ColumnRef],
    root_field: str,
    result_limit: int | None = None,
) -> dict:
    """Serialize flat SQL rows into nested GraphQL JSON.

    Args:
        rows: Result rows from SQL execution (tuples).
        columns: Column references from compilation (maps positions to fields).
        root_field: The GraphQL root field name (e.g., "orders").

    Returns:
        {"data": {root_field: [...]}}
    """
    root_cols, nested_groups = _group_columns(columns)
    one_to_many_paths, _agg_paths, absorbed_paths = _detect_path_sets(nested_groups)  # pyright: ignore[reportUnusedVariable]

    if one_to_many_paths and root_cols:
        return _serialize_with_one_to_many(
            rows,
            root_cols,
            nested_groups,
            one_to_many_paths,
            absorbed_paths,
            result_limit,
            root_field,
        )

    return _serialize_flat(rows, root_cols, nested_groups, result_limit, root_field)


def _transform_row(obj: dict, m2o_paths: set[str], prefix: str) -> None:
    for key in list(obj.keys()):
        value = obj[key]
        path = f"{prefix}.{key}" if prefix else key
        if path in m2o_paths:
            if isinstance(value, list):
                obj[key] = value[0] if value else None
                if obj[key] is not None:
                    _transform_row(obj[key], m2o_paths, path)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    _transform_row(item, m2o_paths, path)
        elif isinstance(value, dict):
            _transform_row(value, m2o_paths, path)


def shape_transform(result: dict, columns: list[ColumnRef]) -> dict:  # REQ-484
    """Collapse many-to-one ARRAY_AGG arrays into single objects."""
    m2o_paths = {
        col.nested_in
        for col in columns
        if col.is_agg and col.cardinality != "one-to-many" and col.nested_in
    }
    if not m2o_paths:
        return result
    root_field = next(iter(result["data"]))
    rows = result["data"][root_field]
    if isinstance(rows, list):
        for row in rows:
            _transform_row(row, m2o_paths, "")
    return result


def serialize_aggregate(  # REQ-196, REQ-197
    agg_rows: list[tuple],
    agg_columns: list[ColumnRef],
    nodes_rows: list[tuple] | None,
    nodes_columns: list[ColumnRef] | None,
    root_field: str,
    agg_alias: str = "aggregate",
) -> dict:
    """Serialize aggregate query result (with optional nodes) into GraphQL JSON.

    Column nested_in paths use agg_alias as prefix (default "aggregate"), e.g.:
      "aggregate"      → aggregate.count
      "aggregate.sum"  → aggregate.sum.amount

    Returns:
        {"data": {root_field: {agg_alias: {...}, "nodes": [...]}}}
        "nodes" key is present only when nodes_rows is not None.
    """
    # Build aggregate inner object from first (only) row.
    # Strip the leading agg_alias prefix from each nested_in path, then
    # reconstruct the sub-structure (sum, avg, min, max).
    agg_inner: dict = {}
    if agg_rows:
        row = agg_rows[0]
        for i, col in enumerate(agg_columns):
            path = col.nested_in or agg_alias
            # Strip agg_alias prefix
            if path == agg_alias:
                sub_path: list[str] = []
            elif path.startswith(f"{agg_alias}."):
                sub_path = path[len(f"{agg_alias}.") :].split(".")
            else:
                sub_path = path.split(".")

            target = agg_inner
            for part in sub_path:
                if part not in target or not isinstance(target[part], dict):
                    target[part] = {}
                target = target[part]
            target[col.field_name] = _convert_value(row[i])

    payload: dict = {agg_alias: agg_inner}

    if nodes_rows is not None and nodes_columns is not None:
        node_list: list[dict] = []
        for row in nodes_rows:
            node: dict = {}
            for i, col in enumerate(nodes_columns):
                node[col.field_name] = _convert_value(row[i])
            node_list.append(node)
        payload["nodes"] = node_list

    return {"data": {root_field: payload}}


def serialize_connection(  # REQ-218
    rows: list[tuple],
    compiled: CompiledQuery,
) -> dict:
    """Serialize flat SQL rows into Relay-style connection format."""
    columns = compiled.columns
    sort_columns = compiled.sort_columns
    page_size = compiled.page_size
    is_backward = compiled.is_backward

    has_more = False
    if page_size is not None and len(rows) > page_size:
        has_more = True
        rows = rows[:page_size]

    if is_backward:
        rows = list(reversed(rows))

    col_index = {col.field_name: i for i, col in enumerate(columns)}

    edges = []
    for row in rows:
        node: dict = {}
        for i, col in enumerate(columns):
            node[col.field_name] = _convert_value(row[i])

        cursor_vals = []
        for sc in sort_columns:
            idx = col_index.get(sc)
            if idx is not None:
                cursor_vals.append(_convert_value(row[idx]))
        cursor = encode_cursor(cursor_vals)
        edges.append({"cursor": cursor, "node": node})

    if not is_backward:
        has_next = has_more
        has_prev = compiled.has_cursor
    else:
        has_next = compiled.has_cursor
        has_prev = has_more

    page_info = {
        "hasNextPage": has_next,
        "hasPreviousPage": has_prev,
        "startCursor": edges[0]["cursor"] if edges else None,
        "endCursor": edges[-1]["cursor"] if edges else None,
    }

    return {"data": {compiled.root_field: {"edges": edges, "pageInfo": page_info}}}
