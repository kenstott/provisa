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
from datetime import timedelta

from dateutil.relativedelta import relativedelta
from decimal import Decimal

from provisa.compiler.cursor import encode_cursor
from provisa.core.ir_types import bytea_hex, iso8601_duration
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
    if isinstance(val, (timedelta, relativedelta)):
        return iso8601_duration(val)  # an IR interval's canonical text form
    if isinstance(val, (bytes, bytearray, memoryview)):
        return bytea_hex(val)  # an IR bytea's canonical text form
    if hasattr(val, "isoformat"):
        from datetime import date as _date
        from typing import cast as _cast

        return _cast(_date, val).isoformat()  # guarded by hasattr — any date/datetime-like type
    # the engine returns JSON columns as strings; parse so object sub-fields resolve correctly.
    # Recursively convert nested values so json_format(...)-wrapped arrays unpack correctly.
    if isinstance(val, str) and len(val) > 1 and val[0] in ("{", "["):
        try:
            parsed = json.loads(val)
            return _recursive_json_convert(parsed)
        except (json.JSONDecodeError, ValueError):
            pass
    # A native JSON column (REQ-980) returns an already-parsed dict/list — recurse so nested values
    # (Decimals, dates, json_format-wrapped strings) convert consistently with the string path above.
    if isinstance(val, (dict, list)):
        return _recursive_json_convert(val)
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


def _one_to_many_paths(nested_groups: dict[str, list[tuple[int, ColumnRef]]]) -> set[str]:
    """The nested paths that are lists: a to-many relationship's rows."""
    return {
        nest_path
        for nest_path, nest_cols in nested_groups.items()
        if any(col.cardinality == "one-to-many" for _, col in nest_cols)
    }


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


def _parent_path(path: str, placed: dict[str, dict | None]) -> str:
    """The nearest enclosing path of ``path`` placed in this row ("" is the root object)."""
    parts = path.split(".")
    for i in range(len(parts) - 1, 0, -1):
        prefix = ".".join(parts[:i])
        if prefix in placed:
            return prefix
    return ""


def _serialize_with_one_to_many(
    rows: list[tuple],
    root_cols: list[tuple[int, ColumnRef]],
    nested_groups: dict[str, list[tuple[int, ColumnRef]]],
    one_to_many_paths: set[str],
    result_limit: int | None,
    root_field: str,
) -> dict:
    """Serialize rows when at least one one-to-many path exists.

    Each row is placed path by path, parents first: a to-many element is the one its own values
    name under the element the row placed its parent in, at any depth (#285), and a to-one
    object is its parent element's. A field built as one JSON value is a value of its element."""
    seen: dict[tuple, int] = {}
    result_rows: list[dict] = []
    #: (the list it is in, its values) -> the element
    elements: dict[tuple[int, tuple], dict] = {}
    paths = sorted(nested_groups)

    for row in rows:
        root_key = tuple(_to_hashable(_convert_value(row[idx])) for idx, _ in root_cols)
        new_root = root_key not in seen
        if new_root:
            obj: dict = {col.field_name: _convert_value(row[idx]) for idx, col in root_cols}
            seen[root_key] = len(result_rows)
            result_rows.append(obj)
        placed: dict[str, dict | None] = {"": result_rows[seen[root_key]]}
        fresh: dict[str, bool] = {"": new_root}
        for path in paths:
            parent = _parent_path(path, placed)
            holder = placed[parent]
            if holder is None:
                placed[path], fresh[path] = None, False
                continue
            parts = (path[len(parent) + 1 :] if parent else path).split(".")
            for part in parts[:-1]:
                holder = holder.setdefault(part, {})
            leaf = parts[-1]
            cols = nested_groups[path]
            values = {col.field_name: _convert_value(row[idx]) for idx, col in cols}
            empty = all(row[idx] is None for idx, _ in cols)
            if path in one_to_many_paths:
                arr = holder.get(leaf)
                if not isinstance(arr, list):
                    arr = holder[leaf] = []
                if empty:
                    placed[path], fresh[path] = None, False
                    continue
                key = (id(arr), tuple(_to_hashable(v) for v in values.values()))
                element = elements.get(key)
                fresh[path] = element is None
                if element is None:
                    element = elements[key] = dict(values)
                    arr.append(element)
                placed[path] = element
            else:
                current = holder.get(leaf)
                if empty:
                    holder.setdefault(leaf, None)
                    placed[path], fresh[path] = None, False
                    continue
                if isinstance(current, dict):
                    current.update(values)
                    fresh[path] = fresh[parent]
                else:
                    current = holder[leaf] = dict(values)
                    fresh[path] = True
                placed[path] = current

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
            # A missing intermediate key is a structural bug — fail loud, don't mask with {}.
            target = target[part]
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
    one_to_many_paths = _one_to_many_paths(nested_groups)

    if one_to_many_paths and root_cols:
        return _serialize_with_one_to_many(
            rows,
            root_cols,
            nested_groups,
            one_to_many_paths,
            result_limit,
            root_field,
        )

    return _serialize_flat(rows, root_cols, nested_groups, result_limit, root_field)


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


def serialize_group_by(
    rows: list[tuple],
    columns: list[ColumnRef],
    nodes_rows: list[tuple] | None,
    nodes_columns: list[ColumnRef] | None,
    root_field: str,
) -> dict:
    """Serialize group-by query result (with optional per-group nodes) into GraphQL JSON.

    nodes_columns uses nested_in="__join_key__" to mark group-by join columns.
    Returns:
        {"data": {root_field: [{"groupKey": {...}, "aggregate": {...}, "nodes": [...]}, ...]}}
        "nodes" key present on each row only when nodes_rows is not None.
    """
    result = serialize_rows(rows, columns, root_field)

    if nodes_rows is None or nodes_columns is None:
        return result

    join_key_indices = [i for i, c in enumerate(nodes_columns) if c.nested_in == "__join_key__"]
    output_cols = [(i, c) for i, c in enumerate(nodes_columns) if c.nested_in is None]

    nodes_map: dict = {}
    for row in nodes_rows:
        join_key = tuple(_convert_value(row[i]) for i in join_key_indices)
        node_dict = {c.field_name: _convert_value(row[i]) for i, c in output_cols}
        nodes_map.setdefault(join_key, []).append(node_dict)

    group_key_indices = [i for i, c in enumerate(columns) if c.nested_in == "groupKey"]
    group_rows = result["data"][root_field]
    for idx, row in enumerate(rows):
        group_key = tuple(_convert_value(row[j]) for j in group_key_indices)
        group_rows[idx]["nodes"] = nodes_map.get(group_key, [])

    return result


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
