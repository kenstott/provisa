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
import re
from collections.abc import Callable
from datetime import date, timedelta

from dateutil.relativedelta import relativedelta
from decimal import Decimal

from provisa.compiler.cursor import encode_cursor
from provisa.compiler.sql_types import is_integer_type, is_json_type
from provisa.core.ir_types import bytea_hex, infinity_of, iso8601_duration
from provisa.core.json_refusal import JsonValueRefused
from provisa.core.statement_warnings import ServerWarning
from provisa.compiler.sql_gen import ColumnRef, CompiledQuery
from provisa.executor.result import QueryResult


_FRACTION_ZEROS = re.compile(r"(\.\d*?[1-9])0+(?=$|[+-])")


def temporal_text(value: date) -> str:
    """A date, time or timestamp as JSON writes it at any depth: PostgreSQL's json, which has the T
    and no trailing zeros in a fraction. PostgreSQL's infinite date or timestamp is its own text,
    ``infinity`` or ``-infinity``, as pgwire writes it (REQ-1867)."""
    sign = infinity_of(value)
    if sign:
        return "infinity" if sign > 0 else "-infinity"
    return _FRACTION_ZEROS.sub(r"\1", value.isoformat())


def decimal_text(value: Decimal) -> str:
    """A DECIMAL as the JSON string of its exact digits (REQ-1867): never a float, which loses
    digits past about 17 and cannot hold a value past 2^64. Positional, never an exponent:
    ``Decimal("1E+3")`` is ``"1000"``, ``Decimal("0E-7")`` is ``"0.0000000"``."""
    return format(value, "f")


class JsonNumber(float):
    """A number inside JSON text, carrying the text it was written as (REQ-1867): the response
    writes that text (``orjson.Fragment``, json_response), so a number past a float's 17 digits
    keeps them. It is a float everywhere else -- a surface the standard library or pydantic
    writes still writes a JSON number, the nearest float."""

    text: str

    def __new__(cls, text: str) -> JsonNumber:
        number = super().__new__(cls, text)
        number.text = text
        return number

    def exact(self) -> Decimal:
        """The number's exact value."""
        return Decimal(self.text)


def integer_value(
    val: object,  # object-ok: an integer column's value — int | Decimal | None
    field: str,
) -> object:
    """An integer-typed column's value as the JSON number it is (REQ-1867), whatever a reader
    represented it as: a DuckDB HUGEINT read through Arrow is a ``decimal128(38,0)`` Decimal."""
    if isinstance(val, Decimal):
        if val != val.to_integral_value():
            raise ValueError(f"field {field!r} is typed integer but holds {val}")
        return int(val)
    return val


def _convert_value(
    val: object,  # object-ok: arbitrary SQL column value — Decimal | date | str | int | dict | list | None
) -> object:
    """Convert database types to JSON-safe Python types. Text is the string it is, even when it
    reads as JSON (REQ-1867): only a JSON-typed column's text is JSON (:func:`column_value`)."""
    if isinstance(val, Decimal):
        return decimal_text(val)
    if isinstance(val, (timedelta, relativedelta)):
        return iso8601_duration(val)  # an IR interval's canonical text form
    if isinstance(val, (bytes, bytearray, memoryview)):
        return bytea_hex(val)  # an IR bytea's canonical text form
    if hasattr(val, "isoformat"):
        from typing import cast as _cast

        return temporal_text(_cast(date, val))  # guarded by hasattr — any date/datetime-like type
    # A native nested value (a struct, a list, a JSON value a driver already parsed): each
    # element converted as a column value is.
    if isinstance(val, dict):
        return {k: _convert_value(v) for k, v in val.items()}
    if isinstance(val, list):
        return [_convert_value(v) for v in val]
    return val


def _refuse_constant(field: str) -> Callable[[str], object]:
    def refuse(token: str) -> object:
        raise JsonValueRefused(field, f"holds {token}, a number JSON cannot carry")

    return refuse


def _no_lone_surrogate(
    val: object,  # object-ok: a parsed JSON value — dict | list | str | number | bool | None
    field: str,
) -> None:
    """Refuse a parsed JSON value holding a lone UTF-16 surrogate (``"\\ud800"``): it is no
    character, and no UTF-8 response can carry it."""
    if isinstance(val, dict):
        for k, v in val.items():
            _no_lone_surrogate(k, field)
            _no_lone_surrogate(v, field)
    elif isinstance(val, list):
        for v in val:
            _no_lone_surrogate(v, field)
    elif isinstance(val, str):
        try:
            val.encode("utf-8")
        except UnicodeEncodeError:
            raise JsonValueRefused(
                field, "holds a lone UTF-16 surrogate escape, which is no character"
            ) from None


def _parse_json_text(text: str, field: str) -> object:
    """JSON text a JSON-typed column (or the engine, for a relationship) wrote, as its value.
    What JSON cannot carry is refused, naming the field: NaN and Infinity, a lone surrogate,
    nesting deeper than the parser reaches, text that is not JSON."""
    try:
        # A number keeps its own text (JsonNumber); an integer is Python's, any size.
        parsed = json.loads(text, parse_float=JsonNumber, parse_constant=_refuse_constant(field))
    except RecursionError:
        raise JsonValueRefused(field, "nests deeper than a JSON response can carry") from None
    except json.JSONDecodeError as exc:
        raise JsonValueRefused(field, f"is typed JSON but its text is not JSON ({exc})") from None
    if "\\u" in text:
        _no_lone_surrogate(parsed, field)
    return parsed


def _json_walk(
    val: object,  # object-ok: a JSON value — dict | list | str | number | bool | None
    field: str,
    json_text: frozenset[str],
    path: str,
) -> object:
    """A JSON value's elements as JSON writes them. A string at one of ``json_text``'s key paths
    (a list's elements share their list's path) is JSON text the engine wrote and is parsed; any
    other string is text."""
    if isinstance(val, dict):
        out: dict = {}
        for k, v in val.items():
            at = f"{path}{k}"
            if isinstance(v, str) and at in json_text:
                v = _parse_json_text(v, field)
            out[k] = _json_walk(v, field, json_text, f"{at}.")
        return out
    if isinstance(val, list):
        return [_json_walk(v, field, json_text, path) for v in val]
    return _convert_value(val)


def json_value(
    val: object,  # object-ok: a JSON column's value — JSON text, or a value a driver parsed
    field: str,
    json_text: frozenset[str] = frozenset(),
) -> object:
    """A JSON-typed value (a json or jsonb column, a relationship built as JSON) as the JSON it
    holds: its text parsed, refused by name where JSON cannot carry it (REQ-1867)."""
    if isinstance(val, str):
        val = _parse_json_text(val, field)
    return _json_walk(val, field, json_text, "")


def column_value(
    val: object,  # object-ok: arbitrary SQL column value
    col: ColumnRef,
) -> object:
    """One result value of ``col`` as JSON writes it: a JSON-typed column's as its JSON, an
    integer-typed column's as its number, any other as :func:`_convert_value` converts it
    (REQ-1867)."""
    if col.json:
        return json_value(val, col.field_name, col.json_text)
    if col.integer:
        return integer_value(val, col.field_name)
    return _convert_value(val)


def result_records(result: QueryResult) -> list[dict]:
    """A result's rows as ``{column: value}``, each value as JSON writes it: a column the engine
    types json or jsonb as its JSON, one it types an integer as its number (by the engine's type,
    not its reader's representation), any other as :func:`_convert_value` converts it."""
    names = result.column_names
    types = result.column_types or [None] * len(names)
    cols = [
        ColumnRef(None, c, c, None, json=is_json_type(t), integer=is_integer_type(t))
        for c, t in zip(names, types, strict=True)
    ]
    return [
        {col.field_name: column_value(v, col) for col, v in zip(cols, r, strict=True)}
        for r in result.rows
    ]


def _exact_numbers(
    v: object,  # object-ok: a JSON value — dict | list | scalar
) -> object:
    """``v`` with each JsonNumber its exact value: two numbers a float cannot tell apart are two
    values."""
    if isinstance(v, JsonNumber):
        return v.exact()
    if isinstance(v, dict):
        return {k: _exact_numbers(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_exact_numbers(x) for x in v]
    return v


def _to_hashable(
    v: object,  # object-ok: arbitrary SQL column value — dict | list | scalar
) -> object:
    """Make a value safe to use as a dict key / tuple component."""
    if isinstance(v, (dict, list)):
        return json.dumps(_exact_numbers(v), sort_keys=True, default=str)
    return _exact_numbers(v)


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
    #: (the list's place in the response, its values) -> the element. A list's place is its
    #: parent's place and its path, and the list it is: a list a later row puts in place of an
    #: earlier one is a new list, whose elements are its own.
    elements: dict[tuple, dict] = {}
    #: a list's place (its parent's place, its path) -> (the list, its number at that place)
    lists: dict[tuple, tuple[list, int]] = {}
    paths = sorted(nested_groups)

    for row in rows:
        root_key = tuple(_to_hashable(column_value(row[idx], col)) for idx, col in root_cols)
        new_root = root_key not in seen
        if new_root:
            obj: dict = {col.field_name: column_value(row[idx], col) for idx, col in root_cols}
            seen[root_key] = len(result_rows)
            result_rows.append(obj)
        placed: dict[str, dict | None] = {"": result_rows[seen[root_key]]}
        fresh: dict[str, bool] = {"": new_root}
        #: each placed object's place in the response: the root row's key, then each path and
        #: the element that path names
        place: dict[str, tuple] = {"": root_key}
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
            values = {col.field_name: column_value(row[idx], col) for idx, col in cols}
            empty = all(row[idx] is None for idx, _ in cols)
            if path in one_to_many_paths:
                at = (place[parent], path)
                arr = holder.get(leaf)
                known = lists.get(at)
                if known is None or known[0] is not arr:
                    if not isinstance(arr, list):
                        arr = holder[leaf] = []
                    known = lists[at] = (arr, known[1] + 1 if known else 0)
                arr = known[0]
                if empty:
                    placed[path], fresh[path] = None, False
                    continue
                key = (at, known[1], tuple(_to_hashable(v) for v in values.values()))
                element = elements.get(key)
                fresh[path] = element is None
                if element is None:
                    element = elements[key] = dict(values)
                    arr.append(element)
                placed[path] = element
                place[path] = key
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
                place[path] = (place[parent], path)

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
                nested_obj[col.field_name] = column_value(row[idx], col)
            target[leaf] = nested_obj


def _detect_truncated_paths(
    result_rows: list[dict],
    seen_root_keys: dict[tuple, int],
    nested_groups: dict[str, list[tuple[int, ColumnRef]]],
    row: tuple,
    root_key: tuple,
) -> set[str]:
    """Return set of nested paths that differ from the already-seen row: the to-one object this
    row would build (None when its columns are all null) against the one the first row built,
    by the path's own fields (a nested path's object sits inside it as another key)."""
    truncated: set[str] = set()
    prev_obj = result_rows[seen_root_keys[root_key]]
    for nest_path, nest_cols in nested_groups.items():
        cur = (
            None
            if all(row[idx] is None for idx, _ in nest_cols)
            else {col.field_name: column_value(row[idx], col) for idx, col in nest_cols}
        )
        parts = nest_path.split(".")
        target: dict | None = prev_obj
        for part in parts[:-1]:
            assert target is not None
            # A missing intermediate key is a structural bug — fail loud, don't mask with {}.
            target = target[part]
            if target is None:
                break  # a parent the first row had null: it built nothing here
        prev_val = None if target is None else target.get(parts[-1])
        prev = (
            {col.field_name: prev_val.get(col.field_name) for _, col in nest_cols}
            if isinstance(prev_val, dict)
            else prev_val
        )
        if _to_hashable(cur) != _to_hashable(prev):
            truncated.add(nest_path)
    return truncated


def _serialize_flat(
    rows: list[tuple],
    columns: list[ColumnRef],
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
        keyed = [idx for idx, col in root_cols if not col.is_agg]
        if keyed:
            root_key = tuple(_to_hashable(column_value(row[idx], columns[idx])) for idx in keyed)
        else:
            # group_by: every column is nested (groupKey/aggregates) or a group's nodes -- every
            # row is distinct
            root_key = tuple(
                _to_hashable(column_value(v, col)) for v, col in zip(row, columns, strict=True)
            )
        if root_key in seen_root_keys:
            truncated_paths |= _detect_truncated_paths(
                result_rows, seen_root_keys, nested_groups, row, root_key
            )
            continue

        obj: dict = {}
        for idx, col in root_cols:
            obj[col.field_name] = column_value(row[idx], col)
        _build_nested_obj(obj, nested_groups, row)
        seen_root_keys[root_key] = len(result_rows)
        result_rows.append(obj)

    if result_limit is not None:
        result_rows = result_rows[:result_limit]

    result: dict = {"data": {root_field: result_rows}}
    if truncated_paths:
        result["extensions"] = {
            "warnings": [
                {"message": to_one_truncated(path).message, "path": path}
                for path in sorted(truncated_paths)
            ]
        }
    return result


#: REQ-1350: the code of the warning a to-one relationship that matched several rows raises
TO_ONE_TRUNCATED = "graphql.to_one_truncated"


def to_one_truncated(path: str) -> ServerWarning:
    """The warning a many-to-one relationship at ``path`` that matched several rows for one
    parent raises: only the first row's object was answered."""
    return ServerWarning(
        code=TO_ONE_TRUNCATED,
        message=(
            f"many-to-one relationship '{path}' returned multiple rows "
            f"for the same parent; only the first value was used. "
            f"Check cardinality — this relationship may need to be one-to-many."
        ),
        params={"path": path},
    )


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

    return _serialize_flat(rows, columns, root_cols, nested_groups, result_limit, root_field)


def serialize_aggregate(  # REQ-196, REQ-197
    agg_rows: list[tuple],
    agg_columns: list[ColumnRef],
    root_field: str,
    agg_alias: str = "aggregate",
) -> dict:
    """Serialize an aggregate query's one row into GraphQL JSON: its aggregates, and its nodes
    (the rows it aggregates, a column of that row) when they were selected.

    Column nested_in paths use agg_alias as prefix (default "aggregate"), e.g.:
      "aggregate"      → aggregate.count
      "aggregate.sum"  → aggregate.sum.amount
    The nodes column is the JSON list at the root (nested_in None, is_agg); any other column
    with no nested_in is the aggregate's own.

    Returns:
        {"data": {root_field: {agg_alias: {...}, "nodes": [...]}}}
    """
    # Strip the leading agg_alias prefix from each nested_in path, then reconstruct the
    # sub-structure (sum, avg, min, max).
    agg_inner: dict = {}
    payload: dict = {agg_alias: agg_inner}
    if agg_rows:
        row = agg_rows[0]
        for i, col in enumerate(agg_columns):
            if col.nested_in is None and col.is_agg:
                payload[col.field_name] = column_value(row[i], col)
                continue
            path = col.nested_in or agg_alias
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
            target[col.field_name] = column_value(row[i], col)

    return {"data": {root_field: payload}}


def _cursor_value(
    val: object,  # object-ok: a sort column's value
    col: ColumnRef,
) -> object:
    """A sort key's value in the opaque cursor, which compiles back into a keyset comparison
    (cursor.cursor_where_clause): a DECIMAL stays the Decimal it is, which the cursor carries by
    its exact digits and the comparison binds as a decimal (REQ-218, REQ-1867)."""
    if isinstance(val, Decimal) and not col.integer:
        return val
    return column_value(val, col)


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
            node[col.field_name] = column_value(row[i], col)

        cursor_vals = []
        for sc in sort_columns:
            idx = col_index.get(sc)
            if idx is not None:
                cursor_vals.append(_cursor_value(row[idx], columns[idx]))
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
