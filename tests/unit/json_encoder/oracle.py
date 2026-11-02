# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The JSON surfaces' Python path as it was before the Rust encoders replaced it (J504w, REQ-2194),
frozen here as the differential's reference: ``serialize_rows`` / ``serialize_aggregate``
(executor/serialize.py), JSON:API's ``rows_to_jsonapi``, ``_extract_included``,
``rename_row_keys`` and ``build_pagination_links``, OData's ``expand.write`` and ``edm_value``.

The reference is the old path *corrected* where the wiring fixed a defect (each correction is
marked ``CORRECTED (n)`` with the defect's number in rust/encoders/json/RESULTS.md):

- (1) rows (and list elements, and to-one objects compared for truncation) are one when they are
  the same typed value (``_key``), not when Python's ``==`` or ``json.dumps(default=str)`` finds
  them equal;
- (2) a zoned timestamp is read as zoneinfo and PostgreSQL read it (tzdata, J498), not by pytz;
- (3) a nanosecond timestamp or duration is the instant floored to the microsecond (no pandas
  ``NaT``, no int64 bound on the local time);
- (4) a JSON:API group whose every key is NULL has that key, each field null;
- (5) an include named twice is the one include;
- (6) a JSON:API id is the text of the value as the answer writes it, and a column named ``id``
  is an attribute unless the id is that column;
- (7) a truncated to-one path is reported (JSON:API's ``meta.warnings``; GraphQL's and REST's
  warnings), the object compared as the row built it;
- (9) OData's Edm.Date, Edm.TimeOfDay and Edm.Boolean are typed: a value that is none raises, and
  an infinite date or timestamp is ``"infinity"`` / ``"-infinity"``.
"""

from __future__ import annotations

import base64
import functools
import json
import math
from collections.abc import Mapping
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from importlib import resources
from typing import Any
from urllib.parse import quote, urlencode
from zoneinfo import ZoneInfo

import orjson
import pyarrow as pa
import pyarrow.compute as pc

from provisa.api.json_response import dumps
from provisa.compiler.sql_types import ColumnRef
from provisa.core.ir_arrow import column_values as product_column_values
from provisa.core.ir_types import infinite_timestamp, infinity_of
from provisa.executor.serialize import JsonNumber, column_value, to_one_truncated

# ------------------------------------------------------------------ reading Arrow


def _has(t: pa.DataType, test: Any) -> bool:  # noqa: ANN401
    if test(t):
        return True
    if pa.types.is_struct(t):
        return any(_has(t.field(i).type, test) for i in range(t.num_fields))
    if pa.types.is_map(t):
        return _has(t.key_type, test) or _has(t.item_type, test)
    if pa.types.is_list(t) or pa.types.is_large_list(t) or pa.types.is_fixed_size_list(t):
        return _has(t.value_type, test)
    if pa.types.is_list_view(t) or pa.types.is_large_list_view(t):
        return _has(t.value_type, test)
    if pa.types.is_dictionary(t):
        return _has(t.value_type, test)
    return False


def _is_ns(t: pa.DataType) -> bool:
    return (pa.types.is_timestamp(t) or pa.types.is_duration(t)) and t.unit == "ns"


def _floor_us(a: pa.Array) -> pa.Array:
    """A nanosecond timestamp or duration array as microseconds, each floored."""
    raw = a.cast(pa.int64())
    q = pc.call_function("divide", [raw, pa.scalar(1000, pa.int64())])
    over = pc.call_function(
        "greater", [pc.call_function("multiply", [q, pa.scalar(1000, pa.int64())]), raw]
    )
    q = pc.call_function("subtract", [q, over.cast(pa.int64())])
    unit = pa.timestamp("us", a.type.tz) if pa.types.is_timestamp(a.type) else pa.duration("us")
    return q.cast(unit)


def _floored(a: pa.Array) -> pa.Array:
    """CORRECTED (3): ``a`` with every nanosecond value at any depth floored to the microsecond."""
    t = a.type
    if not _has(t, _is_ns):
        return a
    if _is_ns(t):
        return _floor_us(a)
    nulls = a.is_null() if a.null_count else None
    if pa.types.is_list(t) or pa.types.is_large_list(t):
        first = a.offsets[0].as_py()
        flat = a.values.slice(first, a.offsets[-1].as_py() - first)
        offsets = pc.call_function("subtract", [a.offsets, a.offsets[0]])
        cls = pa.ListArray if pa.types.is_list(t) else pa.LargeListArray
        return cls.from_arrays(offsets, _floored(flat), mask=nulls)
    if pa.types.is_fixed_size_list(t):
        flat = a.values.slice(a.offset * t.list_size, len(a) * t.list_size)
        return pa.FixedSizeListArray.from_arrays(_floored(flat), t.list_size, mask=nulls)
    if pa.types.is_struct(t):
        kids = [_floored(a.field(i)) for i in range(t.num_fields)]
        return pa.StructArray.from_arrays(
            kids, fields=[t.field(i).with_type(k.type) for i, k in enumerate(kids)], mask=nulls
        )
    if pa.types.is_map(t):
        first = a.offsets[0].as_py()
        n = a.offsets[-1].as_py() - first
        offsets = pc.call_function("subtract", [a.offsets, a.offsets[0]])
        return pa.MapArray.from_arrays(
            offsets,
            _floored(a.keys.slice(first, n)),
            _floored(a.items.slice(first, n)),
            mask=nulls,
        )
    if pa.types.is_dictionary(t):
        return pa.DictionaryArray.from_arrays(a.indices, _floored(a.dictionary))
    raise TypeError(f"the oracle floors no nanosecond value inside {t}")


@functools.cache
def tzdata_zone(name: str) -> ZoneInfo:
    """The IANA zone as the pinned ``tzdata`` release has it (the release the encoders' chrono-tz
    tables are built from, rust/pgwire/RESULTS.md), not the host's /usr/share/zoneinfo."""
    with resources.files("tzdata.zoneinfo").joinpath(name).open("rb") as f:
        return ZoneInfo.from_file(f, key=name)


def _zone(name: str) -> Any:  # noqa: ANN401 — a tzinfo
    """CORRECTED (2): ir_arrow._zone (J498): ``UTC`` and ``+HH:MM`` fixed, else the IANA zone."""
    if name == "UTC":
        return timezone.utc
    if len(name) == 6 and name[0] in "+-" and name[3] == ":":
        east = timedelta(hours=int(name[1:3]), minutes=int(name[4:6]))
        return timezone(-east if name[0] == "-" else east)
    return tzdata_zone(name)


def _tz_name(tz: Any) -> str:  # noqa: ANN401 — the tzinfo pyarrow gave
    zone = getattr(tz, "zone", None)
    if zone is not None:
        return zone
    minutes = getattr(tz, "_minutes", None)  # pytz.FixedOffset
    if minutes is None:
        offset = tz.utcoffset(None)
        minutes = int(offset.total_seconds() // 60) if offset is not None else 0
    if minutes == 0:
        return "UTC"
    sign = "-" if minutes < 0 else "+"
    return f"{sign}{abs(minutes) // 60:02d}:{abs(minutes) % 60:02d}"


def _rezoned(v: Any) -> Any:  # noqa: ANN401 — a to_pylist value
    """CORRECTED (2): each aware datetime at any depth, the same instant in its zone by zoneinfo."""
    if isinstance(v, datetime) and v.tzinfo is not None and not infinity_of(v):
        return v.astimezone(_zone(_tz_name(v.tzinfo)))
    if isinstance(v, list):
        return [_rezoned(x) for x in v]
    if isinstance(v, tuple):
        return tuple(_rezoned(x) for x in v)
    if isinstance(v, dict):
        return {k: _rezoned(x) for k, x in v.items()}
    return v


_TIMESTAMP_INFINITY = 2**63 - 1


def column_values(column: pa.Array) -> list[Any]:
    """ir_arrow.column_values, CORRECTED (2) and (3): a zoned timestamp in its zone by zoneinfo
    (J498 ``_zoned_values``), a nanosecond value floored to the microsecond."""
    t = column.type
    was_us = pa.types.is_timestamp(t) and t.unit == "us"
    column = _floored(column)
    t = column.type
    if pa.types.is_timestamp(t) and t.tz is not None:
        zone = _zone(t.tz)
        utc = column.cast(pa.timestamp(t.unit))
        if was_us:
            raw = utc.cast(pa.int64())
            ends = pc.call_function(
                "equal",
                [pc.call_function("abs", [raw]), pa.scalar(_TIMESTAMP_INFINITY, pa.int64())],
            )
            ends = pc.fill_null(ends, False)
            finite = pc.call_function("if_else", [ends, pa.scalar(None, utc.type), utc])
            values = [
                infinite_timestamp(1 if r > 0 else -1, aware=True) if end else v
                for v, r, end in zip(
                    finite.to_pylist(), raw.to_pylist(), ends.to_pylist(), strict=True
                )
            ]
        else:
            values = utc.to_pylist()
        return [
            v if v is None or infinity_of(v) else zone.fromutc(v.replace(tzinfo=zone))
            for v in values
        ]
    if pa.types.is_timestamp(t) and not was_us:
        return column.to_pylist()  # a floored ns column carries no infinity marker
    values = product_column_values(column)
    if _has(t, lambda x: pa.types.is_timestamp(x) and x.tz is not None):
        values = _rezoned(values)
    return values


def arrow_rows(batches: list[Any]) -> list[tuple]:  # noqa: ANN401
    """The rows the drain collects, each column read as ``column_values`` reads it."""
    rows: list[tuple] = []
    for rb in batches:
        if not rb.num_rows:
            continue
        cols = [column_values(c) for c in rb.columns]
        rows.extend(zip(*cols, strict=True))
    return rows


# ------------------------------------------------------------------ the dedup key


def _key(v: Any) -> Any:  # noqa: ANN401 — a converted value
    """CORRECTED (1): a value's identity as a typed value: a string is never a number, ``1`` is
    not ``1.0`` nor ``true`` (a JSON number keeps its digits), a JSON array is never the text of
    one, and NaN is NaN."""
    if v is None:
        return ("n",)
    if isinstance(v, bool):
        return ("b", v)
    if isinstance(v, JsonNumber):
        return ("j", v.text)
    if isinstance(v, int):
        return ("i", v)
    if isinstance(v, float):
        return ("f", "nan" if math.isnan(v) else v.hex())
    if isinstance(v, str):
        return ("s", v)
    if isinstance(v, list):
        return ("l", tuple(_key(x) for x in v))
    if isinstance(v, tuple):
        return ("t", tuple(_key(x) for x in v))
    if isinstance(v, dict):
        return ("d", tuple((k, _key(x)) for k, x in v.items()))
    return ("o", repr(v))


# ------------------------------------------------------------------ serialize_rows


def _group_columns(columns: list[ColumnRef]):
    root_cols: list[tuple[int, ColumnRef]] = []
    nested_groups: dict[str, list[tuple[int, ColumnRef]]] = {}
    for i, col in enumerate(columns):
        if col.nested_in is None:
            root_cols.append((i, col))
        else:
            nested_groups.setdefault(col.nested_in, []).append((i, col))
    return root_cols, nested_groups


def _parent_path(path: str, placed: dict[str, dict | None]) -> str:
    parts = path.split(".")
    for i in range(len(parts) - 1, 0, -1):
        prefix = ".".join(parts[:i])
        if prefix in placed:
            return prefix
    return ""


def _serialize_with_one_to_many(rows, root_cols, nested_groups, one_to_many_paths, result_limit):
    seen: dict[tuple, int] = {}
    result_rows: list[dict] = []
    elements: dict[tuple, dict] = {}
    lists: dict[tuple, tuple[list, int]] = {}
    paths = sorted(nested_groups)
    for row in rows:
        root_key = tuple(_key(column_value(row[idx], col)) for idx, col in root_cols)
        new_root = root_key not in seen
        if new_root:
            obj: dict = {col.field_name: column_value(row[idx], col) for idx, col in root_cols}
            seen[root_key] = len(result_rows)
            result_rows.append(obj)
        placed: dict[str, dict | None] = {"": result_rows[seen[root_key]]}
        place: dict[str, tuple] = {"": root_key}
        for path in paths:
            parent = _parent_path(path, placed)
            holder = placed[parent]
            if holder is None:
                placed[path] = None
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
                    placed[path] = None
                    continue
                key = (at, known[1], tuple(_key(v) for v in values.values()))
                element = elements.get(key)
                if element is None:
                    element = elements[key] = dict(values)
                    arr.append(element)
                placed[path] = element
                place[path] = key
            else:
                current = holder.get(leaf)
                if empty:
                    holder.setdefault(leaf, None)
                    placed[path] = None
                    continue
                if isinstance(current, dict):
                    current.update(values)
                else:
                    current = holder[leaf] = dict(values)
                placed[path] = current
                place[path] = (place[parent], path)
    if result_limit is not None:
        result_rows = result_rows[:result_limit]
    return result_rows, []


def _object_key(row: tuple, nest_cols: list[tuple[int, ColumnRef]]) -> Any:  # noqa: ANN401
    if all(row[idx] is None for idx, _ in nest_cols):
        return _key(None)
    return _key({col.field_name: column_value(row[idx], col) for idx, col in nest_cols})


def _build_nested_obj(obj: dict, nested_groups, row: tuple) -> dict[str, bool]:
    skipped: dict[str, bool] = {}
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
        skipped[nest_path] = skip
        if skip:
            continue
        assert target is not None
        leaf = parts[-1]
        if all_none:
            target[leaf] = None
        else:
            target[leaf] = {col.field_name: column_value(row[idx], col) for idx, col in nest_cols}
    return skipped


def _walk_fails(prev_obj: dict, nested_groups) -> bool:
    """Whether _detect_truncated_paths' walk of the first row's object raises (a parent that is
    no object): the repeat is then an error, as it was."""
    for nest_path in nested_groups:
        parts = nest_path.split(".")
        target: Any = prev_obj
        for part in parts[:-1]:
            if not isinstance(target, dict) or part not in target:
                return True
            target = target[part]
            if target is None:
                break
        if target is not None and not isinstance(target, dict):
            return True  # the leaf is read with .get: a parent that is no object raises
    return False


def _serialize_flat(rows, columns, root_cols, nested_groups, result_limit):
    seen_root_keys: dict[tuple, int] = {}
    result_rows: list[dict] = []
    objects: list[dict[str, Any]] = []
    fails: list[bool] = []
    truncated_paths: set[str] = set()
    for row in rows:
        keyed = [idx for idx, col in root_cols if not col.is_agg]
        if keyed:
            root_key = tuple(_key(column_value(row[idx], columns[idx])) for idx in keyed)
        else:
            root_key = tuple(
                _key(column_value(v, col)) for v, col in zip(row, columns, strict=True)
            )
        if root_key in seen_root_keys:
            at = seen_root_keys[root_key]
            # CORRECTED (7): each to-one object against the one the first row built.
            current = {p: _object_key(row, cols) for p, cols in nested_groups.items()}
            if fails[at]:
                raise KeyError("a repeated row's relationship path is not an object")
            truncated_paths |= {p for p in nested_groups if current[p] != objects[at][p]}
            continue
        obj: dict = {}
        for idx, col in root_cols:
            obj[col.field_name] = column_value(row[idx], col)
        skipped = _build_nested_obj(obj, nested_groups, row)
        fails.append(_walk_fails(obj, nested_groups))
        objects.append(
            {
                p: _key(None) if skipped[p] else _object_key(row, cols)
                for p, cols in nested_groups.items()
            }
        )
        seen_root_keys[root_key] = len(result_rows)
        result_rows.append(obj)
    if result_limit is not None:
        result_rows = result_rows[:result_limit]
    return result_rows, sorted(truncated_paths)


def serialize_rows(rows, columns, result_limit=None) -> tuple[list[dict], list[str]]:
    """The field's rows and the truncated to-one paths."""
    root_cols, nested_groups = _group_columns(columns)
    one_to_many = {
        p
        for p, cols in nested_groups.items()
        if any(c.cardinality == "one-to-many" for _, c in cols)
    }
    if one_to_many and root_cols:
        return _serialize_with_one_to_many(
            rows, root_cols, nested_groups, one_to_many, result_limit
        )
    return _serialize_flat(rows, columns, root_cols, nested_groups, result_limit)


def serialize_aggregate(agg_rows, agg_columns, agg_alias: str) -> dict:
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
    return payload


def truncation_warnings(truncated: list[str]) -> list[dict]:
    return [to_one_truncated(path).as_dict() for path in truncated]


# ------------------------------------------------------------------ JSON:API


def _id_text(value: Any) -> str:  # noqa: ANN401
    """CORRECTED (6): a string as itself, any other value as its JSON."""
    if isinstance(value, str):
        return value
    written = dumps(value)
    return json.loads(written) if written.startswith(b'"') else written.decode()


def resource_id(row: dict[str, Any], id_columns: list[str]) -> str:
    if len(id_columns) == 1:
        return _id_text(row[id_columns[0]])
    return ":".join(_id_text(row[c]).replace("\\", "\\\\").replace(":", "\\:") for c in id_columns)


def row_to_resource(row, resource_type, relationship_fields, *, id_columns, links=None):
    fk_columns = set(relationship_fields)
    rid = resource_id(row, id_columns) if id_columns is not None else _id_text(row.get("id", ""))
    # CORRECTED (6): ``id`` is the id's own column only when the id is that column.
    id_is_column = id_columns is None or list(id_columns) == ["id"]
    attributes: dict[str, Any] = {}
    relationships: dict[str, Any] = {}
    for key, value in row.items():
        if id_is_column and key == "id":
            continue
        if key in fk_columns:
            related_type = relationship_fields[key]
            rel_name = key[:-3]
            relationships[rel_name] = {
                "data": {"type": related_type, "id": _id_text(value)} if value is not None else None
            }
        else:
            attributes[key] = value
    resource: dict[str, Any] = {"type": resource_type, "id": rid, "attributes": attributes}
    relationships.update(links or {})
    if relationships:
        resource["relationships"] = relationships
    return resource


def extract_included(rows, include_names, id_columns):
    included_rows: dict[str, list[dict[str, Any]]] = {}
    seen: dict[str, set[str]] = {}
    links: list[dict[str, Any]] = []
    for row in rows:
        row_links: dict[str, Any] = {}
        for inc in include_names:
            nested = row.pop(inc, None)
            items = nested if isinstance(nested, list) else [nested] if nested else []
            bucket = included_rows.setdefault(inc, [])
            ids = seen.setdefault(inc, set())
            identifiers = []
            for item in items:
                rid = resource_id(item, id_columns[inc])
                identifiers.append({"type": inc, "id": rid})
                if rid not in ids:
                    bucket.append(item)
                    ids.add(rid)
            if isinstance(nested, list):
                row_links[inc] = {"data": identifiers}
            else:
                row_links[inc] = {"data": identifiers[0] if identifiers else None}
        links.append(row_links)
    return {k: v for k, v in included_rows.items() if v}, links


def _rename_nested(value: Any, column_map: dict[str, str]) -> Any:  # noqa: ANN401
    if isinstance(value, dict):
        return {column_map.get(k, k): v for k, v in value.items()}
    if isinstance(value, list):
        return [_rename_nested(v, column_map) for v in value]
    return value


def rename_row_keys(row, gql_to_physical, rel_gql_to_physical, rel_scalar_maps):
    out: dict[str, Any] = {}
    for key, value in row.items():
        if key in rel_gql_to_physical:
            out[rel_gql_to_physical[key]] = _rename_nested(value, rel_scalar_maps.get(key, {}))
        else:
            out[gql_to_physical.get(key, key)] = value
    return out


def build_pagination_links(base_url, page_number, page_size, total, query_params, *, has_next):
    extra = query_params or {}

    def _url(pn: int) -> str:
        p = {"page[number]": str(pn), "page[size]": str(page_size)}
        p.update(extra)
        return f"{base_url}?{urlencode(p)}"

    links: dict[str, str | None] = {"self": _url(page_number), "first": _url(1)}
    if total is not None:
        last_page = max(1, (total + page_size - 1) // page_size)
        links["last"] = _url(last_page)
        has_next = page_number < last_page
    links["prev"] = _url(page_number - 1) if page_number > 1 else None
    links["next"] = _url(page_number + 1) if has_next else None
    return links


def jsonapi_document(
    rows,
    *,
    truncated: list[str],
    table_name: str,
    page_number: int,
    page_size: int,
    probe_rows: int,
    total_count: int | None,
    base_keys: list[str],
    gql_to_physical,
    rel_gql_to_physical,
    rel_scalar_gql_to_physical,
    physical_include_names: list[str],
    include_keys,
    base_path: str,
    extra: dict[str, str],
    warnings: list[dict] | None,
) -> dict:
    """_jsonapi_table_endpoint, from the rows on."""
    has_next = len(rows) > page_size if probe_rows else len(rows) == page_size
    rows = rows[:page_size]
    rows = [
        rename_row_keys(row, gql_to_physical, rel_gql_to_physical, rel_scalar_gql_to_physical)
        for row in rows
    ]
    # CORRECTED (5): an include named twice is the one include.
    includes = list(dict.fromkeys(physical_include_names))
    relationship_fields = {f"{p}_id": p for p in rel_gql_to_physical.values()}
    included_rows, row_links = extract_included(rows, includes, include_keys)
    data = [
        row_to_resource(
            row,
            table_name,
            relationship_fields,
            id_columns=base_keys,
            links=row_links[i] if includes else None,
        )
        for i, row in enumerate(rows)
    ]
    doc: dict[str, Any] = {"data": data}
    included = [
        row_to_resource(item, inc, {}, id_columns=include_keys.get(inc))
        for inc, items in included_rows.items()
        for item in items
    ]
    if included:
        doc["included"] = included
    meta: dict[str, Any] = {}
    if total_count is not None:
        meta["total"] = total_count
    # CORRECTED (7): each truncated to-one path's warning, after the request's.
    all_warnings = list(warnings or []) + truncation_warnings(truncated)
    if all_warnings:
        meta["warnings"] = all_warnings
    if meta:
        doc["meta"] = meta
    doc["links"] = build_pagination_links(
        base_path, page_number, page_size, total_count, extra or None, has_next=has_next
    )
    return doc


def jsonapi_group_by(
    group_rows,
    columns,
    *,
    table_name,
    gql_to_physical,
    rel_gql_to_physical,
    rel_scalar_gql_to_physical,
):
    key_fields = list(dict.fromkeys(c.field_name for c in columns if c.nested_in == "groupKey"))
    for row in group_rows:
        if "groupKey" in row:
            key = row["groupKey"]
            if key is None:
                # CORRECTED (4): a group whose every key is NULL is that key, each field null.
                key = dict.fromkeys(key_fields)
            row["groupKey"] = {gql_to_physical.get(k, k): v for k, v in key.items()}
        if "nodes" in row:
            row["nodes"] = [
                rename_row_keys(
                    node, gql_to_physical, rel_gql_to_physical, rel_scalar_gql_to_physical
                )
                for node in row["nodes"]
            ]
    return {
        "data": [
            {"type": f"{table_name}GroupBy", "id": str(idx), "attributes": row}
            for idx, row in enumerate(group_rows)
        ]
    }


# ------------------------------------------------------------------ OData


def _datetime_offset(value: Any) -> str:  # noqa: ANN401
    if infinity_of(value):
        # CORRECTED (9): PostgreSQL's infinity, as every JSON surface writes it.
        return "infinity" if infinity_of(value) > 0 else "-infinity"
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    elif not isinstance(value, datetime):
        value = datetime(value.year, value.month, value.day)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat().replace("+00:00", "Z")


def _binary(value: Any) -> str:  # noqa: ANN401
    if isinstance(value, str):
        value = bytes.fromhex(value[2:]) if value.startswith("\\x") else value.encode()
    return base64.urlsafe_b64encode(bytes(value)).decode()


def _floating(value: Any) -> Any:  # noqa: ANN401
    number = float(value)
    if math.isnan(number):
        return "NaN"
    if math.isinf(number):
        return "INF" if number > 0 else "-INF"
    return number


def _decimal(value: Any) -> Any:  # noqa: ANN401
    exact = value if isinstance(value, Decimal) else Decimal(str(value))
    if not exact.is_finite():
        return _floating(exact)
    return orjson.Fragment(format(exact, "f"))


def _string(value: Any) -> str:  # noqa: ANN401
    if isinstance(value, str):
        return value
    if isinstance(value, (dict, list)):
        return orjson.dumps(value, default=str).decode()
    return str(value)


def _date(value: Any) -> str:  # noqa: ANN401
    """CORRECTED (9): a date, a timestamp's date, or engine text of either; else it raises."""
    sign = infinity_of(value)
    if sign:
        return "infinity" if sign > 0 else "-infinity"
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str):
        return datetime.fromisoformat(value).date().isoformat()
    raise TypeError(f"is no Edm.Date: {value!r}")


def _time_of_day(value: Any) -> str:  # noqa: ANN401
    """CORRECTED (9): a time, a timestamp's time of day, or engine text of a time; else it
    raises."""
    if isinstance(value, time):
        return value.replace(tzinfo=None).isoformat()
    if isinstance(value, datetime):
        return value.time().isoformat()
    if isinstance(value, str):
        return time.fromisoformat(value).isoformat()
    raise TypeError(f"is no Edm.TimeOfDay: {value!r}")


def _boolean(value: Any) -> bool:  # noqa: ANN401
    """CORRECTED (9): a boolean, 0 or 1, or text PostgreSQL's boolin reads; else it raises."""
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        t = value.strip().lower()

        def prefix(word: str, least: int) -> bool:
            return len(t) >= least and word.startswith(t)

        if t == "1" or prefix("true", 1) or prefix("yes", 1) or prefix("on", 2):
            return True
        if t == "0" or prefix("false", 1) or prefix("no", 1) or prefix("off", 2):
            return False
    raise TypeError(f"is no Edm.Boolean: {value!r}")


_WRITERS = {
    "Edm.Binary": _binary,
    "Edm.Boolean": _boolean,
    "Edm.Date": _date,
    "Edm.DateTimeOffset": _datetime_offset,
    "Edm.Decimal": _decimal,
    "Edm.Double": _floating,
    "Edm.Guid": str,
    "Edm.Int16": int,
    "Edm.Int32": int,
    "Edm.Int64": int,
    "Edm.SByte": int,
    "Edm.Single": _floating,
    "Edm.String": _string,
    "Edm.TimeOfDay": _time_of_day,
}


def edm_value(value: Any, edm: Any) -> Any:  # noqa: ANN401
    return None if value is None else _WRITERS[edm.name](value)


def _key_literal(value: Any, edm: Any) -> str:  # noqa: ANN401
    written = edm_value(value, edm)
    if edm.name == "Edm.String":
        return "'" + str(written).replace("'", "''") + "'"
    if isinstance(written, bool):
        return "true" if written else "false"
    if isinstance(written, orjson.Fragment):
        return format(Decimal(str(value)), "f")
    return str(written)


def _address(entity: Any, key: tuple[str, ...], values: Mapping[str, Any]) -> str:  # noqa: ANN401
    def literal(name: str) -> str:
        found = entity.property(name)
        assert found is not None
        return _key_literal(values[name], found.type)

    if len(key) == 1:
        return f"{entity.name}({literal(key[0])})"
    return f"{entity.name}({','.join(f'{name}={literal(name)}' for name in key)})"


def _link(root: str, entity: Any, values: Mapping[str, Any], plan: Any) -> str:  # noqa: ANN401
    from provisa.api.odata.errors import QueryOptionRefused
    from provisa.api.odata.expand import page_link

    name = plan.navigation.name
    if not plan.parent_key:
        raise QueryOptionRefused(
            501,
            "not_implemented",
            f"{name} of {entity.name} holds more than {plan.page} rows, and a row of "
            f"{entity.name} has no key this role can address the rest by",
        )
    address = quote(_address(entity, plan.parent_key, values), safe="(),='")
    return page_link(f"{root}/{address}/{name}", plan.raw, plan.options, plan.page)


def _decoded(value: Any) -> Any:  # noqa: ANN401
    return json.loads(value, parse_float=Decimal) if isinstance(value, str) else value


def _expanded(written: dict, values: Mapping[str, Any], entity: Any, plan: Any, root: str) -> None:  # noqa: ANN401
    name = plan.navigation.name
    if not plan.navigation.collection:
        found = _decoded(values[name])
        written[name] = (
            None if found is None else write(found, plan.target, plan.fields, plan.children, root)
        )
        return
    rows = _decoded(values[name]) if plan.page > 0 else []
    written[name] = [
        write(row, plan.target, plan.fields, plan.children, root) for row in rows[: plan.page]
    ]
    if len(rows) > plan.page:
        written[f"{name}@odata.nextLink"] = _link(root, entity, values, plan)


def write(values: Mapping[str, Any], entity: Any, fields, plans, root: str) -> dict:  # noqa: ANN401
    types = {p.name: p.type for p in entity.properties}
    written = {name: edm_value(values[name], types[name]) for name in fields}
    for plan in plans:
        _expanded(written, values, entity, plan, root)
    return written
