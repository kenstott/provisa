# Copyright (c) 2026 Kenneth Stott
# Canary: 3fa9c1e6-7d42-4c08-b5e3-91d6a2f4c7b0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's json and jsonb functions DuckDB has no equivalent of, as UDFs of its text
(REQ-2167, REQ-2171), each following src/backend/utils/adt/jsonfuncs.c, jsonb_op.c or jsonb_util.c.
provisa.transpiler.pg_forms_json writes the calls of them. A jsonb here is its parsed value: None,
bool, Decimal, str, list and a dict in jsonb's key order; a json keeps its keys, duplicates
included, and its numbers' digits."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import json
import re
from collections.abc import Callable, Iterator
from decimal import Decimal
from typing import Any

from provisa.federation import duckdb_pg_jsonpath as jsonpath
from provisa.federation.duckdb_pg_scalars import _error
from provisa.federation.pg_hash import (
    hash_bytes,
    hash_bytes_extended,
    hash_numeric,
    hash_numeric_extended,
    hash_uint32_extended,
)
from provisa.transpiler.pg_forms_json import jsonb_written, stored_keys


class _Number(str):
    """A json number, its digits as written."""


class _Object(list):
    """A json object: its members as written, duplicates included."""


def _refuse_constant(name: str) -> None:
    raise _error("22P02", f"invalid input syntax for type json: {name}")


def _jsonb(text: str) -> Any:
    def stored(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        members = dict(pairs)
        return {key: members[key] for key in stored_keys(members)}

    try:
        return json.loads(
            text,
            parse_float=Decimal,
            parse_int=Decimal,
            object_pairs_hook=stored,
            parse_constant=_refuse_constant,
        )
    except json.JSONDecodeError as refused:
        raise _error("22P02", "invalid input syntax for type json") from refused


def _json(text: str) -> Any:
    try:
        return json.loads(
            text,
            parse_float=_Number,
            parse_int=_Number,
            object_pairs_hook=_Object,
            parse_constant=_refuse_constant,
        )
    except json.JSONDecodeError as refused:
        raise _error("22P02", "invalid input syntax for type json") from refused


def _json_written(value: Any) -> str:
    if isinstance(value, _Object):
        return (
            "{"
            + ", ".join(
                f"{json.dumps(k, ensure_ascii=False)}: {_json_written(v)}" for k, v in value
            )
            + "}"
        )
    if isinstance(value, list):
        return "[" + ", ".join(_json_written(v) for v in value) + "]"
    return str(value) if isinstance(value, _Number) else json.dumps(value, ensure_ascii=False)


def _is_binary(called: str) -> bool:
    return called.startswith("jsonb")


def _parsed(text: str, called: str) -> Any:
    return _jsonb(text) if _is_binary(called) else _json(text)


def _written(value: Any, called: str) -> str:
    return jsonb_written(value) if _is_binary(called) else _json_written(value)


def _kind_name(value: Any) -> str:
    if isinstance(value, (dict, _Object)):
        return "an object"
    return "an array" if isinstance(value, list) else "a scalar"


def _cell(value: Any, called: str) -> str | None:
    """A value as text: a string's own characters, NULL for a json null."""
    if value is None or isinstance(value, str) and not isinstance(value, _Number):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    return _written(value, called)


# --- set-returning -------------------------------------------------------------------------------


def _array_of(value: Any, called: str) -> list[Any]:
    if isinstance(value, _Object) or not isinstance(value, list):
        if _is_binary(called):
            shape = "an object" if isinstance(value, dict) else "a scalar"
            raise _error("22023", f"cannot extract elements from {shape}")
        scalar = not isinstance(value, _Object)
        raise _error("22023", f"cannot call {called} on a {'scalar' if scalar else 'non-array'}")
    return value


def elements(text: str, called: str) -> list[str | None]:
    """json_array_elements and its _text."""
    found = _array_of(_parsed(text, called), called)
    if called.endswith("_text"):
        return [_cell(value, called) for value in found]
    return [_written(value, called) for value in found]


def _members(value: Any, called: str) -> list[tuple[str, Any]]:
    if isinstance(value, dict):
        return list(value.items())
    if isinstance(value, _Object):
        return list(value)
    if _is_binary(called):
        raise _error("22023", f"cannot call {called} on a non-object")
    raise _error("22023", "cannot deconstruct an array as an object")


def each(text: str, called: str) -> list[dict[str, str | None]]:
    """json_each and its _text, as rows of a key and a value."""
    members = _members(_parsed(text, called), called)
    write = _cell if called.endswith("_text") else _written
    return [{"key": key, "value": write(value, called)} for key, value in members]


def object_keys(text: str, called: str) -> list[str]:
    value = _parsed(text, called)
    if not isinstance(value, (dict, _Object)):
        raise _error("22023", f"cannot call {called} on {_kind_name(value)}")
    return [key for key, _ in _members(value, called)]


_INTEGERS = frozenset({"smallint", "int", "integer", "bigint", "int2", "int4", "int8"})
_WHOLE = re.compile(r"\s*[-+]?\d+\s*")


def _field(value: Any, kind: str, called: str) -> str | None:
    """A member as the text of a column of ``kind``, which the statement's CAST then reads."""
    if value is None:
        return None
    if kind in ("json", "jsonb") or kind.endswith("[]") and not isinstance(value, str):
        return _written(value, called)
    cell = _cell(value, called)
    if kind in _INTEGERS and not _WHOLE.fullmatch(cell or ""):  # the engine's cast would round it
        raise _error("22P02", f'invalid input syntax for type {kind}: "{cell}"')
    return cell


def records(
    text: str | None, names: list[str], kinds: list[str], called: str
) -> list[list[str | None]]:
    """json_to_record and json_to_recordset, each row its columns as text."""
    many = called.endswith("recordset")
    value = None if text is None else _parsed(text, called)
    if text is None or value is None:
        return [] if many else [[None] * len(names)]
    if many and (isinstance(value, _Object) or not isinstance(value, list)):
        raise _error("22023", f"cannot call {called} on a non-array")
    if not many and not isinstance(value, (dict, _Object)):
        raise _error("22023", f"cannot call {called} on {_kind_name(value)}")
    rows = value if many else [value]
    if not all(isinstance(row, (dict, _Object)) for row in rows):
        raise _error("22023", f"argument of {called} must be an array of objects")
    written: list[list[str | None]] = []
    for row in rows:
        members = dict(row)
        written.append([_field(members.get(n), k, called) for n, k in zip(names, kinds)])
    return written


# --- reading -------------------------------------------------------------------------------------


def extract_path(text: str, steps: list[str | None], as_text: bool, called: str) -> str | None:
    """json_extract_path and its _text, a path given as an array."""
    value = _parsed(text, called)
    for step in steps:
        if step is None:
            return None
        if isinstance(value, (dict, _Object)):
            members = dict(value.items() if isinstance(value, dict) else value)
            if step not in members:
                return None
            value = members[step]
        elif isinstance(value, list):
            index = int(step) if re.fullmatch(r"-?\d+", step) else None
            if index is None or not -len(value) <= index < len(value):
                return None
            value = value[index]
        else:
            return None
    return _cell(value, called) if as_text else _written(value, called)


# --- what a jsonb holds --------------------------------------------------------------------------


def _same_scalar(left: Any, right: Any) -> bool:
    return type(left) is type(right) and left == right


def _contained(held: Any, wanted: Any) -> bool:
    """JsonbDeepContains."""
    if isinstance(wanted, dict):
        if not isinstance(held, dict):
            return False
        for key, value in wanted.items():
            if key not in held:
                return False
            if isinstance(value, (dict, list)):
                if not _contained(held[key], value) or type(held[key]) is not type(value):
                    return False
            elif not _same_scalar(held[key], value):
                return False
        return True
    if isinstance(wanted, list):
        if not isinstance(held, list):
            return False
        return all(_element_contained(held, value) for value in wanted)
    return _element_contained(held if isinstance(held, list) else [held], wanted)


def _element_contained(held: list[Any], wanted: Any) -> bool:
    if isinstance(wanted, (dict, list)):
        kind = type(wanted)
        return any(type(value) is kind and _contained(value, wanted) for value in held)
    return any(_same_scalar(value, wanted) for value in held)


def contains(left: str, right: str) -> bool:
    """jsonb_contains."""
    held, wanted = _jsonb(left), _jsonb(right)
    return isinstance(held, dict) == isinstance(wanted, dict) and _contained(held, wanted)


def contained(left: str, right: str) -> bool:
    """jsonb_contained."""
    return contains(right, left)


def _holds_key(value: Any, key: str) -> bool:
    if isinstance(value, dict):
        return key in value
    return (
        any(element == key for element in value if isinstance(element, str))
        if isinstance(value, list)
        else value == key
    )


def exists(text: str, key: str) -> bool:
    return _holds_key(_jsonb(text), key)


def exists_any(text: str, keys: list[str | None]) -> bool:
    value = _jsonb(text)
    return any(key is not None and _holds_key(value, key) for key in keys)


def exists_all(text: str, keys: list[str | None]) -> bool:
    value = _jsonb(text)
    return all(key is not None and _holds_key(value, key) for key in keys)


def _count(value: Any) -> int:
    return len(value) if isinstance(value, (dict, list)) else 1


def _is_scalar(value: Any) -> bool:
    return not isinstance(value, (dict, list))


# --- order ---------------------------------------------------------------------------------------

#: JsonbValue's enum order: a value of a later kind sorts after one of an earlier.
_RANK = {"null": 0, "string": 1, "number": 2, "boolean": 3, "array": 4, "object": 5}


def _rank(value: Any) -> int:
    return _RANK[jsonpath._type(value)]


def _tokens(value: Any, top: bool = False) -> Iterator[tuple[str, int, Any]]:
    """The walk of JsonbIteratorNext: a raw scalar is a one-element array that says it is one."""
    if isinstance(value, dict):
        yield ("begin", _RANK["object"], len(value))
        for key, member in value.items():
            yield ("key", _RANK["string"], key)
            yield from _tokens(member)
        yield ("end", _RANK["object"], None)
    elif isinstance(value, list):
        yield ("begin", _RANK["array"], (False, len(value)))
        for element in value:
            yield from _tokens(element)
        yield ("end", _RANK["array"], None)
    elif top:
        yield ("begin", _RANK["array"], (True, 1))
        yield ("scalar", _rank(value), value)
        yield ("end", _RANK["array"], None)
    else:
        yield ("scalar", _rank(value), value)


def _scalar_order(left: Any, right: Any) -> int:
    if left is None:
        return 0
    if isinstance(left, str):  # jsonb_util compares through the default collation; this is C's
        left, right = left.encode(), right.encode()
    return (left > right) - (left < right)


def _container_order(left: Any, right: Any) -> int:
    if isinstance(left, tuple):  # an array: a raw scalar first, unless the counts say otherwise
        order = 0 if left[0] == right[0] else (-1 if left[0] else 1)
        return order if left[1] == right[1] else (1 if left[1] > right[1] else -1)
    return 0 if left == right else (1 if left > right else -1)


def cmp(left: str, right: str) -> int:
    """compareJsonbContainers."""
    for ours, theirs in zip(_tokens(_jsonb(left), True), _tokens(_jsonb(right), True)):
        if ours[1] != theirs[1]:
            return 1 if ours[1] > theirs[1] else -1
        if ours[0] != theirs[0]:
            return 1 if ours[0] == "scalar" else -1
        if ours[0] == "begin":
            order = _container_order(ours[2], theirs[2])
        elif ours[0] == "end":
            order = 0
        else:
            order = _scalar_order(ours[2], theirs[2])
        if order:
            return order
    return 0


# --- hash ----------------------------------------------------------------------------------------

_FOBJECT, _FARRAY = 0x20000000, 0x40000000
_U32, _U64 = 2**32 - 1, 2**64 - 1


def _hashed(value: Any) -> Iterator[Any]:
    """What jsonb_hash takes of a value, in JsonbIteratorNext's order: a flag, or a scalar in a
    one-element tuple."""
    if isinstance(value, dict):
        yield _FOBJECT
        for key, member in value.items():
            yield (key,)
            yield from _hashed(member)
    elif isinstance(value, list):
        yield _FARRAY
        for element in value:
            yield from _hashed(element)
    else:
        yield (value,)


def _scalar_hash(value: Any) -> int:
    if value is None:
        return 0x01
    if isinstance(value, bool):
        return 0x02 if value else 0x04
    return hash_numeric(value) if isinstance(value, Decimal) else hash_bytes(value.encode())


def jsonb_hash(text: str) -> int:
    value = _jsonb(text)
    if _count(value) == 0:
        return 0
    hashed = 0
    for part in _hashed([value] if _is_scalar(value) else value):
        if isinstance(part, int):
            hashed ^= part
        else:
            hashed = ((hashed << 1) | (hashed >> 31)) & _U32
            hashed ^= _scalar_hash(part[0])
    return hashed - 2**32 if hashed >= 2**31 else hashed


def _scalar_hash_extended(value: Any, seed: int) -> int:
    if value is None:
        return (seed + 1) & _U64
    if isinstance(value, bool):
        return hash_uint32_extended(int(value), seed) if seed else (0x02 if value else 0x04)
    if isinstance(value, Decimal):
        return hash_numeric_extended(value, seed)
    return hash_bytes_extended(value.encode(), seed)


def jsonb_hash_extended(text: str, seed: int) -> int:
    value = _jsonb(text)
    unsigned_seed = seed & _U64
    if _count(value) == 0:
        return seed
    hashed = 0
    for part in _hashed([value] if _is_scalar(value) else value):
        if isinstance(part, int):
            hashed ^= (part << 32) | part
        else:
            hashed = ((hashed << 1) & 0xFFFFFFFEFFFFFFFE) | ((hashed >> 31) & 0x100000001)
            hashed ^= _scalar_hash_extended(part[0], unsigned_seed)
    return hashed - 2**64 if hashed >= 2**63 else hashed


# --- what changes one ----------------------------------------------------------------------------


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else [value]


def concat(left: str, right: str) -> str:
    """jsonb_concat: the IteratorConcat of two values."""
    one, other = _jsonb(left), _jsonb(right)
    if _count(one) == 0 and not _is_scalar(other):
        return right
    if _count(other) == 0 and not _is_scalar(one):
        return left
    if isinstance(one, dict) and isinstance(other, dict):
        merged = {**one, **other}
        return jsonb_written({key: merged[key] for key in stored_keys(merged)})
    return jsonb_written([*_as_list(one), *_as_list(other)])


def delete(text: str, key: str) -> str:
    """jsonb_delete of a text, of an integer, of a text[]: ``key`` is the argument's to_json."""
    value, target = _jsonb(text), _jsonb(key)
    if _is_scalar(value):
        raise _error("22023", "cannot delete from scalar")
    if isinstance(target, Decimal):
        return _without_index(text, value, int(target))
    keys = target if isinstance(target, list) else [target]
    if isinstance(value, dict):
        return jsonb_written({k: v for k, v in value.items() if k not in keys})
    return jsonb_written([v for v in value if not (isinstance(v, str) and v in keys)])


def _without_index(text: str, value: Any, index: int) -> str:
    if isinstance(value, dict):
        raise _error("22023", "cannot delete from object using integer index")
    position = index + len(value) if index < 0 else index
    if not 0 <= position < len(value):
        return text
    return jsonb_written(value[:position] + value[position + 1 :])


_CREATE, _REPLACE, _DELETE, _BEFORE, _AFTER = "create", "replace", "delete", "before", "after"
_CREATING = (_CREATE, _BEFORE, _AFTER)


def _set_in_object(
    value: dict[str, Any], path: list[str | None], level: int, new: Any, how: str
) -> dict[str, Any]:
    """setPathObject."""
    key = path[level] if level < len(path) else None
    if key is None:
        return value
    last = level == len(path) - 1
    if key not in value:
        return {**value, key: new} if how in _CREATING and last else value
    if not last:
        return {**value, key: _set(value[key], path, level + 1, new, how)}
    if how in (_BEFORE, _AFTER):
        raise _error("22023", "cannot replace existing key")
    return {k: v for k, v in value.items() if k != key} if how == _DELETE else {**value, key: new}


def _position(step: str | None, count: int, level: int) -> int | None:
    """Where an array is reached: None before its first element."""
    if step is None:
        return count
    if not re.fullmatch(r"\s*[-+]?\d+\s*", step):
        raise _error("22P02", f'path element at position {level + 1} is not an integer: "{step}"')
    index = int(step)
    if index < 0:
        return None if -index > count else count + index
    return min(index, count)


def _set_in_array(
    value: list[Any], path: list[str | None], level: int, new: Any, how: str
) -> list[Any]:
    """setPathArray."""
    last = level == len(path) - 1
    index = _position(path[level], len(value), level) if level < len(path) else len(value)
    if (index is None or not value) and last and how in _CREATING:
        return [new, *value]
    if index is None or index >= len(value):
        return [*value, new] if last and how in _CREATING else value
    if not last:
        return [*value[:index], _set(value[index], path, level + 1, new, how), *value[index + 1 :]]
    middle = {
        _CREATE: [new],
        _REPLACE: [new],
        _DELETE: [],
        _BEFORE: [new, value[index]],
        _AFTER: [value[index], new],
    }[how]
    return [*value[:index], *middle, *value[index + 1 :]]


def _set(value: Any, path: list[str | None], level: int, new: Any, how: str) -> Any:
    if isinstance(value, dict):
        return _set_in_object(value, path, level, new, how)
    return _set_in_array(value, path, level, new, how) if isinstance(value, list) else value


def _stored(value: Any) -> Any:
    """``value`` with each object's keys in jsonb's order, as a result is written."""
    if isinstance(value, dict):
        return {key: _stored(value[key]) for key in stored_keys(value)}
    return [_stored(v) for v in value] if isinstance(value, list) else value


def _changed(text: str, path: list[str | None], new: Any, how: str, in_scalar: str) -> str:
    value = _jsonb(text)
    if _is_scalar(value):
        raise _error("22023", in_scalar)
    if not path or _count(value) == 0 and how == _DELETE:
        return text
    return jsonb_written(_stored(_set(value, path, 0, new, how)))


def delete_path(text: str, path: list[str | None]) -> str:
    return _changed(text, path, None, _DELETE, "cannot delete path in scalar")


def set_path(text: str, path: list[str | None], new: str, create: bool) -> str:
    """jsonb_set."""
    how = _CREATE if create else _REPLACE
    return _changed(text, path, _jsonb(new), how, "cannot set path in scalar")


def insert(text: str, path: list[str | None], new: str, after: bool) -> str:
    """jsonb_insert."""
    how = _AFTER if after else _BEFORE
    return _changed(text, path, _jsonb(new), how, "cannot set path in scalar")


_TREATMENTS = (
    'null_value_treatment must be "delete_key", "return_target", "use_json_null", or '
    '"raise_exception"'
)


def set_lax(
    text: str | None,
    path: list[str | None] | None,
    new: str | None,
    create: bool | None,
    treatment: str | None,
) -> str | None:
    """jsonb_set_lax: not strict in ``new``, which its null_value_treatment answers for."""
    if text is None or path is None or create is None:
        return None
    if treatment is None:
        raise _error("22004", _TREATMENTS)
    if new is not None:
        return set_path(text, path, new, create)
    if treatment == "raise_exception":
        raise _error("22004", "JSON value must not be null")
    if treatment == "delete_key":
        return delete_path(text, path)
    if treatment == "return_target":
        return text
    if treatment == "use_json_null":
        return set_path(text, path, "null", create)
    raise _error(
        "22023",
        'null_value_treatment must be "delete_key", "return_target", '
        '"use_json_null", or "raise_exception"',
    )


def _no_nulls(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _no_nulls(v) for k, v in value.items() if v is not None}
    if isinstance(value, _Object):
        return _Object((k, _no_nulls(v)) for k, v in value if v is not None)
    return [_no_nulls(v) for v in value] if isinstance(value, list) else value


def strip_nulls(text: str, called: str) -> str:
    return _written(_no_nulls(_parsed(text, called)), called)


def _pretty_lines(value: Any, level: int) -> str:
    pad, inner = "    " * level, "    " * (level + 1)
    if isinstance(value, dict):
        members = (
            f"\n{inner}{json.dumps(k, ensure_ascii=False)}: {_pretty_lines(v, level + 1)}"
            for k, v in value.items()
        )
        return "{" + ",".join(members) + f"\n{pad}}}"
    if isinstance(value, list):
        lines = (f"\n{inner}{_pretty_lines(v, level + 1)}" for v in value)
        return "[" + ",".join(lines) + f"\n{pad}]"
    return jsonb_written(value)


def pretty(text: str) -> str:
    """jsonb_pretty, as JsonbToCStringIndent writes one."""
    return _pretty_lines(_jsonb(text), 0)


def object_agg(pairs: list[dict[str, Any]], strict: bool, unique: bool, called: str) -> str:
    """The _strict and _unique object aggregates: ``pairs`` the group's keys and to_json values."""
    binary = _is_binary(called)
    members: dict[str, Any] = {}
    for pair in pairs:
        key, value = pair["k"], pair["v"]
        if key is None:
            if binary:
                raise _error("22023", "field name must not be null")
            raise _error("22004", "null value not allowed for object key")
        if strict and value is None:
            continue
        if unique and key in members:
            shown = "" if binary else f": {json.dumps(key, ensure_ascii=False)}"
            raise _error("22030", f"duplicate JSON object key value{shown}")
        members[key] = None if value is None else _jsonb(value)
    return jsonb_written({key: members[key] for key in stored_keys(members)})


# --- jsonpath ------------------------------------------------------------------------------------


def path_query(text: str, path: str, variables: str, silent: bool) -> list[str]:
    found = jsonpath.items(path, _jsonb(text), _jsonb(variables), silent)
    return [jsonb_written(_stored(item)) for item in found]


def path_query_array(text: str, path: str, variables: str, silent: bool) -> str:
    return jsonb_written(_stored(jsonpath.items(path, _jsonb(text), _jsonb(variables), silent)))


def path_query_first(text: str, path: str, variables: str, silent: bool) -> str | None:
    found = jsonpath.items(path, _jsonb(text), _jsonb(variables), silent)
    return jsonb_written(_stored(found[0])) if found else None


def path_exists(text: str, path: str, variables: str, silent: bool) -> bool | None:
    return jsonpath.exists(path, _jsonb(text), _jsonb(variables), silent)


def path_match(text: str, path: str, variables: str, silent: bool) -> bool | None:
    return jsonpath.match(path, _jsonb(text), _jsonb(variables), silent)


_V, _L, _B = "VARCHAR", "VARCHAR[]", "BOOLEAN"
_MEMBERS = "STRUCT(key VARCHAR, value VARCHAR)[]"
_PATH = (_V, _V, _V, _B)
_UDFS: tuple[tuple[str, Callable[..., Any], list[str], str], ...] = (
    ("provisa_json_elements", elements, [_V, _V], _L),
    ("provisa_json_each", each, [_V, _V], _MEMBERS),
    ("provisa_json_object_keys", object_keys, [_V, _V], _L),
    ("provisa_jsonb_contains", contains, [_V, _V], _B),
    ("provisa_jsonb_contained", contained, [_V, _V], _B),
    ("provisa_jsonb_exists", exists, [_V, _V], _B),
    ("provisa_jsonb_exists_any", exists_any, [_V, _L], _B),
    ("provisa_jsonb_exists_all", exists_all, [_V, _L], _B),
    ("provisa_jsonb_cmp", cmp, [_V, _V], "INTEGER"),
    ("provisa_jsonb_hash", jsonb_hash, [_V], "INTEGER"),
    ("provisa_jsonb_hash_extended", jsonb_hash_extended, [_V, "BIGINT"], "BIGINT"),
    ("provisa_jsonb_concat", concat, [_V, _V], _V),
    ("provisa_jsonb_delete", delete, [_V, _V], _V),
    ("provisa_jsonb_delete_path", delete_path, [_V, _L], _V),
    ("provisa_jsonb_set", set_path, [_V, _L, _V, _B], _V),
    ("provisa_jsonb_insert", insert, [_V, _L, _V, _B], _V),
    ("provisa_json_strip_nulls", strip_nulls, [_V, _V], _V),
    ("provisa_jsonb_pretty", pretty, [_V], _V),
    ("provisa_json_object_agg", object_agg, ["STRUCT(k VARCHAR, v VARCHAR)[]", _B, _B, _V], _V),
    ("provisa_jsonb_path_query", path_query, [*_PATH], _L),
    ("provisa_jsonb_path_query_array", path_query_array, [*_PATH], _V),
)
#: What may answer NULL of arguments that are not: DuckDB's default takes a NULL out for a call.
_MAY_BE_NULL: tuple[tuple[str, Callable[..., Any], list[str], str], ...] = (
    ("provisa_json_extract_path", extract_path, [_V, _L, _B, _V], _V),
    ("provisa_jsonb_path_query_first", path_query_first, [*_PATH], _V),
    ("provisa_jsonb_path_exists", path_exists, [*_PATH], _B),
    ("provisa_jsonb_path_match", path_match, [*_PATH], _B),
)


def _strict(function: Callable[..., Any]) -> Callable[..., Any]:
    def call(*args: Any) -> Any:
        return None if any(a is None for a in args) else function(*args)

    call.__wrapped__ = function  # type: ignore[attr-defined]  # DuckDB counts its parameters
    return call


def register(con: Any) -> None:
    """Create the UDFs on DuckDB connection ``con``."""
    for name, function, parameters, returns in _UDFS:
        con.create_function(name, function, parameters, returns, side_effects=False)
    for name, function, parameters, returns in _MAY_BE_NULL:
        con.create_function(
            name,
            _strict(function),
            parameters,
            returns,
            null_handling="special",
            side_effects=False,
        )
    con.create_function(
        "provisa_jsonb_set_lax",
        set_lax,
        [_V, _L, _V, _B, _V],
        _V,
        null_handling="special",
        side_effects=False,
    )
    con.create_function(
        "provisa_json_records",
        lambda text, names, kinds, called: records(text, names, kinds, called),
        [_V, _L, _L, _V],
        "VARCHAR[][]",
        null_handling="special",
        side_effects=False,
    )
