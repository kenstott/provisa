# Copyright (c) 2026 Kenneth Stott
# Canary: 78c04e5f-8b68-4b34-a630-8a27dd8ef21f
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Which columns of a remote GraphQL table its rows can be fetched by, a list of values at a
time, in one request (REQ-1865).

A table is replicated row by row (``row_materialize``) only where its source fetches the rows a
statement names by their primary key. For a remote GraphQL source that is a property of each
table, read off the remote's schema when the table is registered:

- ``where_in``: the table's own root field takes a ``where`` filter with an ``_in`` list on the
  column (Hasura: ``albums(where: {id: {_in: [1, 3]}})``);
- ``ids``: a root field takes one list of IDs and returns the table's type, or an interface or
  union the type is one of (Relay: ``nodes(ids: [...])``). The list is of the type's ``id``.

Each fetch is stored with the table's spec, keyed by the column's sql name, so a process that
starts later knows it without asking the remote again."""

# Requirements: REQ-1865

from __future__ import annotations

from provisa.compiler.naming import apply_sql_name
from provisa.graphql_remote.mapper import _find_type, _gql_type_string, _unwrap_type

# Where a plain source's row keeps its registered tables' specs: ``mapping[TABLE_SPECS_KEY]``,
# by table name (admin._graphql_table_registration.remember_table).
TABLE_SPECS_KEY = "tables"
BY_KEYS = "by_keys"
WHERE_IN = "where_in"
IDS = "ids"


def _input_fields(types: list[dict], type_ref: dict) -> list[dict]:
    kind, name = _unwrap_type(type_ref)
    if kind != "INPUT_OBJECT":
        return []
    return (_find_type(types, name) or {}).get("inputFields") or []


def _takes_a_list_in(types: list[dict], type_ref: dict) -> bool:
    """Whether the comparison input ``type_ref`` takes ``_in``, a list of values."""
    return any(f["name"] == "_in" and _is_list(f["type"]) for f in _input_fields(types, type_ref))


def _is_list(type_ref: dict) -> bool:
    while type_ref.get("kind") == "NON_NULL":
        type_ref = type_ref["ofType"]
    return type_ref.get("kind") == "LIST"


def _where_in(types: list[dict], field: dict, scalar_fields: set[str]) -> dict[str, dict]:
    where = next((a for a in field.get("args") or [] if a["name"] == "where"), None)
    if where is None:
        return {}
    return {
        apply_sql_name(f["name"]): {
            "style": WHERE_IN,
            "field": field["name"],
            "arg": "where",
            "arg_type": _gql_type_string(where["type"]),
            "key": f["name"],
        }
        for f in _input_fields(types, where["type"])
        if f["name"] in scalar_fields and _takes_a_list_in(types, f["type"])
    }


def _by_ids(types: list[dict], query_fields: list[dict], type_name: str) -> dict | None:
    for field in query_fields:
        args = field.get("args") or []
        if len(args) != 1 or not _is_list(args[0]["type"]):
            continue
        if _unwrap_type(args[0]["type"])[1] != "ID":
            continue
        kind, returned = _unwrap_type(field["type"])
        if not _is_list(field["type"]):
            continue
        if returned == type_name:
            on = None
        elif kind in ("INTERFACE", "UNION") and type_name in {
            t["name"] for t in (_find_type(types, returned) or {}).get("possibleTypes") or []
        }:
            on = type_name
        else:
            continue
        return {
            "style": IDS,
            "field": field["name"],
            "arg": args[0]["name"],
            "arg_type": _gql_type_string(args[0]["type"]),
            "on": on,
        }
    return None


def key_fetches(schema: dict, table: dict) -> dict[str, dict]:
    """Each column of ``table`` (a mapped table) its rows can be fetched by, a list of values in
    one request, with how: ``{sql column name: fetch}``. Empty when there is none, and for a
    table whose root field requires arguments of its own (a function of them, not rows)."""
    if table.get("required_args"):
        return {}
    types = schema.get("types") or []
    query = _find_type(types, (schema.get("queryType") or {}).get("name", "Query")) or {}
    query_fields = query.get("fields") or []
    returned = _find_type(types, table.get("gql_type_name") or "") or {}
    scalar_fields = {
        f["name"] for f in returned.get("fields") or [] if _unwrap_type(f["type"])[0] == "SCALAR"
    }
    own = next((f for f in query_fields if f["name"] == table["field_name"]), None)
    # A filter on the root field filters ITS rows: only where they are the table's rows -- the
    # field's own list, or a connection on the field itself, not one on an object it returns.
    rows_path = table.get("rows_path") or []
    on_root = len(rows_path) == (2 if rows_path[-2:] == ["edges", "node"] else len(rows_path[-1:]))
    fetches = _where_in(types, own, scalar_fields) if own is not None and on_root else {}
    if "id" in scalar_fields and "id" not in fetches:
        by_ids = _by_ids(types, query_fields, returned.get("name") or "")
        if by_ids is not None:
            fetches["id"] = by_ids
    return fetches


def has_key_fetch(fetches: dict[str, dict], pk_columns: list[str]) -> bool:
    """Whether a table with ``fetches`` (:func:`key_fetches`) fetches its rows by
    ``pk_columns``: a single primary-key column with a fetch of its own."""
    return len(pk_columns) == 1 and pk_columns[0] in fetches


def fetches_by_keys(mapping: dict, table_name: str, pk_columns: list[str]) -> bool:
    """:func:`has_key_fetch` for ``table_name`` as its source's ``mapping`` stores its spec. A
    table with no stored spec has none."""
    spec = (mapping.get(TABLE_SPECS_KEY) or {}).get(table_name) or {}
    return has_key_fetch(spec.get(BY_KEYS) or {}, pk_columns)
