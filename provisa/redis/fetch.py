# Copyright (c) 2026 Kenneth Stott
# Canary: d8242d9f-7dd7-4801-b4d8-e6e1593cbc42
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Redis as a table source, engine-independently (REQ-1675).

The Trino connector was the only reader of a Redis source. This module is the native reader, with
the connector's own key convention: a table is a key prefix — ``<table>:*`` in the default schema
(:func:`provisa.redis.source.required_key_pattern`) — and a row is one key's hash (or string) value.
Register Table lists the prefixes present in the keyspace as tables and types a prefix's columns
from its hashes' fields; a ``mapping.tables`` entry (the type's mapping DSL, REQ-251) overrides the
pattern, the key column, the value type and the columns. Synchronous redis-py — callers run it in
a thread.
"""

from __future__ import annotations

from dataclasses import dataclass

import redis

from provisa.redis.source import DEFAULT_SCHEMA, KEY_DELIMITER, ValueType, required_key_pattern

KEY_COLUMN = "key"
_SCAN_COUNT = 500
_SAMPLE_KEYS = 5000  # keys read to discover prefixes
_SAMPLE_HASHES = 25  # hashes read to type a prefix's columns
# Provisa's own cache and control entries share the keyspace on an embedded deployment; they
# are never tables.
_OWN_PREFIXES = ("provisa",)


@dataclass(frozen=True)
class RedisConnection:  # REQ-1675
    host: str
    port: int
    password: str | None = None
    db: int = 0

    def client(self) -> "redis.Redis":
        return redis.Redis(
            host=self.host,
            port=self.port,
            password=self.password,
            db=self.db,
            decode_responses=True,
            socket_timeout=30,
        )


def _prefix_of(key: str) -> str | None:
    head, sep, _ = key.partition(KEY_DELIMITER)
    return head if sep else None


def list_prefixes(conn: RedisConnection) -> list[str]:  # REQ-1675
    """The distinct ``<prefix>:`` namespaces in the keyspace (a sample of up to 5000 keys),
    sorted — what Register Table lists as tables when the source declares no mapping."""
    seen: set[str] = set()
    read = 0
    with conn.client() as c:
        for key in c.scan_iter(match="*", count=_SCAN_COUNT):
            read += 1
            prefix = _prefix_of(key)
            if prefix and not prefix.startswith(_OWN_PREFIXES):
                seen.add(prefix)
            if read >= _SAMPLE_KEYS:
                break
    return sorted(seen)


def _table_entry(mapping: dict, table_name: str) -> dict | None:
    return next((t for t in mapping.get("tables", []) if t.get("name") == table_name), None)


def table_spec(mapping: dict, table_name: str) -> tuple[str, str, str]:  # REQ-1675
    """(key pattern, key column, value type) for a table: the mapping DSL's entry when declared,
    else the connector convention — ``<table>:*``, key column ``key``, hash values."""
    entry = _table_entry(mapping, table_name)
    if entry is None:
        return required_key_pattern(table_name, DEFAULT_SCHEMA), KEY_COLUMN, ValueType.HASH
    return (
        entry.get("key_pattern") or required_key_pattern(table_name, DEFAULT_SCHEMA),
        entry.get("key_column") or KEY_COLUMN,
        entry.get("value_type") or ValueType.HASH,
    )


def prefix_columns(
    conn: RedisConnection, mapping: dict, table_name: str
) -> list[tuple[str, str]]:  # REQ-1675
    """``(column, data_type)`` for a table: the mapping DSL's declared columns, else the key column
    plus the union of the fields of a sample of the prefix's hashes — a hash stores only strings,
    so a discovered field is ``varchar``."""
    entry = _table_entry(mapping, table_name)
    pattern, key_column, value_type = table_spec(mapping, table_name)
    if entry and entry.get("columns"):
        return [(key_column, "varchar")] + [
            (c["name"], str(c.get("data_type", "VARCHAR")).lower()) for c in entry["columns"]
        ]
    if value_type == ValueType.STRING:
        return [(key_column, "varchar"), ("value", "varchar")]
    if value_type != ValueType.HASH:
        raise ValueError(
            f"Redis table {table_name!r}: value_type {value_type!r} needs declared columns in the "
            "mapping (only hash tables are discovered)"
        )
    fields: list[str] = []
    with conn.client() as c:
        for i, key in enumerate(c.scan_iter(match=pattern, count=_SCAN_COUNT)):
            if i >= _SAMPLE_HASHES:
                break
            if c.type(key) != "hash":
                continue
            for f in c.hkeys(key):
                name = f.decode() if isinstance(f, bytes) else str(f)
                if name not in fields:
                    fields.append(name)
    return [(key_column, "varchar")] + [(f, "varchar") for f in fields]


def fetch_rows(
    conn: RedisConnection, mapping: dict, table_name: str, columns: list[str]
) -> list[dict]:  # REQ-1675
    """Every key of the table's pattern as a row of ``columns``: the key column carries the full
    key; a hash table's other columns are its fields (a mapping DSL column may name its ``field``);
    a string table's ``value`` column is the value."""
    entry = _table_entry(mapping, table_name)
    pattern, key_column, value_type = table_spec(mapping, table_name)
    field_of = {c["name"]: (c.get("field") or c["name"]) for c in (entry or {}).get("columns", [])}
    if value_type not in (ValueType.HASH, ValueType.STRING):
        raise ValueError(
            f"Redis table {table_name!r}: value_type {value_type!r} is not readable natively "
            "(hash and string are)"
        )
    rows: list[dict] = []
    with conn.client() as c:
        for key in sorted(c.scan_iter(match=pattern, count=_SCAN_COUNT)):
            if value_type == ValueType.HASH:
                if c.type(key) != "hash":
                    continue
                h = c.hgetall(key)
                row = {
                    col: (key if col == key_column else h.get(field_of.get(col, col)))
                    for col in columns
                }
            else:  # ValueType.STRING — the only other readable type (checked above)
                if c.type(key) != "string":
                    continue
                v = c.get(key)
                row = {col: (key if col == key_column else v) for col in columns}
            rows.append(row)
    return rows
