# Copyright (c) 2026 Kenneth Stott
# Canary: 6b1d9e42-8c3a-4f27-9a05-2d7e1b4c8f36
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Materialization LAND executor for a relational store (REQ-844, REQ-848, REQ-932).

Non-attachable sources (openapi/API, graphql_remote) cannot be referenced in place, so a
single-node engine LANDs them into a relational store it can then attach and federate. All three
landing shapes execute through the ``Connection`` abstraction (``provisa/core/database.py``) with
vanilla SQLAlchemy Core — no dialect-specific SQL here. Whatever relational backend the store is
(the reachable superset ``_RELATIONAL``: postgresql/mysql/mariadb/sqlite/duckdb/sqlserver/
singlestore), the same code path runs; the one dialect decision (upsert = UPDATE-then-INSERT)
lives inside ``Connection.upsert``.

Three landing shapes, selected by the table's change_signal + watermark_column (REQ-932,
``select_landing_shape``):
- REPLACE — ``land_replace`` — drop+create+insert; a full refresh (poll signal, no watermark).
- APPEND  — ``land_append``  — insert a watermark-filtered delta into the existing table.
- CDC     — ``apply_cdc``    — upsert (insert/update) and tombstone (delete) by PK, the only shape
  that carries hard deletes; fed by a Debezium/Kafka/native provider stream.
"""

from __future__ import annotations

import json
from typing import Any, Protocol

from sqlalchemy import JSON, Column, MetaData, Table
from sqlalchemy.schema import CreateTable

from provisa.core.ir_types import to_sqlalchemy


class StoreConn(Protocol):
    """The materialization-store write face these ops need — the structural subset of
    ``provisa.core.database.Connection`` (which satisfies it): run a Core statement, upsert a row
    dialect-agnostically, and expose per-dialect capabilities. Structural so any conforming
    connection (or a test fake) works without importing the concrete class."""

    capabilities: Any

    async def execute_core(self, stmt: Any) -> Any: ...

    async def upsert(self, table: Table, values: dict, *, index_elements: list[str]) -> None: ...


# The write face's IR → SQLAlchemy mapping is the canonical ir_types registry (REQ-846): one
# engine-independent vocabulary, so the fed engine is never the type authority. ``_sa_type`` is the
# store-DDL side of that hub; it raises on an unknown type (never a silent varchar widen).
_sa_type = to_sqlalchemy


def build_table(
    schema: str,
    table: str,
    columns: list[tuple[str, str]],
    pk_columns: tuple[str, ...] | list[str] = (),
) -> Table:
    """A Core ``Table`` for the landed relation on a fresh ``MetaData``. ``columns`` are
    (name, sql_type) pairs — the projected source result shape. ``pk_columns`` names the primary
    key (required for the CDC shape; empty for replace/append)."""
    pk = set(pk_columns)
    cols = [Column(name, _sa_type(sql_type), primary_key=name in pk) for name, sql_type in columns]
    return Table(table, MetaData(), *cols, schema=schema or None)


def _json_columns(table: Table) -> frozenset[str]:
    """Names of the table's JSON-typed columns (REQ-980)."""
    return frozenset(c.name for c in table.columns if isinstance(c.type, JSON))


def _coerce_json_row(row: dict, json_cols: frozenset[str]) -> dict:
    """Parse JSON-column string values into Python objects before insert (REQ-980).

    The fed engine returns a JSON column as serialized TEXT; a SQLAlchemy JSON column re-serializes on
    bind, so a string value would be double-encoded (a JSON string of a JSON string). Parse it here so
    the store holds the object. A malformed JSON string is a real upstream error — it raises loud (no
    silent passthrough). ``None`` and already-parsed dict/list values pass through untouched."""
    if not json_cols:
        return row
    out = dict(row)
    for name in json_cols:
        val = out.get(name)
        if isinstance(val, str):
            out[name] = json.loads(val)  # loud on malformed JSON — upstream contract violation
    return out


async def land_replace(conn: StoreConn, table: Table, rows: list[dict]) -> str:
    """REPLACE land: **DELETE + INSERT** — a full refresh of the table's CONTENTS (REQ-932).

    Never DROPs the table: its existence/schema/grants are the reconcile controller's concern (a
    pre-created, restart-surviving, reconcile-managed table). Creates it only if absent (first land
    before reconcile ran), then deletes all rows and re-inserts — so ``replace`` replaces contents,
    not the table."""
    await conn.execute_core(CreateTable(table, if_not_exists=True))
    await conn.execute_core(table.delete())
    json_cols = _json_columns(table)
    for row in rows:
        await conn.execute_core(table.insert().values(**_coerce_json_row(row, json_cols)))
    return _qualified(table)


async def land_append(conn: StoreConn, table: Table, rows: list[dict]) -> str:
    """APPEND land: insert ``rows`` into an existing table without dropping it (REQ-932).

    ``rows`` are the already-watermark-filtered delta (``WHERE watermark > cursor`` upstream), so
    this only creates-if-absent and inserts — no truncation. The caller advances the cursor."""
    await conn.execute_core(CreateTable(table, if_not_exists=True))
    json_cols = _json_columns(table)
    for row in rows:
        await conn.execute_core(table.insert().values(**_coerce_json_row(row, json_cols)))
    return _qualified(table)


async def apply_cdc(
    conn: StoreConn,
    table: Table,
    pk_columns: list[str],
    events: list,
) -> dict[str, int]:
    """CDC land: apply change events to a landed table by primary key (REQ-932).

    insert/update → upsert (UPDATE-by-PK, else INSERT — dialect-agnostic via ``Connection.upsert``);
    delete → tombstone (``DELETE … WHERE pk``). ``events`` are ChangeEvent-like objects with
    ``.operation`` (insert|update|delete) and ``.row``. A primary key is REQUIRED — without it
    there is no identity to upsert or delete by."""
    if not pk_columns:
        raise ValueError(
            f"CDC land into {_qualified(table)} requires primary key columns for upsert/delete"
        )
    await conn.execute_core(CreateTable(table, if_not_exists=True))

    json_cols = _json_columns(table)
    counts = {"upsert": 0, "delete": 0}
    for ev in events:
        if ev.operation.lower() == "delete":
            where = _pk_where(table, pk_columns, ev.row)
            await conn.execute_core(table.delete().where(where))
            counts["delete"] += 1
        else:  # insert / update → upsert
            await conn.upsert(
                table, _coerce_json_row(dict(ev.row), json_cols), index_elements=pk_columns
            )
            counts["upsert"] += 1
    return counts


def _pk_where(table: Table, pk_columns: list[str], row: dict) -> Any:
    from sqlalchemy import and_

    return and_(*[table.c[pk] == row.get(pk) for pk in pk_columns])


def _qualified(table: Table) -> str:
    return f"{table.schema}.{table.name}" if table.schema else table.name
