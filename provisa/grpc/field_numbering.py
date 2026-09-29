# Copyright (c) 2026 Kenneth Stott
# Canary: 4ee82ae6-c2cb-42a7-af73-37078c999906
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Stable proto field-number allocation for the gRPC wire schema (REQ-1903).

``proto_gen.generate_proto`` used to assign field numbers via ``enumerate(sorted_cols, start=1)``
freshly on every schema regeneration. A client holding an older-generation stub decodes a later
generation's bytes with the OLD field-number assignment: if a column was added, renamed, or the
sort order otherwise shifted, the client silently reads the wrong field into the wrong slot — no
error, corrupted data. Protobuf's answer is ``reserved``: a name/number pair, once retired, is
never reused for a different meaning. This module is the dynamic-generation equivalent, backed by
a persisted table instead of a static ``.proto`` file's ``reserved`` block.

Numbers are assigned once per ``(table_id, namespace, field_name)`` and never reassigned. Distinct
namespaces (the {Type} row message vs. {Type}Filter vs. {Type}Input) get independent field-number
spaces, since each is a separate protobuf message with its own numbering — reusing the {Type}
message's numbers for {Type}Filter would just be coincidence, not a requirement, and would block a
Filter-only field from getting number 1 when the row message doesn't start there.

Persisted per org (the same ``org_<id>`` schema ``query_audit_log`` lives in — see
``provisa.audit.query_log.init_audit_schema``), since ``table_id`` is only unique within one org's
registered_tables.
"""

from __future__ import annotations

FIELD_NUMBERS_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS grpc_field_numbers (
    table_id TEXT NOT NULL,
    namespace TEXT NOT NULL,
    field_name TEXT NOT NULL,
    field_number INTEGER NOT NULL,
    removed BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (table_id, namespace, field_name)
);
"""


class FieldNumberAllocator:
    """In-memory view of ``grpc_field_numbers``, plus the allocation rules.

    Loaded once per schema build (``load_field_number_allocator``), consulted by every
    ``generate_proto`` call in that build (role-scoped and the wire/union schema alike — they must
    agree on numbers for the same column, since the server's wire bytes are decoded against a
    role's own downloaded ``.proto``), then flushed back (``persist_field_numbers``) once the build
    has seen the FULL (union) column set for every table.
    """

    def __init__(self, rows: list[tuple[str, str, str, int, bool]]) -> None:
        self._all_numbers: dict[tuple[str, str], dict[str, int]] = {}
        self._active: dict[tuple[str, str], dict[str, int]] = {}
        self._removed: dict[tuple[str, str], set[str]] = {}
        for table_id, namespace, field_name, field_number, removed in rows:
            key = (table_id, namespace)
            self._all_numbers.setdefault(key, {})[field_name] = field_number
            if removed:
                self._removed.setdefault(key, set()).add(field_name)
            else:
                self._active.setdefault(key, {})[field_name] = field_number
        self._pending: dict[tuple[str, str, str], tuple[int, bool]] = {}

    def numbers_for(self, table_id: str, namespace: str, field_names: list[str]) -> dict[str, int]:
        """Field numbers for ``field_names`` in this (table_id, namespace) — existing names keep
        their persisted number, new names get ``max(every number ever used here) + 1`` (never a
        retired one, since retired numbers stay in ``_all_numbers``)."""
        key = (str(table_id), namespace)
        all_nums = self._all_numbers.setdefault(key, {})
        active = self._active.setdefault(key, {})
        removed = self._removed.setdefault(key, set())
        out: dict[str, int] = {}
        for name in field_names:
            if name in all_nums:
                num = all_nums[name]
                if name in removed:
                    removed.discard(name)
                    self._pending[(key[0], key[1], name)] = (num, False)
                active[name] = num
                out[name] = num
                continue
            num = max(all_nums.values(), default=0) + 1
            all_nums[name] = num
            active[name] = num
            out[name] = num
            self._pending[(key[0], key[1], name)] = (num, False)
        return out

    def reconcile_removed(
        self, table_id: str, namespace: str, authoritative_field_names: set[str]
    ) -> None:
        """Mark any previously-active field absent from the AUTHORITATIVE (union/wire) field set as
        removed, so its number is never handed to a differently-named field later. Only call this
        with a table's full column set (the wire schema's), never a role-filtered subset — a role
        simply not seeing a column is not the same as the column being dropped from the table."""
        key = (str(table_id), namespace)
        active = self._active.setdefault(key, {})
        removed = self._removed.setdefault(key, set())
        for name in list(active):
            if name not in authoritative_field_names:
                del active[name]
                removed.add(name)
                num = self._all_numbers[key][name]
                self._pending[(key[0], key[1], name)] = (num, True)

    def pending_rows(self) -> list[tuple[str, str, str, int, bool]]:
        return [
            (table_id, namespace, name, num, rm)
            for (table_id, namespace, name), (num, rm) in self._pending.items()
        ]


async def load_field_number_allocator(conn) -> FieldNumberAllocator:
    """Load the persisted allocation state for the current org's tenant DB, creating the table on
    first use (same ``CREATE TABLE IF NOT EXISTS`` convention as ``init_audit_schema`` — V1, no
    migrations).

    Postgres-only (``ON CONFLICT`` upsert syntax): same restriction ``init_audit_schema`` documents
    for the append-only audit rules — a non-Postgres tenant DB gets an allocator with nothing
    persisted, so every ``generate_proto`` call in this build falls back to fresh enumeration (the
    pre-REQ-1903 behavior for that backend, not a partial/incorrect persisted state)."""
    if (
        getattr(conn, "capabilities", None) is not None
        and conn.capabilities.dialect != "postgresql"
    ):
        return FieldNumberAllocator([])
    await conn.execute(FIELD_NUMBERS_SCHEMA_SQL)
    rows = await conn.fetch(
        "SELECT table_id, namespace, field_name, field_number, removed FROM grpc_field_numbers"
    )
    return FieldNumberAllocator(
        [
            (r["table_id"], r["namespace"], r["field_name"], r["field_number"], r["removed"])
            for r in rows
        ]
    )


async def persist_field_numbers(conn, allocator: FieldNumberAllocator) -> None:
    """Flush every number allocated or retired during this build. A no-op build (no new/removed
    columns anywhere) issues no write."""
    if (
        getattr(conn, "capabilities", None) is not None
        and conn.capabilities.dialect != "postgresql"
    ):
        return
    rows = allocator.pending_rows()
    if not rows:
        return
    await conn.executemany(
        """
        INSERT INTO grpc_field_numbers (table_id, namespace, field_name, field_number, removed)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (table_id, namespace, field_name)
        DO UPDATE SET field_number = EXCLUDED.field_number, removed = EXCLUDED.removed
        """,
        rows,
    )
