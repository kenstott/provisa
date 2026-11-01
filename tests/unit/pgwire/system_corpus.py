# Copyright (c) 2026 Kenneth Stott
# Canary: a2c7e05d-4f18-4b63-8d97-3e6f1b90c742
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The statements of pg_forms_system: each is recorded on postgres:17.11 in its default state, which
is the state every Provisa server is in (REQ-2171)."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

_REFUSED = (
    "pg_get_wal_replay_pause_state",
    "pg_is_wal_replay_paused",
    "pg_wal_replay_pause",
    "pg_wal_replay_resume",
    "pg_last_committed_xact",
    "pg_ls_logdir",
    "pg_replication_origin_session_reset",
    "pg_sync_replication_slots",
)
_SLOTS = (
    "pg_logical_slot_get_changes",
    "pg_logical_slot_peek_changes",
    "pg_logical_slot_get_binary_changes",
    "pg_logical_slot_peek_binary_changes",
)
_SLOT_CALLS = (
    "NULL::name, NULL::pg_lsn, NULL::integer, VARIADIC NULL::text[]",
    "'s'::name, NULL::pg_lsn, NULL::integer",
    "'s'::name, NULL::pg_lsn, 1::integer, 'k'::text, 'v'::text",
)
_NOTIFY = (
    "'conformance'::text, NULL::text",
    "'c'::text, 'p'::text",
    "NULL::text, 'p'::text",
    "''::text, 'p'::text",
    "repeat('x', 63), 'p'::text",
    "repeat('x', 64), 'p'::text",
    "'c'::text, repeat('x', 7999)",
    "'c'::text, repeat('x', 8000)",
)
_COLLATIONS = (
    "NULL::text",
    "'a'::text",
    "'a'::varchar",
    "'a'::name",
    "'a'",
    "NULL::integer",
    "1::integer",
    "1.5::numeric",
    "true::boolean",
)
_STORED = ("NULL::integer", "1::integer", "'a'::text", "repeat('x', 10000)")
_REPLACED = (
    "'abc'::text, 'b'::text, 'abc'::text, 3::integer, 3::integer",
    "NULL::text, NULL::text, NULL::text, NULL::integer, NULL::integer",
    "NULL::text, 'b'::text, 'x'::text, 1::integer, 1::integer",
    "'abc'::text, 'b'::text, 'x'::text, NULL::integer, 1::integer",
    "'abcabc'::text, 'b'::text, 'X'::text, 1::integer, 1::integer",
    "'abcabc'::text, 'b'::text, 'X'::text, 1::integer, 2::integer",
    "'abcabc'::text, 'b'::text, 'X'::text, 1::integer, 3::integer",
    "'abcabc'::text, 'b'::text, 'X'::text, 1::integer, 0::integer",
    "'abcabc'::text, 'b'::text, 'X'::text, 3::integer, 1::integer",
    "'abcabc'::text, 'b'::text, 'X'::text, 3::integer, 0::integer",
    "'abcabc'::text, 'b'::text, 'X'::text, 7::integer, 1::integer",
    "'abcabc'::text, '(b)(c)'::text, '\\2\\1'::text, 1::integer, 2::integer",
    "'abcabc'::text, '(b)(c)'::text, '$\\2'::text, 1::integer, 2::integer",
    "'a雪b雪c'::text, '雪'::text, 'X'::text, 2::integer, 2::integer",
    "'abc'::text, 'b'::text, 'x'::text, 0::integer, 1::integer",
    "'abc'::text, 'b'::text, 'x'::text, 1::integer, -1::integer",
)


def statements() -> list[str]:
    """Every statement, each as a client writes it."""
    return [
        *(f'SELECT pg_catalog."{name}"() AS v' for name in _REFUSED),
        *(f"SELECT {name}() AS v" for name in _REFUSED),
        *(f'SELECT pg_catalog."{name}"({call}) AS v' for name in _SLOTS for call in _SLOT_CALLS),
        *(f'SELECT pg_catalog."pg_notify"({call}) AS v' for call in _NOTIFY),
        *(f'SELECT pg_catalog."pg_collation_for"({call}) AS v' for call in _COLLATIONS),
        *(f'SELECT pg_catalog."pg_column_compression"({call}) AS v' for call in _STORED),
        *(f'SELECT pg_catalog."pg_column_toast_chunk_id"({call}) AS v' for call in _STORED),
        *(f'SELECT pg_catalog."regexp_replace"({call}) AS v' for call in _REPLACED),
    ]
