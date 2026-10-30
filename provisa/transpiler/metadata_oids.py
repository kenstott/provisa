# Copyright (c) 2026 Kenneth Stott
# Canary: 0e6a1f3c-92d4-4c7b-b1a8-5d7e3c9f4a21
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The object identifiers PostgreSQL's catalog shows of the model, each a function of a key the
model holds (REQ-2190): nothing is numbered in the order a build happens to meet it, so one model
shows one set of oids on every build, to every role."""

# Requirements: REQ-2190

from __future__ import annotations

import hashlib

#: PostgreSQL's first oid for a user's object: a table's is this plus its registered id.
FIRST_USER_OID = 16384
#: A key's number is its table's id times this, plus its slot on the table (meta.table_keys).
KEY_SLOTS = 4096
#: The slot before a table's first foreign key; a primary key is slot 0, unique keys 1 and up.
FOREIGN_KEY_SLOT = 1024
#: Added to a key's number for its oid in pg_constraint, and for its index's in pg_class: clear
#: of every table's oid.
KEY_OID_BASE = 1_000_000

#: The schemas PostgreSQL itself numbers.
_FIXED_SCHEMAS = {"pg_catalog": 11, "information_schema": 12, "public": 2200}
#: A schema's oid is this many bits of its name's digest above FIRST_USER_OID.
_SCHEMA_BITS = 28


def table_oid(table_id: int) -> int:
    """A registered table's oid."""
    return FIRST_USER_OID + table_id


def schema_oid(schema: str) -> int:
    """A schema's oid, from its SQL name: a domain added to the model moves no other's."""
    fixed = _FIXED_SCHEMAS.get(schema)
    if fixed is not None:
        return fixed
    digest = hashlib.sha256(schema.encode()).digest()
    return FIRST_USER_OID + (int.from_bytes(digest[:8], "big") >> (64 - _SCHEMA_BITS))


def table_id_sql(oid: str) -> str:
    """The registered table id the oid ``oid`` (SQL) names."""
    return f"(({oid}) - {FIRST_USER_OID})"


def key_id_sql(oid: str) -> str:
    """The meta.table_keys.key_id the constraint or index oid ``oid`` (SQL) names."""
    return f"(({oid}) - {KEY_OID_BASE})"
