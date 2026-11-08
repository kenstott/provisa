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

import sqlglot.expressions as exp

import hashlib

#: PostgreSQL's first oid for a user's object: a table's is this plus its registered id.
FIRST_USER_OID = 16384
#: A key's number is its table's id times this, plus its slot on the table (meta.table_keys).
KEY_SLOTS = 4096
#: The slot before a table's first foreign key; a primary key is slot 0, unique keys 1 and up.
FOREIGN_KEY_SLOT = 1024
#: The last unique slot, held by the key every table's link to its registration references.
UNIQUE_QUERY_NAME_SLOT = 1023
#: Added to a key's number for its oid in pg_constraint, and for its index's in pg_class: clear
#: of every table's oid.
KEY_OID_BASE = 1_000_000

#: PostgreSQL's own schemas, at PostgreSQL 17's oids, and the role that owns every table.
PG_CATALOG_OID = 11
INFORMATION_SCHEMA_OID = 13283
#: The system schemas, as SQL's list of their oids.
SYSTEM_SCHEMA_OIDS = f"({PG_CATALOG_OID}, {INFORMATION_SCHEMA_OID})"
PUBLIC_OID = 2200
OWNER_OID = 10
#: A tracked function's oid is this plus its id.
FIRST_FUNCTION_OID = 40000
#: A metric, which no registered id numbers: above every key's oid.
FIRST_METRIC_OID = 3_000_000_000


def table_oid(table_id: int) -> int:
    """A registered table's oid."""
    return FIRST_USER_OID + table_id


def metric_oid(metric: str) -> int:
    """A metric's oid, as :func:`metric_oid_sql` computes it."""
    digest = hashlib.md5(metric.encode(), usedforsecurity=False).hexdigest()
    return FIRST_METRIC_OID + int(digest[:7], 16)


def digest_sql(text: str) -> str:
    """28 bits of the digest of ``text`` (SQL): stable while the text is, whatever else is added
    to the model. Two texts share one about once in 500 across 1,000 of them."""
    return f"(CAST('x' || SUBSTR(MD5({text}), 1, 7) AS BIT(28))::INTEGER)"


def schema_oid_sql(schema: str) -> str:
    """A schema's oid, from its SQL name (SQL): a domain added to the model moves no other's."""
    return (
        f"CASE {schema} WHEN 'pg_catalog' THEN {PG_CATALOG_OID} "
        f"WHEN 'information_schema' THEN {INFORMATION_SCHEMA_OID} WHEN 'public' THEN {PUBLIC_OID} "
        f"ELSE {FIRST_USER_OID} + {digest_sql(schema)} END"
    )


def role_oid_sql(role: str) -> str:
    """A role's oid, from its id (SQL)."""
    return f"{FIRST_USER_OID} + {digest_sql(role)}"


def metric_oid_sql(metric: str) -> str:
    """A metric's oid, from its name (SQL)."""
    return f"{FIRST_METRIC_OID} + {digest_sql(metric)}"


def owner_table(oid: exp.Expression) -> exp.Expression:  # REQ-913
    """The registered table the relation oid ``oid`` is or belongs to; NULL for a system relation
    or a metric, which belong to none. An expression: a row rule is made of it."""

    def relation() -> exp.Expression:
        return exp.cast(oid.copy(), "BIGINT")

    def at_least(bound: int) -> exp.Expression:
        return exp.GTE(this=relation(), expression=exp.Literal.number(bound))

    keyed = exp.Div(
        this=exp.paren(exp.Sub(this=relation(), expression=exp.Literal.number(KEY_OID_BASE))),
        expression=exp.Literal.number(KEY_SLOTS),
        typed=True,  # PostgreSQL's integer division, as the column is an integer
    )
    return exp.Case(
        ifs=[
            exp.If(this=at_least(FIRST_METRIC_OID), true=exp.Null()),
            exp.If(this=at_least(KEY_OID_BASE), true=keyed),
            exp.If(
                this=at_least(FIRST_USER_OID),
                true=exp.Sub(this=relation(), expression=exp.Literal.number(FIRST_USER_OID)),
            ),
        ]
    )


def table_oid_sql(table_id: str) -> str:
    """A registered table's oid (SQL)."""
    return f"({FIRST_USER_OID} + {table_id})"


def key_oid_sql(key_id: str) -> str:
    """A key's oid, its constraint's and its index's (SQL)."""
    return f"({KEY_OID_BASE} + {key_id})"


def table_id_sql(oid: str) -> str:
    """The registered table id the oid ``oid`` (SQL) names."""
    return f"(CAST({oid} AS BIGINT) - {FIRST_USER_OID})"


def key_id_sql(oid: str) -> str:
    """The meta.table_keys.key_id the constraint or index oid ``oid`` (SQL) names."""
    return f"(CAST({oid} AS BIGINT) - {KEY_OID_BASE})"
