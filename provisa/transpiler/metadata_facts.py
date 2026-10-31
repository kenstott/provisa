# Copyright (c) 2026 Kenneth Stott
# Canary: 7b42c9e5-1d38-4f06-a3c7-e08d52b6f193
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What the privilege functions read that no view can give (REQ-2190): what a role holds on each
relation and which of a listed table's columns it is not served. Both come from the role's
governance context: what a source takes is decided in code from its type and the engine, and a
column's grant rule is the compiler's. They are relations of the statement that asks, not of meta:
every other fact a catalog shows is a view."""

# Requirements: REQ-2190

from __future__ import annotations

from collections.abc import Collection, Iterable, Mapping
from dataclasses import dataclass
from typing import Any

import sqlglot.expressions as exp

from provisa.transpiler.metadata_oids import table_oid

PRIVILEGES = "_provisa_privileges"
UNSERVED = "_provisa_unserved_columns"

_RELATIONS: Mapping[str, tuple[tuple[str, str], ...]] = {
    PRIVILEGES: (("relation_oid", "BIGINT"), ("privilege", "TEXT")),
    UNSERVED: (("relation_oid", "BIGINT"), ("column_name", "TEXT")),
}


@dataclass(frozen=True)
class CatalogFacts:
    """A role's facts: rows of ``PRIVILEGES`` and ``UNSERVED``, and the role."""

    role_id: str
    privileges: tuple[tuple[Any, ...], ...] = ()
    unserved: tuple[tuple[Any, ...], ...] = ()

    def rows(self, relation: str) -> tuple[tuple[Any, ...], ...]:
        return {PRIVILEGES: self.privileges, UNSERVED: self.unserved}[relation]


def role_privileges(
    table_ids: Iterable[int],
    write_grants: Mapping[int, Mapping[str, Any]],
    write_ops: Mapping[int, Collection[str]],
    can_write: bool,
) -> dict[int, frozenset[str]]:
    """What a role holds on the tables it is served, ``table_ids``: SELECT on each, and an
    operation its source takes where the role holds the write right and the table carries its
    grant (compiler/write_admission's rule, read from its governance context)."""
    return {
        table_id: frozenset(
            {
                "SELECT",
                *(
                    operation.upper()
                    for operation in write_grants.get(table_id, ())
                    if can_write and operation in write_ops.get(table_id, ())
                ),
            }
        )
        for table_id in table_ids
    }


def catalog_facts(
    role_id: str,
    privileges: Mapping[int, Collection[str]],
    unserved: Iterable[tuple[int, str]] = (),
    metric_oids: Iterable[int] = (),
) -> CatalogFacts:
    """``role_id``'s facts. ``privileges`` is :func:`role_privileges`'; ``unserved`` is each
    (table id, SQL name) of a listed table's column the role is not served; ``metric_oids`` the
    metrics it reads, each of which it holds SELECT on."""
    return CatalogFacts(
        role_id=role_id,
        privileges=(
            *(
                (table_oid(table_id), privilege)
                for table_id, held in sorted(privileges.items())
                for privilege in sorted(held)
            ),
            *((oid, "SELECT") for oid in sorted(metric_oids)),
        ),
        unserved=tuple(sorted((table_oid(table_id), name) for table_id, name in unserved)),
    )


def _literal(value: Any) -> exp.Expr:
    return exp.Literal.number(value) if isinstance(value, int) else exp.Literal.string(str(value))


def relation(name: str, rows: tuple[tuple[Any, ...], ...]) -> exp.Select:
    """The select a statement's CTE ``name`` holds: ``rows``, each column of its type; none where
    there are no rows."""
    columns = _RELATIONS[name]
    if not rows:
        return exp.select(
            *(exp.alias_(exp.cast(exp.null(), sql_type), column) for column, sql_type in columns)
        ).where(exp.false())
    values = exp.Values(
        expressions=[exp.Tuple(expressions=[_literal(v) for v in row]) for row in rows],
        alias=exp.TableAlias(
            this=exp.to_identifier("_v"),
            columns=[exp.to_identifier(column) for column, _ in columns],
        ),
    )
    typed = [
        exp.alias_(exp.cast(exp.column(column), sql_type), column) for column, sql_type in columns
    ]
    return exp.select(*typed).from_(values)
