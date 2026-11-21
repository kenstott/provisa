# Copyright (c) 2026 Kenneth Stott
# Canary: 71c4e9a0-2b85-4f3d-9e16-d8a5b07f3c42
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a statement pushes into a QuickBooks table's call (REQ-2197): its conditions and LIMIT,
lowered by ``quickbooks.query``, as arguments of the call, so the answer is kept under them and
never held as the whole table's."""

# Requirements: REQ-2197
from __future__ import annotations

from typing import Any

from provisa.api_source.models import ApiEndpoint
from provisa.quickbooks.entities import entity
from provisa.quickbooks.query import narrowed, sql_columns
from provisa.quickbooks.rows import NORMALIZER

#: The query every QuickBooks table is registered with starts so; the entity follows.
_FROM = "SELECT * FROM "


def endpoint_entity(endpoint: ApiEndpoint) -> str:
    """The entity a QuickBooks table's endpoint reads, from its registered query."""
    query = endpoint.query_template or ""
    if not query.startswith(_FROM):
        raise ValueError(f"{endpoint.table_name}: not a QuickBooks entity query: {query!r}")
    return query[len(_FROM) :].split(" ", 1)[0]


def pushed_arguments(endpoint: ApiEndpoint, sql: str, table: str) -> dict[str, Any]:
    """The arguments a QuickBooks table's call is sent with to answer the statement ``sql``:
    none for any other table, or when nothing is pushed."""
    if endpoint.response_normalizer != NORMALIZER:
        return {}
    read = entity(endpoint_entity(endpoint))
    registered = {c.name for c in endpoint.columns}
    columns = {name: c for name, c in sql_columns(read).items() if c.name in registered}
    return narrowed(read, sql, table, columns)
