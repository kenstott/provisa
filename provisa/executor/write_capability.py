# Copyright (c) 2026 Kenneth Stott
# Canary: 7a3e9c15-0b84-4d62-9f17-c5e2a8b0d463
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Which data writes a registered table's source can take — the one place it is decided.

A table offers ``insert``, ``update`` and ``delete`` (a pgwire ``COPY … FROM STDIN`` is an insert)
only as far as its source can carry them. The answer is computed here once per table when the model
loads, carried on the table's record (``write_ops``), and read everywhere a write is offered or
admitted: the write admission refuses an operation the table does not offer, naming the table and
the operation, before any right is checked; GraphQL offers a mutation field, and gRPC an insert RPC,
only for the operations the table offers; the admin table page shows them.

* A view or materialized view takes no writes.
* The model catalog's own tables (the meta and ops domains, read from the control plane's own
  sources) take none: a write there would change the model without the model store — its
  dependency refusals, origin and the rights a role may be granted.
* A remote API's tables (GraphQL, OpenAPI, gRPC) take no table writes: a remote write is one of the
  remote's operations, registered as a command.
* A SharePoint or Salesforce source takes all three through its own pgwire server, whatever
  engine serves its reads (REQ-1946).
* A source with no write route (``executor/writable.resolve_write_path``) takes none.
* An append-only store takes inserts only: ClickHouse (its UPDATE and DELETE are asynchronous
  mutations), Iceberg and Delta Lake, a Kafka topic (a produce) and an ingest stream.
* Every other writable source takes all three.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from provisa.executor.writable import resolve_write_path

if TYPE_CHECKING:
    from provisa.federation.engine import FederationEngine

WRITE_OPS: tuple[str, ...] = ("insert", "update", "delete")
_INSERT_ONLY = frozenset({"clickhouse", "iceberg", "delta_lake", "kafka", "ingest"})
_REMOTE_API = frozenset({"graphql_remote", "openapi", "grpc_remote"})
# The control plane's own sources: the model catalog and the operations log are read from them.
CONTROL_PLANE_SOURCE_IDS = frozenset({"provisa-admin", "provisa-otel"})


def table_write_ops(
    table: dict[str, Any], source_type: str | None, engine: "FederationEngine | None"
) -> frozenset[str]:
    """The data writes ``table`` (a registered table record) can take, given its source's type
    (None for a view, which has no source of its own)."""
    if table.get("view_sql") or source_type is None or source_type in _REMOTE_API:
        return frozenset()
    if table.get("source_id") in CONTROL_PLANE_SOURCE_IDS:
        return frozenset()
    if resolve_write_path(source_type, engine) is None:
        return frozenset()
    if source_type in _INSERT_ONLY:
        return frozenset({"insert"})
    return frozenset(WRITE_OPS)
