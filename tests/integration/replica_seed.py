# Copyright (c) 2026 Kenneth Stott
# Canary: 4c53457d-3871-4530-b0ba-e9e95cd24a13
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Seed a replica on a live engine runtime and read it where it lives (REQ-1912).

The engine-level integration suites replicate a few rows of a table and read them back through the
governed pipeline's SQL. A replica lives in the replicas schema under the one replica name, and the
statement reaches it through the address pass — so a suite seeds with :func:`seed_replica` and
lowers its statement with :func:`routes_to`, exactly the two halves the product uses."""

from __future__ import annotations

from typing import Any

from provisa.federation.replica_address import (
    ReplicaRoute,
    ReplicaRoutes,
    TableKey,
    replica_table_name,
)

#: The replicas schema of the integration suites' org.
REPLICAS_SCHEMA = "org_itest_replicas"


def replica_of(source_id: str, schema_name: str, table_name: str) -> tuple[str, str]:
    """The (schema, table) the replica of ``source_id``'s table is written to."""
    return REPLICAS_SCHEMA, replica_table_name(source_id, schema_name, table_name)


async def seed_replica(
    runtime: Any,
    source_id: str,
    schema_name: str,
    table_name: str,
    columns: list[tuple[str, str]],
    rows: list[dict],
    *,
    pk_columns: list[str] | None = None,
    change_signal: str = "ttl",
    watermark_column: str | None = None,
) -> tuple[str, str]:
    """Reconcile the table's replica on ``runtime`` and land ``rows`` into it. Returns its
    (schema, table)."""
    schema, table = replica_of(source_id, schema_name, table_name)
    await runtime.reconcile_replica(
        schema=schema, table=table, columns=columns, pk_columns=pk_columns
    )
    await runtime.land_table(
        schema=schema,
        table=table,
        columns=columns,
        rows=rows,
        change_signal=change_signal,
        watermark_column=watermark_column,
        pk_columns=pk_columns,
    )
    return schema, table


def routes_to(
    engine_name: str,
    read_catalog: str | None,
    *,
    live: TableKey,
    source_id: str,
    schema_name: str,
    table_name: str,
) -> ReplicaRoutes:
    """The published routes for one replica-served table: ``live`` is the table as a lowered
    statement names it, ``read_catalog`` the catalog the engine reads its store under."""
    schema, table = replica_of(source_id, schema_name, table_name)
    route = ReplicaRoute(source_id, table_name, (read_catalog, schema, table))
    return ReplicaRoutes(engine_name=engine_name, routes={live: route})
