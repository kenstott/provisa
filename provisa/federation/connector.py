# Copyright (c) 2026 Kenneth Stott
# Canary: 3f8a2c60-7b19-4d54-9e02-1c7a0d6f8b52
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Generic connector (REQ-842, REQ-947).

``WarehouseNativeConnector`` — the land-reach connector used by all warehouse engines for source
types they cannot attach live. Trino connectors live in trino_connectors.py; ClickHouse connectors
live in clickhouse_connectors.py; DuckDB/Postgres connectors live in connector_duckdb.py.
"""

# complexity-gate: allow-ble=1 reason="connector probe (REQ-904) reports any extension load failure as unavailable, surfacing the error type in the ProbeResult"

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from provisa.core.models import Source

from provisa.federation.connector_base import Capability, Connector, Mechanism


class WarehouseNativeConnector(Connector):
    """A source the engine cannot attach live: Provisa reads it and lands a refreshed replica into the
    engine's materialization store, which the engine reads — MATERIALIZED, not a live attach (the engine
    never lands; Provisa does — REQ-848/951). ``mechanism`` records HOW Provisa reads the source:
    ``DIRECT`` (a native async driver, executor/drivers) or ``FETCH`` (an API/push adapter,
    source_adapters). This is the unified land-reach connector that completes ``engine.connectors`` for
    every direct-route / adapter source type, so the source-creation dropdown is a pure projection of
    the registry (REQ-947)."""

    def __init__(
        self, engine: str, source_type: str, mechanism: Mechanism = Mechanism.DIRECT
    ) -> None:
        self.engine = engine
        self.source_type = source_type
        self.mechanism = mechanism
        # A stable key so discover()'s report + override strike-list address it distinctly per type.
        self.key = f"land_{engine}_{source_type}"

    def capability(self) -> Capability:
        # The engine reads the LANDED replica in its own store — full pushdown and writable.
        return Capability(
            predicate_pushdown=True, join_pushdown=True, aggregate_pushdown=True, write=True
        )

    def details(self, source: Source) -> dict:
        return {}  # land: nothing to attach; the engine reads the replica in its store
