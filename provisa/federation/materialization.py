# Copyright (c) 2026 Kenneth Stott
# Canary: 4d8b2c71-6a09-4f53-9e12-3c7a0d4f8b83
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Materialization store: backend validity, write face, and the reactive set.

The materialization_store is the durable, FEDERATABLE, REFRESHABLE store that the engine
reads as a real relation (REQ-844). This module holds the parts that compose purely on the
federation engine + connector contract:

- REQ-846: a backend is valid iff the engine can READ what was landed into it — i.e. it has
  an ATTACH connector for the backend (or the backend is the engine's own native store). A
  backend with no connector, or only a LAND connector, is a land-into-land regress → rejected.
- REQ-848: the write face is pluggable — engine-native CTAS/load (collapses in when the
  backend is the engine's own store), SQLAlchemy upsert (a separate attach-able relational
  store), or app-side land.
- REQ-845: the reactive-replica set is engine-relative — the sources that federate() resolves
  to MATERIALIZED (reach == land) for the configured engine.
"""

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

from provisa.federation.connector import Mechanism
from provisa.federation.engine import UnreachableSource
from provisa.federation.strategy import Strategy, federate

if TYPE_CHECKING:
    from provisa.core.models import Source
    from provisa.federation.engine import FederationEngine

# Relational store types that can back a materialization store via SQLAlchemy upsert.
_RELATIONAL = frozenset(
    {"postgresql", "mysql", "mariadb", "sqlite", "duckdb", "sqlserver", "singlestore"}
)


class InvalidMaterializationBackend(Exception):  # REQ-846
    """Raised at config validation when a materialization_store backend is unusable."""


def validate_materialization_backend(
    engine: FederationEngine, backend_type: str
) -> None:  # REQ-846
    """Reject a materialization backend the engine could not read back (REQ-846).

    Valid iff the backend is the engine's own native store, or the engine has an ATTACH
    connector for it. No connector, or a LAND-only connector, is a land-into-land regress.
    """
    if backend_type == engine.native_store:
        return  # engine materializes into its own store and reads it natively
    connector = engine.connectors.get(backend_type)
    if connector is None:
        raise InvalidMaterializationBackend(
            f"engine {engine.name!r} has no connector for materialization backend {backend_type!r} "
            f"— it could not read what was landed"
        )
    if connector.mechanism not in (Mechanism.ATTACH_RW, Mechanism.ATTACH_R):
        raise InvalidMaterializationBackend(
            f"materialization backend {backend_type!r} on engine {engine.name!r} is not attach-"
            f"readable (FETCH/DIRECT) — the engine could not read what was landed"
        )


class WriteFace(str, Enum):  # REQ-848
    ENGINE_NATIVE = "engine_native"  # CTAS/load into the engine's own store
    SQLALCHEMY_UPSERT = "sqlalchemy_upsert"  # a separate attach-able relational store
    APP_LAND = "app_land"  # app-side land (non-relational / no upsert face)


def select_write_face(engine: FederationEngine, backend_type: str) -> WriteFace:  # REQ-848
    """Pick the write face for landing into ``backend_type`` on ``engine``.

    Validates the backend first (REQ-846). Collapses into the engine when the backend is its
    own store; uses SQLAlchemy upsert for a separate attach-able relational store; else app-land.
    """
    validate_materialization_backend(engine, backend_type)
    if backend_type == engine.native_store:
        return WriteFace.ENGINE_NATIVE
    if backend_type in _RELATIONAL:
        return WriteFace.SQLALCHEMY_UPSERT
    return WriteFace.APP_LAND


def reactive_sources(engine: FederationEngine, sources: list[Source]) -> set[str]:  # REQ-845
    """The engine-relative reactive-replica set: sources that federate to MATERIALIZED.

    reactive = { source.id : federate(source, engine) is MATERIALIZED } — the sources with
    no attach/scan reach on this engine, which must be pulled through into the store.
    """
    out: set[str] = set()
    for source in sources:
        try:
            strategy = federate(source, engine)
        except UnreachableSource:
            continue  # unreachable sources are not reactive replicas
        if strategy is Strategy.MATERIALIZED:
            out.add(source.id)
    return out
