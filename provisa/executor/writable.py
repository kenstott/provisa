# Copyright (c) 2026 Kenneth Stott
# Canary: 1e7c9a52-4b38-4d75-8e02-2c7a0d4f9d22
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The single source of truth for the write path: which source types are writable, and by which route.

A mutation can reach a source three ways, in DESCENDING preference (native → sqlalchemy → engine):

1. NATIVE     — a bespoke async driver (asyncpg/aiomysql/…) runs the mutation directly. Requires the
                driver's dependency to import AND a SQLGlot write dialect (the mutation is compiled to
                PG-canonical SQL then transpiled to the source's dialect before it reaches the driver).
2. SQLALCHEMY — the generic SQLAlchemy fallback driver, for a type with no native driver. Same two
                gates (DBAPI installed + SQLGlot dialect); broadens the set to any SQLAlchemy dialect.
3. ENGINE     — the federation engine writes upstream through a write-capable ATTACH connector
                (postgres_fdw, DuckDB/the engine ATTACH — Capability.write). The engine executes the
                mutation in ITS OWN dialect against the attached/foreign table, so no per-source
                SQLGlot gate applies; the only gate is the connector declaring write support. This is
                the only route that can reach a source with no direct driver AND no SQLGlot dialect
                (e.g. a NoSQL source exposed as a writable relation by its connector).

``resolve_write_path`` returns the highest-preference available route (or None). The direct predicates
(``is_writable`` = native-or-sqlalchemy) remain for callers that only care about the driver path.
"""

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

from provisa.core.source_registry import SOURCE_TO_DIALECT
from provisa.executor.drivers.registry import (
    has_driver,
    has_native_driver,
    has_sqlalchemy_fallback,
)

if TYPE_CHECKING:
    from provisa.federation.engine import FederationEngine


class WritePath(str, Enum):  # REQ-229, REQ-842
    NATIVE = "native"  # bespoke async driver, direct
    SQLALCHEMY = "sqlalchemy"  # generic SQLAlchemy fallback, direct
    ENGINE = "engine"  # federation engine writes upstream via a write-capable connector


def sqlglot_write_dialect(source_type: str) -> str | None:
    """The SQLGlot write dialect for ``source_type``, or None when SQLGlot cannot emit it.

    Resolves the source type to its dialect name (SOURCE_TO_DIALECT) and confirms SQLGlot
    recognizes it — an unmapped type, or a name SQLGlot does not know, yields None (no guess).
    """
    dialect = SOURCE_TO_DIALECT.get(source_type)
    if dialect is None:
        return None
    from sqlglot.dialects.dialect import Dialect

    try:
        Dialect.get_or_raise(dialect)
    except ValueError:  # sqlglot raises ValueError for an unknown dialect name
        return None
    return dialect


def is_writable(source_type: str) -> bool:
    """Whether the direct write path can mutate ``source_type``: native driver AND SQLGlot dialect."""
    return has_driver(source_type) and sqlglot_write_dialect(source_type) is not None


def writable_source_types() -> list[str]:
    """Every source type the direct write path can mutate (both gates satisfied)."""
    return [t for t in SOURCE_TO_DIALECT if is_writable(t)]


def is_writable_via_engine(source_type: str, engine: FederationEngine) -> bool:
    """Whether ``source_type`` can be written through the engine's connector (Capability.write).

    The engine executes the mutation in its own dialect against the attached/foreign table, so no
    per-source SQLGlot gate applies — the sole gate is the connector declaring upstream write support.
    """
    connector = engine.connectors.get(source_type)
    return connector is not None and connector.capability().write


def engine_writable_source_types(engine: FederationEngine) -> set[str]:
    """The source types the engine can write upstream through a write-capable connector."""
    return {t for t, c in engine.connectors.items() if c.capability().write}


def resolve_write_path(
    source_type: str, engine: FederationEngine | None = None
) -> WritePath | None:
    """The highest-preference write route for ``source_type``, or None if unwritable.

    Preference order is native → sqlalchemy → engine: a bespoke driver is fastest and best-tuned; the
    SQLAlchemy fallback fills driver gaps; the engine route is last, used when there is no direct
    driver at all (and it alone can reach a source with no SQLGlot dialect). ``engine`` is required to
    consider the ENGINE route.
    """
    has_dialect = sqlglot_write_dialect(source_type) is not None
    if has_native_driver(source_type) and has_dialect:
        return WritePath.NATIVE
    if has_sqlalchemy_fallback(source_type) and has_dialect:
        return WritePath.SQLALCHEMY
    if engine is not None and is_writable_via_engine(source_type, engine):
        return WritePath.ENGINE
    return None


def is_writable_on(source_type: str, engine: FederationEngine | None = None) -> bool:
    """Whether ``source_type`` is writable by ANY route (direct driver, or engine when supplied)."""
    return resolve_write_path(source_type, engine) is not None
