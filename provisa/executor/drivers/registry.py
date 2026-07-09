# Copyright (c) 2026 Kenneth Stott
# Canary: 707be178-3988-403f-afd9-6057f92280e8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Driver registry — maps source types to their DirectDriver implementations.

Drivers are lazily imported so missing optional dependencies don't break startup.
"""

# Requirements: REQ-229, REQ-550

from __future__ import annotations

from collections.abc import Callable

from provisa.executor.drivers.base import DirectDriver


def _make_pg() -> DirectDriver:
    from provisa.executor.drivers.postgresql import PostgreSQLDriver

    return PostgreSQLDriver()


def _make_mysql() -> DirectDriver:
    from provisa.executor.drivers.mysql import MySQLDriver

    return MySQLDriver()


def _make_duckdb() -> DirectDriver:
    from provisa.executor.drivers.duckdb_driver import DuckDBDriver

    return DuckDBDriver()


def _make_sqlserver() -> DirectDriver:
    from provisa.executor.drivers.sqlserver import SQLServerDriver

    return SQLServerDriver()


def _make_oracle() -> DirectDriver:
    from provisa.executor.drivers.oracle import OracleDriver

    return OracleDriver()


# source_type → factory function
_DRIVER_FACTORIES: dict[str, Callable[[], DirectDriver]] = {  # REQ-229, REQ-550
    "postgresql": _make_pg,
    "mysql": _make_mysql,
    "singlestore": _make_mysql,  # MySQL wire-compatible
    "mariadb": _make_mysql,  # MySQL wire-compatible
    "duckdb": _make_duckdb,
    "sqlserver": _make_sqlserver,
    "oracle": _make_oracle,
    # Wire-compatible RDBs reuse the base wire's native driver (REQ-950)
    "cockroachdb": _make_pg,
    "yugabytedb": _make_pg,
    "greenplum": _make_pg,
    "tidb": _make_mysql,
}

# FALLBACK: source types with no bespoke async driver, served by the generic SQLAlchemy driver
# (REQ-229). Value is the SQLAlchemy URL drivername. A native factory above always wins; this only
# fills gaps, broadening the writable/readable set to any SQLAlchemy dialect whose DBAPI is installed.
_SQLALCHEMY_FALLBACK: dict[str, str] = {
    "sqlite": "sqlite",
}


def _make_sqlalchemy(drivername: str) -> Callable[[], DirectDriver]:
    def factory() -> DirectDriver:
        from provisa.executor.drivers.sqlalchemy_driver import SQLAlchemyDriver

        return SQLAlchemyDriver(drivername)

    return factory


def create_driver(source_type: str, **kwargs) -> DirectDriver:  # REQ-550
    """Create a driver instance for a source type.

    Raises KeyError if no driver is registered for the type.
    Raises ImportError if the driver's dependency is not installed.

    Keyword args are forwarded to driver constructors that accept them
    (e.g., use_pgbouncer for PostgreSQL).
    """
    factory = _DRIVER_FACTORIES.get(source_type)
    if factory is None:
        fallback = _SQLALCHEMY_FALLBACK.get(source_type)
        if fallback is None:
            raise KeyError(f"No direct driver for source type: {source_type!r}")
        factory = _make_sqlalchemy(fallback)
    driver = factory()
    # Apply kwargs to driver if it accepts them (e.g., PgBouncer config)
    for key, value in kwargs.items():
        if hasattr(driver, f"_{key}"):
            object.__setattr__(driver, f"_{key}", value)
    return driver


def has_native_driver(source_type: str) -> bool:  # REQ-550
    """Whether a BESPOKE native driver is registered for the type and its dependency imports."""
    factory = _DRIVER_FACTORIES.get(source_type)
    if factory is None:
        return False
    try:
        factory()
        return True
    except ImportError:
        return False


def has_sqlalchemy_fallback(source_type: str) -> bool:  # REQ-229
    """Whether the generic SQLAlchemy fallback covers the type and its DBAPI imports."""
    drivername = _SQLALCHEMY_FALLBACK.get(source_type)
    if drivername is None:
        return False
    try:
        _make_sqlalchemy(drivername)()
        return True
    except ImportError:
        return False


def has_driver(source_type: str) -> bool:  # REQ-550
    """Check if a direct driver (native or SQLAlchemy fallback) exists and its dependency is installed."""
    return has_native_driver(source_type) or has_sqlalchemy_fallback(source_type)


def available_drivers() -> list[str]:  # REQ-550
    """List source types with a registered direct driver (native or SQLAlchemy fallback)."""
    available = []
    for stype in list(_DRIVER_FACTORIES) + list(_SQLALCHEMY_FALLBACK):
        if has_driver(stype):
            available.append(stype)
    return available
