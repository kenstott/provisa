# Copyright (c) 2025 Kenneth Stott
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

from __future__ import annotations

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
_DRIVER_FACTORIES: dict[str, callable] = {
    "postgresql": _make_pg,
    "mysql": _make_mysql,
    "singlestore": _make_mysql,   # MySQL wire-compatible
    "mariadb": _make_mysql,       # MySQL wire-compatible
    "duckdb": _make_duckdb,
    "sqlserver": _make_sqlserver,
    "oracle": _make_oracle,
}


def create_driver(source_type: str, **kwargs) -> DirectDriver:
    """Create a driver instance for a source type.

    Raises KeyError if no driver is registered for the type.
    Raises ImportError if the driver's dependency is not installed.

    Keyword args are forwarded to driver constructors that accept them
    (e.g., use_pgbouncer for PostgreSQL).
    """
    factory = _DRIVER_FACTORIES.get(source_type)
    if factory is None:
        raise KeyError(f"No direct driver for source type: {source_type!r}")
    driver = factory()
    # Apply kwargs to driver if it accepts them (e.g., PgBouncer config)
    for key, value in kwargs.items():
        if hasattr(driver, f"_{key}"):
            object.__setattr__(driver, f"_{key}", value)
    return driver


def has_driver(source_type: str) -> bool:
    """Check if a direct driver exists and its dependency is installed."""
    factory = _DRIVER_FACTORIES.get(source_type)
    if factory is None:
        return False
    try:
        factory()
        return True
    except ImportError:
        return False


def available_drivers() -> list[str]:
    """List source types with registered direct drivers."""
    available = []
    for stype, factory in _DRIVER_FACTORIES.items():
        try:
            factory()
            available.append(stype)
        except ImportError:
            pass
    return available
