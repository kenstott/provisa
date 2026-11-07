# Copyright (c) 2026 Kenneth Stott
# Canary: 3396dbaa-ba84-4218-936f-b650e97e7206
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The memory cap of a DuckDB database this process opens (REQ-2006, engine.duckdb_memory_limit).

DuckDB caps each database on its own: the federation runtime's, the in-memory store's, a DuckDB
source's, and every short-lived one a step opens. Each is opened with :func:`connect`, which caps
it; each long-lived one also says its cap once it is open (:func:`log_limit`), so a run whose server
grew can tell whether the cap was in force.

Each is opened, too, without DuckDB 2.0's ``remote_pushdown`` optimizer (:data:`PLANNED`; new in 2.0,
1.5.5 had none): once a PostgreSQL catalog is attached it plans a statement over a large constant
list for minutes -- Cypher's ``UNWIND range(1000000, 2000000)`` took 73 s to plan where it answers
in 0.2 s without it (REQ-1142)."""

from __future__ import annotations

import logging
from typing import Any

import duckdb

# Said where a server's startup lines are said (uvicorn's own error log, shown at INFO): the cap a
# database runs under is read off a run's log, and this module's own logger is not shown at INFO.
_said = logging.getLogger("uvicorn.error")

_MIB = 1024 * 1024


#: A DuckDB connection's config, as duckdb.connect takes it.
DuckDBConfig = dict[str, str | bool | int | float | list[str]]

#: How every database this process opens plans: as DuckDB 1.5.5 did, without 2.0's remote_pushdown.
PLANNED: DuckDBConfig = {"disabled_optimizers": "remote_pushdown"}


def config(own: int | None = None) -> DuckDBConfig:
    """The ``memory_limit`` entry of a DuckDB config: ``own`` (a demo sandbox's cap) when given,
    else the deployment's setting; none when both are unset, which is DuckDB's own."""
    limit = own
    if limit is None:
        from provisa.core import settings_registry

        limit = settings_registry.value("engine.duckdb_memory_limit")
    held: DuckDBConfig = {}
    if limit is not None:
        held["memory_limit"] = f"{limit // _MIB}MiB"
    return held


def connect(
    database: str = ":memory:",
    *,
    read_only: bool = False,
    extra: DuckDBConfig | None = None,
) -> duckdb.DuckDBPyConnection:
    """A DuckDB database opened under the deployment's cap (``extra`` adds to its config, and a
    ``memory_limit`` in it -- a demo sandbox's own, a code source's -- stands, the deployment's
    not read: a built code host's image carries no settings of Provisa's, REQ-2100)."""
    extra = extra or {}
    capped = {} if "memory_limit" in extra else config()
    # Planned as 1.5.5 planned (PLANNED): 2.0's remote_pushdown is off on every database.
    return duckdb.connect(database, read_only=read_only, config={**PLANNED, **capped, **extra})


def log_limit(con: Any, what: str) -> None:
    """Log the cap ``con``'s database runs under, as DuckDB reports it."""
    (limit,) = first_row(con.execute("SELECT current_setting('memory_limit')"))
    _said.info("DuckDB %s: memory_limit %s", what, limit)


def first_row(result: Any) -> Any:
    """The first row of a DuckDB ``result`` or ``None``, read to the end so none is left open.

    ``fetchone()`` leaves the result open, and from DuckDB 2.0 a connection holding an open result
    refuses ``register``/``sql``/``create_function``/``load_extension``."""
    rows = result.fetchall()
    return rows[0] if rows else None
