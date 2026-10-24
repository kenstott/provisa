# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The memory cap of a DuckDB database this process opens (REQ-2006, engine.duckdb_memory_limit).

DuckDB caps each database on its own: the federation runtime's, the in-memory store's, a DuckDB
source's. Each long-lived one is opened with :func:`config` and says its cap once it is open
(:func:`log_limit`), so a run whose server grew can tell whether the cap was in force."""

from __future__ import annotations

import logging
from typing import Any

log = logging.getLogger(__name__)

_MIB = 1024 * 1024


#: A DuckDB connection's config, as duckdb.connect takes it.
DuckDBConfig = dict[str, str | bool | int | float | list[str]]


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


def log_limit(con: Any, what: str) -> None:
    """Log the cap ``con``'s database runs under, as DuckDB reports it."""
    (limit,) = con.execute("SELECT current_setting('memory_limit')").fetchone()
    log.info("DuckDB %s: memory_limit %s", what, limit)
