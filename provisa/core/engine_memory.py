# Copyright (c) 2026 Kenneth Stott
# Canary: 3396dbaa-ba84-4218-936f-b650e97e7206
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The memory cap of a DuckDB database this process opens (REQ-2006, engine.duckdb_memory_limit),
and the rule that it only ever loads extensions already staged (REQ-294).

DuckDB caps each database on its own: the federation runtime's, the in-memory store's, a DuckDB
source's, and every short-lived one a step opens. Each is opened with :func:`connect`, which caps
it; each long-lived one also says its cap once it is open (:func:`log_limit`), so a run whose server
grew can tell whether the cap was in force."""

from __future__ import annotations

import logging
import os
from typing import Any

import duckdb

# Said where a server's startup lines are said (uvicorn's own error log, shown at INFO): the cap a
# database runs under is read off a run's log, and this module's own logger is not shown at INFO.
_said = logging.getLogger("uvicorn.error")

_MIB = 1024 * 1024

#: The staged extension directory, when an install names one (else DuckDB's per-user directory).
EXT_DIR_ENV = "PROVISA_DUCKDB_EXT_DIR"


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
    settings = load_only_config(
        str(extra["extension_directory"]) if "extension_directory" in extra else None
    )
    return duckdb.connect(database, read_only=read_only, config={**capped, **extra, **settings})


class ExtensionNotStaged(RuntimeError):
    """A DuckDB extension is not in the extension directory, and the runtime never fetches one
    (REQ-294, AIR-GAPPED IS LOCAL FIRST)."""


#: How to put an extension in place: the install steps that exist. Named in every refusal.
STAGE_FIX = (
    "run `python -m provisa.federation.stage_duckdb_extensions` once on a connected machine "
    "(it stages what the provisa[embedded] wheel could not hold), or on a source checkout "
    "`python scripts/stage_duckdb_extensions.py`; the Provisa installers hold every extension"
)


def load_only_config(directory: str | None = None) -> DuckDBConfig:
    """REQ-294: the settings every DuckDB connection is opened with. DuckDB would otherwise fetch a
    known extension from extensions.duckdb.org the first time a query names it. Installing happens
    only at build or packaging time, so a connection only ever loads what is already staged."""
    settings: DuckDBConfig = {
        "autoinstall_known_extensions": False,
        "autoload_known_extensions": False,
    }
    directory = directory or os.environ.get(EXT_DIR_ENV)
    if directory:
        settings["extension_directory"] = directory
    return settings


def load(con: Any, extension: str) -> None:
    """LOAD ``extension`` from the extension directory, or refuse naming it and the fix. Never an
    INSTALL and never a retry: an extension that isn't staged is an install defect."""
    try:
        con.execute(f"LOAD {extension}")
    except (duckdb.IOException, duckdb.InvalidInputException) as e:
        raise ExtensionNotStaged(
            f"DuckDB extension '{extension}' could not be loaded from the extension directory, "
            f"and Provisa never downloads one at runtime. To fix it: {STAGE_FIX}. ({e})"
        ) from e


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
