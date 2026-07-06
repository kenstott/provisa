# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Stage + probe the DuckDB community extensions that back federation connectors (REQ-899, REQ-904).

Each DuckDB connector that needs an extension declares it (``Connector.extension``). Before the engine
can attach such a source, the extension binary must be present in DuckDB's extension directory
(``INSTALL ... FROM community`` — the STAGE step, run once at startup / packaging so it is reachable
offline) and must actually load (``LOAD`` — the PROBE step). ``probe`` is deliberately LOAD-ONLY: it
verifies the extension loads and registers its scanner/attach symbol; it never opens a live connection
to a source. That answers "is this connector's engine capability installed and reachable?", which is
distinct from "can we reach a live source?" (live reachability is a per-source concern, not a probe).

Set ``PROVISA_DUCKDB_EXT_DIR`` to stage into (and load from) a bundled directory instead of the default
per-user cache; the federation runtime reads the same variable so what is staged here is reachable there.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

import duckdb

from provisa.federation.connector import Connector
from provisa.federation.engine import build_duckdb_engine

_EXT_DIR_ENV = "PROVISA_DUCKDB_EXT_DIR"


def extension_directory() -> str | None:
    """The bundled DuckDB extension directory, or None to use DuckDB's default per-user cache."""
    return os.environ.get(_EXT_DIR_ENV) or None


def _connect() -> duckdb.DuckDBPyConnection:
    ext_dir = extension_directory()
    config: dict[str, str | bool | int | float | list[str]] = (
        {"extension_directory": ext_dir} if ext_dir else {}
    )
    return duckdb.connect(config=config)


def _fetch(con: duckdb.DuckDBPyConnection):
    """An async-shaped ``fetch(sql) -> list[dict]`` adapter over a DuckDB connection (REQ-904 probe API).

    DuckDB is synchronous; the connector probe signature is async, so this wraps ``execute`` in a
    coroutine that returns dict rows (empty for statements like INSTALL/LOAD that yield no result set).
    """

    async def fetch(sql: str) -> list[dict]:
        cur = con.execute(sql)
        if cur.description is None:
            return []
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    return fetch


@dataclass(frozen=True)
class ExtensionProbe:  # REQ-899
    key: str
    source_type: str
    extension: str
    available: bool
    reason: str
    remediation: str | None = None


def duckdb_extension_connectors() -> list[Connector]:
    """The DuckDB engine's connectors that depend on a loadable extension (REQ-899)."""
    engine = build_duckdb_engine()
    return [c for c in engine.connectors.values() if c.extension is not None]


async def stage_and_probe() -> list[ExtensionProbe]:
    """Stage (INSTALL) then probe (LOAD + symbol) each extension-backed DuckDB connector.

    A fresh connection per connector isolates a failed load from the others. No live source is opened.
    """
    probes: list[ExtensionProbe] = []
    for connector in duckdb_extension_connectors():
        con = _connect()
        try:
            result = await connector.probe(_fetch(con))
        finally:
            con.close()
        probes.append(
            ExtensionProbe(
                key=connector.key or connector.source_type,
                source_type=connector.source_type,
                extension=connector.extension or "",
                available=result.available,
                reason=result.reason,
                remediation=result.remediation,
            )
        )
    return probes
