# Copyright (c) 2026 Kenneth Stott
# Canary: bb2d339d-3016-4e08-bf18-26c31caae766
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Neo4j direct source driver (GitHub issue #119 / REQ-1668's DIRECT-route counterpart).

Same ``execute(query_text, params) -> QueryResult`` shape as the SQL drivers (see
``provisa/executor/drivers/clickhouse.py``), but ``query_text`` here is Cypher, not SQL — this
driver is only ever reached for a single-source Cypher-translatable pattern (see
``provisa/pgwire/_pipeline.py``'s ``_govern_and_route_compiled_planned``, which reverse-compiles
the already-GOVERNED semantic SQL back into Cypher via ``provisa.nl.runner.best_effort_cypher_for_sql``
before it ever reaches this driver — masking/RLS are therefore already baked into the Cypher text).

POSTs to the same ``/db/<database>/tx/commit`` transaction endpoint the materialize path
(``provisa/neo4j/persist.py`` / ``provisa/neo4j/source.py``) and ``provisa/api_source/caller.py``'s
``neo4j_tx`` body encoding already use — one wire convention for both the materialize path and this
direct path.
"""

from __future__ import annotations

from typing import Any

from provisa.executor.drivers.base import DirectDriver
from provisa.executor.result import QueryResult


class Neo4jDriver(DirectDriver):
    def __init__(self) -> None:
        self._base_url: str | None = None
        self._database: str = "neo4j"
        self._auth: tuple[str, str] | None = None

    async def connect(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        min_pool: int = 1,  # pyright: ignore[reportUnusedParameter]
        max_pool: int = 5,  # pyright: ignore[reportUnusedParameter]
    ) -> None:
        # No persistent connection: each execute() opens its own short-lived HTTP request, same as
        # the materialize path's api_source caller — there is no native connection/pool to hold.
        self._base_url = f"http://{host}:{port}"
        self._database = database or "neo4j"
        self._auth = (user, password) if user else None

    async def execute(self, sql: str, params: list | None = None) -> QueryResult:
        """``sql`` is actually Cypher (see module docstring). Positional params are addressed in
        the Cypher text as ``$1``, ``$2``, ... (the same spelling SQLGlot's postgres dialect
        renders a positional bind parameter as, and also valid Cypher parameter syntax — see the
        reverse-compiler's docstring for why no placeholder rewriting is needed between the two)."""
        import httpx

        assert self._base_url is not None
        url = f"{self._base_url}/db/{self._database}/tx/commit"
        cypher_params = {str(i): v for i, v in enumerate(params or [], start=1)}
        body: dict[str, Any] = {"statements": [{"statement": sql, "parameters": cypher_params}]}
        request_kwargs: dict[str, Any] = {"json": body, "timeout": 60.0}
        if self._auth is not None:
            request_kwargs["auth"] = self._auth
        async with httpx.AsyncClient() as client:
            resp = await client.post(url, **request_kwargs)
            resp.raise_for_status()
            response = resp.json()

        errors = response.get("errors") or []
        if errors:
            raise RuntimeError(f"neo4j query failed: {errors}")

        column_names: list[str] = []
        rows: list[tuple] = []
        for result in response.get("results") or []:
            column_names = list(result.get("columns") or [])
            for entry in result.get("data") or []:
                rows.append(tuple(entry.get("row") or []))
        return QueryResult(rows=rows, column_names=column_names)

    async def close(self) -> None:
        self._base_url = None

    @property
    def is_connected(self) -> bool:
        return self._base_url is not None
