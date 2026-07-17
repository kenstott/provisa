# Copyright (c) 2026 Kenneth Stott
# Canary: 625ca81d-bd7a-4c2d-bd62-4a5f71ebe9bb
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""BigQuery direct source driver.

Makes BigQuery a first-class NAMED SOURCE reachable on ANY engine: Provisa reads it directly then
lands a replica. The connection is PER-SOURCE — the project and the service-account credentials come
from the source config (``database`` = project; ``federation_hints`` carries the SA key path or inline
JSON, and the location) — so two BigQuery sources from different projects/accounts coexist, no shared
env. Falls back to Application Default Credentials when the source carries no explicit key.
"""

from __future__ import annotations

import asyncio
from typing import Any

from provisa.executor.drivers.base import DirectDriver
from provisa.executor.result import QueryResult


class BigQueryDriver(DirectDriver):
    def __init__(self) -> None:
        self._client: Any = None
        self._extra: dict[str, str] = {}

    def configure(self, extra: dict[str, str]) -> None:
        """Per-source auth: ``credentials_path`` (SA key file) or ``credentials_json`` (inline), and an
        optional ``location``. Absent → Application Default Credentials."""
        self._extra = dict(extra)

    async def connect(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        host: str,
        port: int,  # pyright: ignore[reportUnusedParameter]
        database: str,
        user: str,  # pyright: ignore[reportUnusedParameter]
        password: str,  # pyright: ignore[reportUnusedParameter]
        min_pool: int = 1,  # pyright: ignore[reportUnusedParameter]
        max_pool: int = 5,  # pyright: ignore[reportUnusedParameter]
    ) -> None:
        project = database or host
        if not project:
            raise ValueError("bigquery source requires a project (source database)")

        def _open() -> Any:
            from google.cloud import bigquery

            credentials = None
            if self._extra.get("credentials_path"):
                from google.oauth2 import service_account

                credentials = service_account.Credentials.from_service_account_file(
                    self._extra["credentials_path"]
                )
            elif self._extra.get("credentials_json"):
                import json

                from google.oauth2 import service_account

                credentials = service_account.Credentials.from_service_account_info(
                    json.loads(self._extra["credentials_json"])
                )
            return bigquery.Client(
                project=project, credentials=credentials, location=self._extra.get("location", "US")
            )

        self._client = await asyncio.to_thread(_open)

    async def execute(self, sql: str, params: list | None = None) -> QueryResult:
        del params  # BigQuery read SQL arrives fully formed from the governed pipeline

        def _run() -> QueryResult:
            it = self._client.query(sql).result()
            cols = [f.name for f in it.schema]
            rows = [tuple(row.values()) for row in it]
            return QueryResult(rows=rows, column_names=cols)

        return await asyncio.to_thread(_run)

    async def close(self) -> None:
        if self._client is not None:
            await asyncio.to_thread(self._client.close)
            self._client = None

    @property
    def is_connected(self) -> bool:
        return self._client is not None
