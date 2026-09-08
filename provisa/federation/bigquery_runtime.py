# Copyright (c) 2026 Kenneth Stott
# Canary: 03e3c08f-e206-445c-833d-fadfb4d8b4cc
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""BigQueryFederationRuntime — BigQuery as a first-class federation engine (Arrow-native).

A partial federator warehouse: object/lake sources on cloud storage ATTACH as zero-copy external
tables (SCAN); every other readable source LANDs into a per-source BigQuery dataset. Governed SQL runs
against BigQuery in the BigQuery dialect; reads are Arrow-native via the BigQuery Storage Read API
(``to_arrow`` / ``to_arrow_iterable``), so no Python rows are materialized for the Flight transport.

Physical naming: BigQuery is ``project.dataset.table`` (a fixed project + per-source dataset), so the
governed pipeline pins each source's catalog to the project (state.source_catalogs) and its dataset is
the schema — the runtime lands/attaches at exactly that name. Auth is Application Default Credentials
(``GOOGLE_APPLICATION_CREDENTIALS`` service-account key). The client is imported lazily so this module
loads where google-cloud-bigquery is absent.
"""

# Requirements: REQ-1658

from __future__ import annotations

import json
from typing import Any
from urllib.parse import parse_qs, urlparse

from provisa.executor.result import QueryResult
from provisa.executor.result import ResultStream
from provisa.federation.bigquery_store import bq_type
from provisa.federation.runtime_support import run_async_materialized, stream_rows_from_arrow


class BigQueryFederationRuntime:  # REQ — BigQuery federation engine
    def __init__(self, *, url: str | None = None) -> None:
        # bigquery://<project>?location=US  — project/location fall back to the standard GCP env.
        import os

        u = urlparse(url or "")
        q = parse_qs(u.query)
        self._project = u.hostname or os.environ.get("GOOGLE_CLOUD_PROJECT")
        if not self._project:
            raise ValueError(
                "bigquery engine requires a project (bigquery://<project> or $GOOGLE_CLOUD_PROJECT)"
            )
        self._location = q.get("location", [os.environ.get("BIGQUERY_LOCATION", "US")])[0]
        self._engine: Any = None
        from google.cloud import bigquery

        # ADC via GOOGLE_APPLICATION_CREDENTIALS (service-account key) — no secret in the URL.
        self._client = bigquery.Client(project=self._project, location=self._location)

    @property
    def dialect(self) -> str:
        return "bigquery"

    @property
    def project(self) -> str:
        return self._project  # type: ignore[return-value]

    def _engine_for(self) -> Any:
        if self._engine is None:
            from provisa.federation.engine import build_bigquery_engine

            self._engine = build_bigquery_engine()
        return self._engine

    def _phys_parts(self, source: Any) -> tuple[str, str, str]:
        """(project, dataset, table) — the governed physical name. The compiler pins the catalog to
        the project (via state.source_catalogs), the dataset is the source's schema, so a landed or
        externally-linked table sits exactly where the governed query reads it."""
        return self._project, source.schema_name, source.table_name  # type: ignore[return-value]

    # -- source exposure -------------------------------------------------------

    def attach_source(self, source: Any) -> None:
        """Object/lake sources on cloud storage attach as a ZERO-COPY external table (an ``ATTACH_R``
        SCAN); every other source LANDs (materialize_source), so attach is a no-op for it."""
        from provisa.federation.connector_base import LIVE_IN_PLACE

        entry = self._engine_for().resolve(source)
        if (
            entry.mechanism not in LIVE_IN_PLACE
        ):  # attach only what the engine reads in place (REQ-951)
            return None
        from provisa.federation.bigquery_connectors import external_table_ddl

        project, dataset, table = self._phys_parts(source)
        self._ensure_dataset(dataset)
        self._client.query(external_table_ddl(project, dataset, table, entry.details)).result()
        return None

    # -- materialization store -------------------------------------------------

    def ensure_materialize_attached(self) -> str:
        return self._project  # type: ignore[return-value]

    def mv_store_schema(self, org_id: str) -> str:
        """MVs materialize into an org-scoped cache dataset in the project — a dedicated dataset
        (distinct from the per-source landing datasets), created on demand at refresh.

        REQ-1623: scoped to the environment being served, so one environment's refresh does not
        overwrite the rows another environment reads."""
        from provisa.core.environments import active_org_schema

        return active_org_schema(org_id, "_mv_cache")

    @property
    def connection(self):
        return self._client

    def _ensure_dataset(self, dataset: str) -> None:
        from google.cloud import bigquery

        self._client.create_dataset(bigquery.Dataset(f"{self._project}.{dataset}"), exists_ok=True)

    async def materialize_source(
        self,
        source: Any,
        columns: list[tuple[str, str]],
        rows: list[dict],
        *,
        change_signal: str = "ttl",
        watermark_column: str | None = None,
        pk_columns: list[str] | None = None,
    ) -> None:
        """LAND a source into a per-source BigQuery dataset at the compiler-physical name (REQ-987):
        converge the table (``attach_landed_source``), then a columnar BigQuery LOAD job
        (WRITE_TRUNCATE for replace, WRITE_APPEND for a poll+watermark delta) — never per-row
        INSERT. The dataset/table are the physical relation the governed query reads directly."""
        import asyncio

        from provisa.core.change_signal import APPEND, select_landing_shape

        await self.attach_landed_source(source, columns, pk_columns=pk_columns)
        _, dataset, table = self._phys_parts(source)
        append = select_landing_shape(change_signal, watermark_column) == APPEND
        await asyncio.to_thread(self._load, dataset, table, columns, rows, append)

    async def attach_landed_source(
        self, source: Any, columns: list[tuple[str, str]], *, pk_columns: list[str] | None = None
    ) -> str:
        """Eager reconcile (boot / registration, REQ-1658): converge the landed table to
        ``columns`` + ``pk_columns`` (DDL only, no data), so the catalog is complete at startup and
        survives restart. Returns the reconcile outcome."""
        import asyncio

        from provisa.federation.bigquery_store import reconcile_bigquery_native

        parts = self._phys_parts(source)
        return await asyncio.to_thread(
            reconcile_bigquery_native,
            self._client,
            parts=parts,
            columns=columns,
            pk_columns=pk_columns,
        )

    async def reconcile_landed_metadata(self, plan: Any) -> int:
        """Apply the landed model's keys, descriptions and tags (REQ-1658): ``NOT ENFORCED``
        PRIMARY/FOREIGN KEY constraints, table and column descriptions, and ``provisa_governance_*``
        labels on each landed table. No view layer: the landed table is the physical name."""
        import asyncio

        from provisa.federation.bigquery_store import reconcile_metadata_native
        from provisa.federation.landed_keys import plan_targets

        targets = plan_targets(
            plan, replica_for=lambda t: (self._project, t.schema_name, t.table_name)
        )
        return await asyncio.to_thread(
            reconcile_metadata_native,
            self._client,
            targets=targets,
            edges=plan.edges,
            known_tags=plan.known_tags,
        )

    def _load(self, dataset: str, table: str, columns, rows: list[dict], append: bool) -> None:
        from google.cloud import bigquery

        schema = [bigquery.SchemaField(n, bq_type(t)) for n, t in columns]
        cfg = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=(
                bigquery.WriteDisposition.WRITE_APPEND
                if append
                else bigquery.WriteDisposition.WRITE_TRUNCATE
            ),
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )
        ref = f"{self._project}.{dataset}.{table}"
        if not rows:
            # The table already exists (attach_landed_source); a replace with no rows empties it.
            if not append:
                self._client.query(
                    f"TRUNCATE TABLE `{self._project}`.`{dataset}`.`{table}`"
                ).result()
            return
        colnames = [n for n, _ in columns]
        data = "\n".join(json.dumps({c: r.get(c) for c in colnames}, default=str) for r in rows)
        self._client.load_table_from_file(
            __import__("io").BytesIO(data.encode()), ref, job_config=cfg
        ).result()

    # -- execution -------------------------------------------------------------

    def run_sync(self, sql: str, params: list | None = None) -> ResultStream:
        """Execute BigQuery-dialect SQL (transpiled by the backend seam) and STREAM it.

        Built on the lazy ``to_arrow_iterable`` terminal (``run_arrow_stream``) so the pgwire ENGINE
        route stays memory-bounded — no full ``QueryResult`` materialization (REQ-1217, Defect 3)."""
        schema, batches = self.run_arrow_stream(sql, params)
        return stream_rows_from_arrow(schema, batches)

    async def run(self, sql: str, params: list | None = None) -> QueryResult:
        return await run_async_materialized(self.run_sync, sql, params)

    # -- Arrow transport -------------------------------------------------------

    def run_arrow(self, sql: str, params: list | None = None) -> Any:
        """Execute BigQuery-dialect SQL and return a ``pyarrow.Table`` — BigQuery delivers Arrow
        natively via the Storage Read API (``to_arrow``), so no Python rows are materialized."""
        del params
        return self._client.query(sql).to_arrow()

    def run_arrow_stream(self, sql: str, params: list | None = None) -> tuple[Any, Any]:
        """Execute BigQuery-dialect SQL and return ``(schema, batch_generator)`` for lazy record-batch
        streaming through the Flight server's GeneratorStream (REQ-1216, REQ-1217).

        Genuinely lazy: ``RowIterator.to_arrow_iterable`` pulls record batches from the Storage Read API
        on demand, so the full result never materializes — peak memory is bounded by one batch. A
        zero-row result yields an empty-schema stream (column names from the query schema, no rows)."""
        import pyarrow as pa

        del params
        it = self._client.query(sql).result()
        batch_iter = iter(it.to_arrow_iterable())
        first = next(batch_iter, None)
        if first is None:  # zero-row result yields no batches
            names = [f.name for f in it.schema]
            return pa.table({name: [] for name in names}).schema, iter(())
        schema = first.schema

        def _batches():
            yield first
            yield from batch_iter

        return schema, _batches()

    def close(self) -> None:
        self._client.close()
