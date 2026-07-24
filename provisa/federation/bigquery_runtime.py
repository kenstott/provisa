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

from __future__ import annotations

import json
from typing import Any
from urllib.parse import parse_qs, urlparse

from provisa.core.ir_types import to_ir
from provisa.executor.result import QueryResult
from provisa.executor.result import ResultStream
from provisa.federation.runtime_support import run_async_materialized, stream_rows_from_arrow

# Canonical IR name → BigQuery standard-SQL type (for landed-table DDL / load schema).
_IR_TO_BQ: dict[str, str] = {
    "smallint": "INT64",
    "integer": "INT64",
    "bigint": "INT64",
    "text": "STRING",
    "boolean": "BOOL",
    "float": "FLOAT64",
    "double": "FLOAT64",
    "numeric": "NUMERIC",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
    "time": "TIME",
    "uuid": "STRING",
    "bytea": "BYTES",
    "json": "JSON",
}


def _bq_type(ir_type: str) -> str:
    canonical = to_ir(ir_type)
    t = _IR_TO_BQ.get(canonical)
    if t is None:
        raise ValueError(
            f"no BigQuery type mapping for IR type {ir_type!r} (canonical {canonical!r})"
        )
    return t


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
        (distinct from the per-source landing datasets), created on demand at refresh."""
        return f"org_{org_id}_mv_cache"

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
        """LAND a source into a per-source BigQuery dataset at the compiler-physical name (REQ-987).
        A columnar BigQuery LOAD job (WRITE_TRUNCATE for replace, WRITE_APPEND for a poll+watermark
        delta) — never per-row INSERT. The dataset/table are the physical relation the governed query
        reads directly."""
        del pk_columns
        import asyncio

        from provisa.core.change_signal import APPEND, select_landing_shape

        project, dataset, table = self._phys_parts(source)
        append = select_landing_shape(change_signal, watermark_column) == APPEND
        await asyncio.to_thread(self._load, dataset, table, columns, rows, append)

    def _load(self, dataset: str, table: str, columns, rows: list[dict], append: bool) -> None:
        from google.cloud import bigquery

        self._ensure_dataset(dataset)
        schema = [bigquery.SchemaField(n, _bq_type(t)) for n, t in columns]
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
            # Create/truncate the empty table so the catalog is complete even with no rows.
            self._client.query(
                f"CREATE TABLE IF NOT EXISTS `{self._project}`.`{dataset}`.`{table}` "
                f"({', '.join(f'`{n}` {_bq_type(t)}' for n, t in columns)})"
            ).result()
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
