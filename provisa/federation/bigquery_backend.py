# Copyright (c) 2026 Kenneth Stott
# Canary: c8dfe8d7-7a39-4b09-947e-ff5a69caa6c6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""BigQueryBackend — the BigQuery engine's terminal. Lifecycle lives in NativeEngineBackend; this
subclass supplies the BigQueryFederationRuntime and the BigQuery SQL dialect (transpile target)."""

from __future__ import annotations

from typing import Any

from provisa.federation.native_backend import NativeEngineBackend


class BigQueryBackend(NativeEngineBackend):
    """A partial-federator warehouse: object/lake sources attach as external tables (SCAN), the rest
    land, and governed SQL runs against BigQuery with Arrow-native reads (Storage Read API)."""

    @property
    def dialect(self) -> str:
        return "bigquery"

    def landing_target(
        self,
        *,
        store_schema: str,
        source_id: str,
        source_type: Any,
        schema_name: str,
        table_name: str,
    ) -> tuple[str, str]:
        """EVERY MATERIALIZED source's replica lands at its REGISTERED address — same reasoning and
        same fix as ``PgBackend``/``SqlAlchemyBackend`` (REQ-1730): there is no DuckDB-style
        mangled-name+separate-view indirection to redirect a MATERIALIZE_ONLY source through here.
        ``attach_landed_source`` (``BigQueryFederationRuntime``, wired at boot via
        ``reconcile_landed_tables``) already DDL-reconciles the table at exactly
        ``source.schema_name``/``source.table_name`` (its ``_phys_parts`` reads nothing else); the
        base ``EngineBackend`` default (a mangled ``source_id__schema__table`` name under a landing
        schema) sent ``land_table``'s actual row LOAD to a dataset/table BigQuery never DDL-reconciled
        and the compiled query never reads. Verified live (REQ-1730 engine-swap harness,
        2026-09-20): a cassandra source rebooted into BigQuery queried 0 rows with no error — the
        DDL reconcile created the right table, but every landed row went to the mangled address
        instead. Unlike ``PgBackend``, no catalog-fold is needed: BigQuery's project already is the
        catalog (``catalog_qualified`` stays the engine default, True), so schema/table alone
        (dataset/table) is the complete, unambiguous physical address."""
        del store_schema, source_id, source_type  # never chooses a different table
        return schema_name, table_name

    def _new_runtime(self) -> Any:
        from provisa.federation.bigquery_runtime import BigQueryFederationRuntime
        from provisa.federation.engine import configured_engine_url

        # URL is optional — project/location fall back to $GOOGLE_CLOUD_PROJECT / $BIGQUERY_LOCATION.
        return BigQueryFederationRuntime(url=configured_engine_url())
