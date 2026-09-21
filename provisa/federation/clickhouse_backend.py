# Copyright (c) 2026 Kenneth Stott
# Canary: 1b9aa493-129c-4569-a722-2a74743e28a4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""ClickHouseBackend — the ClickHouse engine's in-process terminal. All lifecycle lives in
NativeEngineBackend; this subclass supplies the ClickHouseFederationRuntime."""

from __future__ import annotations

from typing import Any

from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime
from provisa.federation.native_backend import NativeEngineBackend


class ClickHouseBackend(NativeEngineBackend):
    """Every registered source mounts (via a ClickHouse integration/table engine) into ONE runtime;
    governed physical SQL runs against it. The runtime is a server (``clickhouse://``) or embedded
    chdb (``chdb://`` / default) per the configured engine URL."""

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
        same fix as ``PgBackend`` (REQ-1730/REQ-1633): there is no DuckDB-style mangled-name+
        separate-view indirection to redirect a MATERIALIZE_ONLY source through here.
        ``attach_landed_source``/``land_table`` (``ClickHouseFederationRuntime``, REQ-1633's own
        gap — ClickHouse had neither before this) address the landed table directly at
        ``{catalog}_{schema_name}``.``table_name``, the SAME fold ``ClickHouseFederationRuntime.
        attach_source`` uses for a live-attached source: real ClickHouse has no catalog/schema
        split at all (verified live, REQ-1730 — a literal 3-part reference is a SYNTAX_ERROR), so
        ``build_clickhouse_engine``'s own ``catalog_qualified=False`` (this same change) makes the
        compiler fold the per-source catalog into the schema half of every compiled reference
        (``sql_rewrite.fold_catalog_into_schema``) rather than keep it 3-part or drop it —
        preserving the catalog is what keeps two MATERIALIZED sources sharing a native
        ``schema_name`` (e.g. two elasticsearch sources both reporting "default") from colliding
        once ClickHouse's flat namespace is all that's left to address with. The base
        ``EngineBackend`` default (a mangled name under the ``store_schema`` this function
        receives) would send ``land_table``'s row LOAD to a schema nothing ever DDL-reconciles."""
        del store_schema, source_type  # never chooses a different table
        from provisa.compiler.naming import source_to_catalog

        return f"{source_to_catalog(source_id)}_{schema_name}", table_name

    def _new_runtime(self) -> Any:
        from provisa.federation.engine import configured_engine_url

        url = configured_engine_url()
        if url:
            return ClickHouseFederationRuntime.from_url(url)
        # No URL configured → embedded chdb (in-process, no server).
        return ClickHouseFederationRuntime.embedded()

    # -- engine-specific Arrow transports (REQ-986) ----------------------------
    # ClickHouse honors its declared ARROW / ARROW_STREAM capabilities: the runtime returns native
    # Arrow (query_arrow over HTTP, chdb ArrowStream) with no row materialization, mirroring
    # TrinoBackend.execute_arrow / execute_stream. The SQL is already ClickHouse-dialect (transpiled
    # by the backend seam, like execute_sync). The native-TCP backend has no Arrow format and raises.

    def execute_arrow(self, state: Any, sql: str, params: list | None = None) -> Any:
        return self._runtime_for(state).run_arrow(sql)

    def execute_stream(self, state: Any, sql: str, params: list | None = None) -> Any:
        return self._runtime_for(state).run_arrow_stream(sql)
