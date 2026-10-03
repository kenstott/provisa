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

    def _new_runtime(self) -> Any:
        from provisa.federation.engine import configured_engine_url

        url = configured_engine_url()
        if url:
            return ClickHouseFederationRuntime.from_url(url)
        # No URL configured → embedded chdb (in-process, no server).
        return ClickHouseFederationRuntime.embedded()

    def replica_target(self, state: Any, *, address: Any, args: Any, engine: Any) -> Any:
        """A replica in this engine's own ClickHouse store: Arrow batches into a build table,
        exchanged with the replica (REQ-1915)."""
        del engine
        from provisa.federation.replica_target import ClickHouseStoreTarget

        runtime = self._runtime_for(state)
        return ClickHouseStoreTarget(
            runtime._backend,
            runtime._land_guard.run,
            schema=address.schema,
            table=address.table,
            columns=args.columns,
            pk_columns=list(args.pk_columns or ()),
        )

    result_formats = frozenset({"parquet"})

    def ctas_redirect(
        self, state: Any, physical_sql: str, output_format: str, params: list | None
    ) -> dict:
        """REQ-1194: ClickHouse runs the statement and writes its result to the results bucket
        itself (``INSERT INTO FUNCTION s3(...) SELECT``). The runtime takes no bound values, so
        the statement's are written into it as literals, in ``$N`` order."""
        from provisa.compiler.params import _sql_literal, substitute_positional_placeholders
        from provisa.executor import redirect
        from provisa.federation import result_sink

        result_sink.require_format(self.engine.name, output_format, self.result_formats)
        config = redirect.RedirectConfig.from_env()
        redirect.ensure_results_bucket_sync(config)
        target = result_sink.new_target(config)
        bound = list(params or [])
        select_sql = substitute_positional_placeholders(
            physical_sql, bound, lambda i: _sql_literal(bound[i - 1])
        )
        clickhouse = self._runtime_for(state).connection
        clickhouse.command(result_sink.clickhouse_insert(select_sql, target, config))
        counted, _names = clickhouse.query(result_sink.clickhouse_count(target, config))
        return {"s3_prefix": target.s3_prefix, "row_count": int(counted[0][0])}

    # -- engine-specific Arrow transports (REQ-986) ----------------------------
    # ClickHouse honors its declared ARROW / ARROW_STREAM capabilities: the runtime returns native
    # Arrow (query_arrow over HTTP, chdb ArrowStream) with no row materialization, mirroring
    # TrinoBackend.execute_arrow / execute_stream. The SQL is already ClickHouse-dialect (transpiled
    # by the backend seam, like execute_sync). The native-TCP backend has no Arrow format and raises.

    def execute_arrow(self, state: Any, sql: str, params: list | None = None) -> Any:
        return self._runtime_for(state).run_arrow(sql)

    def execute_stream(self, state: Any, sql: str, params: list | None = None) -> Any:
        return self._runtime_for(state).run_arrow_stream(sql)
