# Copyright (c) 2026 Kenneth Stott
# Canary: 9e58263f-5b00-49d7-a1eb-7ad64ffd9d07
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SnowflakeBackend — the Snowflake engine's terminal (REQ-988). Lifecycle lives in
NativeEngineBackend; this subclass supplies the SnowflakeFederationRuntime bound to the engine URL,
and its dialect is the Snowflake SQL dialect (transpile target)."""

from __future__ import annotations

from typing import Any

from provisa.federation.native_backend import NativeEngineBackend


class SnowflakeBackend(NativeEngineBackend):
    """A self-only MPP warehouse: sources land into Snowflake and governed SQL runs against it, with
    Arrow-native read transport (execute_arrow/execute_stream via NativeEngineBackend → runtime)."""

    @property
    def dialect(self) -> str:
        return "snowflake"

    def _new_runtime(self) -> Any:
        from provisa.federation.engine import configured_engine_url
        from provisa.federation.snowflake_runtime import SnowflakeFederationRuntime

        url = configured_engine_url()
        if not url:
            raise RuntimeError("snowflake engine requires a URL ($PROVISA_ENGINE_URL)")
        return SnowflakeFederationRuntime(url=url)

    result_formats = frozenset({"parquet"})

    def ctas_redirect(
        self, state: Any, physical_sql: str, output_format: str, params: list | None
    ) -> dict:
        """REQ-1194: Snowflake runs the statement and unloads its result to the results bucket
        itself (``COPY INTO 's3://...' FROM (query)``); its result row is the rows unloaded."""
        from provisa.executor import redirect
        from provisa.federation import result_sink

        result_sink.require_format(self.engine.name, output_format, self.result_formats)
        config = redirect.RedirectConfig.from_env()
        redirect.ensure_results_bucket_sync(config)
        target = result_sink.new_target(config)
        unloaded = (
            self._runtime_for(state)
            .run_sync(result_sink.snowflake_copy(physical_sql, target, config), params)
            .rows
        )
        return {"s3_prefix": target.s3_prefix, "row_count": int(unloaded[0][0])}

    def replica_target(self, state: Any, *, address: Any, args: Any, engine: Any) -> Any:
        """A replica in this engine's own Snowflake database: Arrow batches ingested into a
        build table, then the replica overwritten from it in one statement (REQ-1915)."""
        del engine
        from provisa.federation.replica_target_warehouse import (
            SnowflakeStoreTarget,
            snowflake_adbc_connect,
        )

        runtime = self._runtime_for(state)
        url = runtime._url
        database = runtime.ensure_materialize_attached()
        return SnowflakeStoreTarget(
            lambda: snowflake_adbc_connect(url, database),
            database=database,
            schema=address.schema,
            table=address.table,
            columns=args.columns,
            pk_columns=list(args.pk_columns or ()),
        )
