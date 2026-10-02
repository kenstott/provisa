# Copyright (c) 2026 Kenneth Stott
# Canary: 32a4fb3c-3412-461e-8bb4-3b66246f18fa
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""DatabricksBackend — the Databricks engine's terminal (REQ-987). Lifecycle lives in
NativeEngineBackend; this subclass supplies the DatabricksFederationRuntime bound to the engine URL,
and its dialect is the Databricks SQL dialect (transpile target)."""

from __future__ import annotations

from typing import Any

from provisa.federation.native_backend import NativeEngineBackend


class DatabricksBackend(NativeEngineBackend):
    """A self-only MPP warehouse: sources land into Databricks and governed SQL runs against it, with
    Arrow-native read transport (execute_arrow/execute_stream via NativeEngineBackend → runtime)."""

    @property
    def dialect(self) -> str:
        return "databricks"

    def _new_runtime(self) -> Any:
        from provisa.federation.databricks_runtime import DatabricksFederationRuntime
        from provisa.federation.engine import configured_engine_url

        url = configured_engine_url()
        if not url:
            raise RuntimeError("databricks engine requires a URL ($PROVISA_ENGINE_URL)")
        return DatabricksFederationRuntime(url=url)

    def replica_target(self, state: Any, *, address: Any, args: Any, engine: Any) -> Any:
        """A replica in this engine's own Unity Catalog: Parquet batches streamed to a volume of
        the replicas schema, then the replica overwritten from them in one commit (REQ-1915)."""
        del engine
        from provisa.federation.replica_target_warehouse import DatabricksStoreTarget

        runtime = self._runtime_for(state)
        return DatabricksStoreTarget(
            runtime._open,
            catalog=runtime.ensure_materialize_attached(),
            schema=address.schema,
            table=address.table,
            columns=args.columns,
            pk_columns=list(args.pk_columns or ()),
        )
