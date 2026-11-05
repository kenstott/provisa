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

    def _driver_errors(
        self,
    ) -> tuple[tuple[type[BaseException], ...], tuple[type[BaseException], ...]]:
        """REQ-778: BigQuery answers a statement it refuses with 400 (BadRequest) and an
        unavailable service with 5xx or a transport error."""
        import google.api_core.exceptions as gapi

        return (
            (
                gapi.ServiceUnavailable,
                gapi.InternalServerError,
                gapi.GatewayTimeout,
                ConnectionError,
            ),
            (gapi.BadRequest,),
        )

    @property
    def dialect(self) -> str:
        return "bigquery"

    def _new_runtime(self) -> Any:
        from provisa.federation.bigquery_runtime import BigQueryFederationRuntime
        from provisa.federation.engine import configured_engine_url

        # URL is optional — project/location fall back to $GOOGLE_CLOUD_PROJECT / $BIGQUERY_LOCATION.
        return BigQueryFederationRuntime(url=configured_engine_url())

    def replica_indexer(self, state: Any, *, address: Any) -> Any:
        """REQ-2015: none. This engine's replicas live in its own warehouse (``replica_target``):
        a BigQuery table is ordered by its own partitioning and clustering, and has no secondary index to build."""
        del state, address
        return None

    def replica_target(self, state: Any, *, address: Any, args: Any, engine: Any) -> Any:
        """A replica in this project's replicas dataset: Arrow batches through one pending
        Storage Write stream into a build table, then the replica's rows replaced from it in
        one statement (REQ-1915)."""
        del engine
        from provisa.federation.replica_target_warehouse import BigQueryStoreTarget

        runtime = self._runtime_for(state)
        return BigQueryStoreTarget(
            runtime._client,
            project=runtime.project,
            dataset=address.schema,
            table=address.table,
            columns=args.columns,
            pk_columns=list(args.pk_columns or ()),
        )
