# Copyright (c) 2026 Kenneth Stott
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

    def _new_runtime(self) -> Any:
        from provisa.federation.bigquery_runtime import BigQueryFederationRuntime
        from provisa.federation.engine import configured_engine_url

        # URL is optional — project/location fall back to $GOOGLE_CLOUD_PROJECT / $BIGQUERY_LOCATION.
        return BigQueryFederationRuntime(url=configured_engine_url())
