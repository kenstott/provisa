# Copyright (c) 2026 Kenneth Stott
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
