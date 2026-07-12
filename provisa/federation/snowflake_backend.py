# Copyright (c) 2026 Kenneth Stott
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
