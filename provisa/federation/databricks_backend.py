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

    def landing_target(
        self,
        *,
        store_schema: str,
        source_id: str,
        source_type: Any,
        schema_name: str,
        table_name: str,
    ) -> tuple[str, str]:
        """EVERY MATERIALIZED source's replica lands at its REGISTERED address, same reasoning as
        ``BigQueryBackend``/``SnowflakeBackend`` (REQ-1730) — but Databricks additionally lands each
        source into its OWN per-source Unity Catalog (``DatabricksFederationRuntime._phys_parts``'s
        own docstring; ``engine.fixed_catalog_for`` returns ``None`` for ``"databricks"``, confirming
        the compiler resolves a per-source catalog here too, unlike BigQuery/Snowflake's ONE fixed
        one). ``land_table``'s hook signature only carries plain ``(schema, table)`` strings — no
        source id — so the catalog is folded into the ``schema`` half here (NUL-joined; never a
        legal identifier character, so it can never collide with a real schema name) and unpacked
        by ``DatabricksFederationRuntime.land_table``, which calls ``_to_catalog_name`` the identical
        way ``_phys_parts`` does so both agree on the exact same physical catalog."""
        del store_schema, source_type  # never chooses a different table
        from provisa.core.catalog import _to_catalog_name

        return f"{_to_catalog_name(source_id)}\x00{schema_name}", table_name

    def _new_runtime(self) -> Any:
        from provisa.federation.databricks_runtime import DatabricksFederationRuntime
        from provisa.federation.engine import configured_engine_url

        url = configured_engine_url()
        if not url:
            raise RuntimeError("databricks engine requires a URL ($PROVISA_ENGINE_URL)")
        return DatabricksFederationRuntime(url=url)
