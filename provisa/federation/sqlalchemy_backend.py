# Copyright (c) 2026 Kenneth Stott
# Canary: 0cd5ec3d-5940-45c1-93cd-a25e66918997
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SqlAlchemyBackend — the self-only SQLAlchemy engine's terminal. Lifecycle lives in
NativeEngineBackend; this subclass supplies the SqlAlchemyFederationRuntime bound to the engine URL."""

from __future__ import annotations

from typing import Any

from provisa.federation.native_backend import NativeEngineBackend
from provisa.federation.sqlalchemy_runtime import SqlAlchemyFederationRuntime


class SqlAlchemyBackend(NativeEngineBackend):
    """A self-only warehouse: every source lands into the store defined by the SQLAlchemy URL, and
    governed SQL runs against it."""

    def _new_runtime(self) -> Any:
        from provisa.federation.engine import configured_engine_url

        url = configured_engine_url()
        if not url:
            raise RuntimeError("sqlalchemy engine requires a URL ($PROVISA_ENGINE_URL)")
        return SqlAlchemyFederationRuntime(url=url)

    def landing_target(
        self,
        *,
        store_schema: str,
        source_id: str,
        source_type: Any,
        schema_name: str,
        table_name: str,
    ) -> tuple[str, str]:
        """EVERY MATERIALIZED source's replica lands at its REGISTERED address (``schema_name``,
        ``table_name``) — unconditionally, unlike ``TrinoBackend``'s own override (which checks
        ``is_adapter_fetched`` because TRINO has real live connectors for SOME source types and
        needs to tell those apart from the adapter-only ones). This engine is genuinely SELF_ONLY
        (REQ-905's ``_warehouse_connectors`` gives it zero live/ATTACH connectors for ANYTHING —
        confirmed via ``build_sqlalchemy_engine``), so there is never a live mirror to distinguish
        from: `is_adapter_fetched` answers "does this SOURCE TYPE ever have a live mirror on ANY
        engine" (elasticsearch/mongodb/redis/cassandra answer NO to that question only because
        Trino happens to have connectors for them — an unrelated engine's capability), not "does
        THIS engine have one," so checking it here silently fell through to the base mangled-name
        default for exactly those types. Verified live (REQ-1730 engine-swap harness,
        2026-09-20): elasticsearch's rows landed correctly at the mangled name, but the compiler
        queried the registered ``schema_name.table_name`` address (via ``fixed_catalog_for``'s one
        shared catalog) — genuinely different tables, so the query saw 0 rows with no error.

        Same reasoning as ``fixed_catalog_for``'s own doc: this engine has no DuckDB-style
        ATTACH+view layer to redirect a mangled ``mat``-schema name back to the physical name the
        compiler emits, so for EVERY source here (not just adapter-only ones) the landing address
        IS the physical address."""
        del (
            store_schema,
            source_id,
            source_type,
        )  # unconditional: never chooses an alternate address
        return schema_name, table_name
