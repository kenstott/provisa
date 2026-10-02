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
        return SqlAlchemyFederationRuntime(url=url, catalog_qualified=self.engine.catalog_qualified)

    def replica_target(self, state: Any, *, address: Any, args: Any, engine: Any) -> Any:
        """A replica in this engine's own store, written through the store's SQLAlchemy engine
        by batched inserts and swapped in by the dialect's atomic rename (REQ-1915)."""
        del engine
        from provisa.federation.replica_target import SqlAlchemyStoreTarget

        return SqlAlchemyStoreTarget(
            self._runtime_for(state)._sa,
            schema=address.schema,
            table=address.table,
            columns=args.columns,
            pk_columns=list(args.pk_columns or ()),
        )
