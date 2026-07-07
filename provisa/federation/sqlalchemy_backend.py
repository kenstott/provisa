# Copyright (c) 2026 Kenneth Stott
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
