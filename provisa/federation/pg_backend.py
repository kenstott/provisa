# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PgBackend — the PostgreSQL engine's in-process terminal. All lifecycle lives in NativeEngineBackend;
this subclass supplies the PgFederationRuntime and the psycopg driver error type."""

from __future__ import annotations

from typing import Any

import psycopg2

from provisa.federation.native_backend import NativeEngineBackend
from provisa.federation.pg_runtime import PgFederationRuntime


class PgBackend(NativeEngineBackend):
    """Every registered source ATTACHes (via FDW) into ONE PostgreSQL connection; governed physical
    SQL runs against it. The engine runs on the configured ``federation_engine_url`` Postgres, else the
    platform database (its declared default store)."""

    _attach_errors = (psycopg2.Error, KeyError)

    def _new_runtime(self) -> Any:
        from provisa.federation.engine import configured_engine_url

        raw = configured_engine_url() or self.engine.default_materialize_store()
        if raw is None:
            raise RuntimeError(
                "pg engine requires a Postgres URL (federation_engine_url) or a platform database"
            )
        # libpq/psycopg2 want a driver-agnostic DSN (strip a SQLAlchemy '+driver' suffix).
        scheme, sep, rest = raw.partition("://")
        dsn = f"{scheme.split('+', 1)[0]}://{rest}" if sep else raw
        return PgFederationRuntime(engine_dsn=dsn)
