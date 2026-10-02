# Copyright (c) 2026 Kenneth Stott
# Canary: 638baac1-9f2b-4a54-8232-aeca384b61c6
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

import logging
from typing import Any

import psycopg2

from provisa.federation.engine import UnreachableSource
from provisa.federation.native_backend import NativeEngineBackend
from provisa.federation.pg_runtime import PgFederationRuntime

_log = logging.getLogger(__name__)


class PgBackend(NativeEngineBackend):
    """Every registered source ATTACHes (via FDW) into ONE PostgreSQL connection; governed physical
    SQL runs against it. The engine runs on the configured ``federation_engine_url`` Postgres, else the
    platform database (its declared default store)."""

    # UnreachableSource must stay in this tuple (base default in NativeEngineBackend) — REQ-841/REQ-947:
    # a leftover source of an unreachable type must be skipped here, not raised uncaught into an
    # unrelated later query's attach pass (see duckdb_backend.py for the observed failure mode).
    _attach_errors = (psycopg2.Error, KeyError, UnreachableSource)

    def replica_engine(
        self, state: Any, source: Any, table: Any, *, address: Any, args: Any
    ) -> Any:
        """A full-refresh replica of a table this engine reaches through postgres_fdw is copied
        by the engine itself, as one statement; any other is streamed into the store."""
        from provisa.core.change_signal import REPLACE, select_landing_shape
        from provisa.federation.replica_parties import PgStatementCopy

        if select_landing_shape(args.change_signal, args.watermark_column) != REPLACE:
            return super().replica_engine(state, source, table, address=address, args=args)
        merged = self._merged_source(source, table.schema_name, table.table_name)
        if "server_ddl_for_copy" not in self._runtime_for(state)._engine.resolve(merged).details:
            return super().replica_engine(state, source, table, address=address, args=args)
        return PgStatementCopy(
            self,
            state,
            merged,
            address=address,
            columns=args.columns,
            pk_columns=list(args.pk_columns or ()),
        )

    def replica_target(self, state: Any, *, address: Any, args: Any, engine: Any) -> Any:
        """A replica in this engine's own PostgreSQL database: the one PostgreSQL write face
        (the held COPY), which also takes the engine's own statement-level copy."""
        from provisa.federation.data_replicator import EngineRun
        from provisa.federation.replica_parties import store_target

        return store_target(
            "postgresql",
            self._runtime_for(state)._engine_dsn,
            address=address,
            columns=args.columns,
            pk_columns=list(args.pk_columns or ()),
            engine_writes_store=EngineRun.STATEMENT in engine.caps.runs,
        )

    def transpile_physical(self, pg_sql: str) -> str:  # REQ-902
        """Postgres physical SQL, then collapse JSON_OBJECT colon syntax into flat json_build_object so
        nested-relationship queries survive pg_duckdb's transparent DuckDB execution path (REQ-902).
        json_build_object is valid in plain Postgres too, so this is unconditional for the pg engine."""
        from provisa.transpiler.transpile import rewrite_json_object_to_build_object, transpile

        return rewrite_json_object_to_build_object(transpile(pg_sql, self.dialect))

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
