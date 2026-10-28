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
import psycopg2.errors

from provisa.core.read_refusal import ReadRefused
from provisa.federation.native_backend import NativeEngineBackend
from provisa.federation.pg_runtime import PgFederationRuntime

_log = logging.getLogger(__name__)


class TryCastNeedsPostgres16(ReadRefused):  # REQ-778
    """toInteger, toFloat or toBoolean of a value that may not convert, on a PostgreSQL engine
    older than 16: it has no ``pg_input_is_valid``, which the null-on-failure cast is written by."""

    code = "cypher.try_cast_needs_postgres_16"

    def __init__(self) -> None:
        super().__init__(
            "toInteger, toFloat and toBoolean of a value that may not convert need PostgreSQL 16 "
            "or later on this engine"
        )
        self.params = {}


class PgBackend(NativeEngineBackend):
    """Every registered source ATTACHes (via FDW) into ONE PostgreSQL connection; governed physical
    SQL runs against it. The engine runs on the configured ``federation_engine_url`` Postgres, else the
    platform database (its declared default store)."""

    reads_other_regions = True  # REQ-1922: postgres_fdw import from that region's store

    # UnreachableSource must stay in this tuple (base default in NativeEngineBackend) — REQ-841/REQ-947:
    # a leftover source of an unreachable type must be skipped here, not raised uncaught into an
    # unrelated later query's attach pass (see duckdb_backend.py for the observed failure mode).
    _attach_errors = (psycopg2.Error, KeyError, *NativeEngineBackend._attach_errors)

    #: REQ-1195: Postgres writes a result file in CSV (``COPY ... TO STDOUT``).
    local_result_formats = frozenset({"csv"})

    def refusal_of(self, exc: Exception) -> ReadRefused | None:
        """REQ-778: a null-on-failure cast is written ``pg_input_is_valid`` for PostgreSQL
        (``transpile.postgres_try_casts``), which a server older than 16 does not have."""
        if isinstance(exc, psycopg2.errors.UndefinedFunction) and "pg_input_is_valid" in str(exc):
            return TryCastNeedsPostgres16()
        return None

    def _driver_errors(
        self,
    ) -> tuple[tuple[type[BaseException], ...], tuple[type[BaseException], ...]]:
        """REQ-778: psycopg2's own (PEP 249)."""
        from provisa.federation.dbapi_errors import dbapi_errors

        return dbapi_errors(psycopg2)

    def write_result_file(
        self, state: Any, physical_sql: str, output_format: str, params: list | None, path: str
    ) -> int:
        """REQ-1195: Postgres runs the statement and its CSV result is streamed into ``path``."""
        from provisa.federation import result_sink

        result_sink.require_format(self.engine.name, output_format, self.local_result_formats)
        return self._runtime_for(state).write_result_csv(physical_sql, params, path)

    def replica_engine(
        self, state: Any, source: Any, table: Any, *, address: Any, args: Any
    ) -> Any:
        """A full-refresh replica of a table this engine reaches through postgres_fdw is copied
        by the engine itself, as one statement; any other is streamed into the store."""
        from provisa.core.change_signal import REPLACE, select_landing_shape
        from provisa.federation.replica_parties import PgStatementCopy
        from provisa.core.landing_filter import where as landing_where
        from provisa.transpiler.native_json import source_projection

        from provisa.federation.commit_time import SOURCE_COMMITTED_AT

        if select_landing_shape(args.change_signal, args.watermark_column) != REPLACE or any(
            name == SOURCE_COMMITTED_AT for name, _type in args.columns
        ):
            # REQ-1163: a table carrying its commit time is streamed -- its foreign table here
            # cannot see the source row's xmin.
            return super().replica_engine(state, source, table, address=address, args=args)
        merged = self._merged_source(source, table.schema_name, table.table_name)
        merged.catalog = state.source_catalogs[source.id]  # REQ-1266/1529: what it attaches under
        if "server_ddl_for_copy" not in self._runtime_for(state)._engine.resolve(merged).details:
            return super().replica_engine(state, source, table, address=address, args=args)
        return PgStatementCopy(
            self,
            state,
            merged,
            address=address,
            columns=args.columns,
            # REQ-1965: a natively array/struct/map column is copied as PostgreSQL's own JSON;
            # REQ-2171: a subtyped one in its own form, and one not landed left out.
            projection=source_projection(
                table.columns,
                "postgres",
                frozenset(name for name, _subtype, _reader in args.not_landed),
            ),
            where=landing_where(table, lambda: "postgres"),  # REQ-2062
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
            engine_writes_store=EngineRun.STATEMENT in engine.caps.runs,
            native_vectors=True,  # REQ-424: the engine reads its replicas in its own database
        )

    def session_rows(self, state: Any) -> Any:
        """REQ-2176: a session's tables are in this engine's own database, written there in
        place."""
        from provisa.federation.session_rows import SqlAlchemySessionRows

        return SqlAlchemySessionRows(self._runtime_for(state)._engine_dsn)

    def replica_indexer(self, state: Any, *, address: Any) -> Any:
        """REQ-2015: a replica in this engine's own database is indexed there, and read through
        its indexes."""
        from provisa.federation.replica_target import PostgresIndexer

        return PostgresIndexer(
            self._runtime_for(state)._engine_dsn, schema=address.schema, table=address.table
        )

    def indexes_replica_vectors(self, state: Any) -> bool:
        """REQ-424: the engine reads its replicas in its own database, through their indexes."""
        del state
        return True

    @property
    def computes_fakes(self) -> bool:  # type: ignore[override]
        """REQ-1494: the engine's Postgres holds the fake functions (PL/Python), proven when its
        runtime came up; until then, and where it cannot, faked reads are refused by name."""
        runtime = getattr(self, "_runtime", None)
        return runtime is not None and runtime.computes_fakes

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
