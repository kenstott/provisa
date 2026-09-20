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

from typing import Any

import psycopg2

from provisa.federation.engine import UnreachableSource
from provisa.federation.native_backend import NativeEngineBackend
from provisa.federation.pg_runtime import PgFederationRuntime


class PgBackend(NativeEngineBackend):
    """Every registered source ATTACHes (via FDW) into ONE PostgreSQL connection; governed physical
    SQL runs against it. The engine runs on the configured ``federation_engine_url`` Postgres, else the
    platform database (its declared default store)."""

    # UnreachableSource must stay in this tuple (base default in NativeEngineBackend) — REQ-841/REQ-947:
    # a leftover source of an unreachable type must be skipped here, not raised uncaught into an
    # unrelated later query's attach pass (see duckdb_backend.py for the observed failure mode).
    _attach_errors = (psycopg2.Error, KeyError, UnreachableSource)

    def landing_target(
        self,
        *,
        store_schema: str,
        source_id: str,
        source_type: Any,
        schema_name: str,
        table_name: str,
    ) -> tuple[str, str]:
        """EVERY MATERIALIZED source's replica lands at its REGISTERED address — same reasoning and
        same fix as ``SqlAlchemyBackend``'s own override (REQ-1730): there is no DuckDB-style
        mangled-name+separate-view indirection to redirect a MATERIALIZE_ONLY source through here,
        so the landing address is the physical address. Verified live (2026-09-20): the base
        ``EngineBackend`` default (a mangled ``source_id__schema__table`` name under
        ``store_schema``) left the compiler querying a schema/table that was never created.

        UNLIKE ``SqlAlchemyBackend`` (a genuinely fixed, single catalog), the schema here is
        ``{catalog}_{schema_name}``, not bare ``schema_name``: this engine's own
        ``catalog_qualified=False`` makes the compiler's ENGINE route fold the catalog into the
        schema (``sql_rewrite.fold_catalog_into_schema``) instead of stripping it, because — unlike
        the DIRECT route's single live-attached source (``PgFederationRuntime.attach_source``,
        genuinely redundant catalog there) — the catalog here is the ONLY thing that keeps two
        MATERIALIZED sources sharing the same native ``schema_name`` (e.g. two elasticsearch-type
        sources both reporting "default") from colliding once real Postgres's single schema
        namespace is all that's left to address with. Must produce EXACTLY what
        ``fold_catalog_into_schema`` derives for the compiled query to resolve.

        KNOWN GAP: recomputes the bare per-source catalog (``source_to_catalog``), not the actual
        org-scoped one (``state.source_catalogs``, potentially ``org_prefixed_catalog``-wrapped) —
        this function has no ``state``/org context to read that from. Correct for the default org
        (where org-prefixing is a no-op, verified live); a non-default org would need this plumbed
        through, same class of limitation as the schema-collision risk itself — not fixed here."""
        del store_schema, source_type  # never chooses a different table, only the schema prefix
        from provisa.compiler.naming import source_to_catalog

        return f"{source_to_catalog(source_id)}_{schema_name}", table_name

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
