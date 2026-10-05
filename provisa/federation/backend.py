# Copyright (c) 2026 Kenneth Stott
# Canary: 2a9f5c73-8e1d-4b62-a70f-6c3e9d1a4b58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Per-engine backend: the engine-specific implementation of the runtime seam (REQ-825, REQ-840).

``EngineRuntime`` (runtime.py) is engine-agnostic — it never branches on the engine name. Each
``FederationEngine`` instance carries a ``backend`` that implements the concrete terminal behavior
(execute, dialect, lifecycle, source registration, introspection, error mapping). The Trino
implementation (``TrinoBackend``) is the only backend that references Trino; native in-process
engines (duckdb/pg/clickhouse/sqlalchemy) use the default ``EngineBackend``. This is what keeps
every Trino reference inside the Trino engine's own instance and out of the generic seam.
"""

# complexity-gate: allow-ble=2 reason="introspection augmentation + cluster-health probe are best-effort: any driver/network failure keeps declared types / reports unhealthy, never crashes the seam"

from __future__ import annotations

import asyncio
import logging
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from collections.abc import Iterator

    from provisa.executor.result import QueryResult, ResultStream
    from provisa.executor.session import EngineSession, StoreBrokerSession
    from provisa.federation.engine import FederationEngine
    from provisa.federation.replica_address import ReplicaAddress

_log = logging.getLogger(__name__)


class RegionStoreUnreachable(RuntimeError):
    """Another region's replicas store could not be attached to read from (REQ-1922): the read
    is refused naming the table and its region (``HomeRegionUnavailable``)."""


class EngineReadsNoOtherRegion(RuntimeError):
    """An engine that cannot attach another region's replicas store (REQ-1922)."""

    code = "query.engine_reads_no_other_region"

    def __init__(self, engine: str, region: str) -> None:
        self.params = {"engine": engine, "region": region}
        super().__init__(
            f"the {engine} engine cannot read region {region!r}'s replicas store, so a table kept "
            "there cannot be read from this region on it"
        )


class EngineBackend:
    """Default backend for native in-process engines (duckdb/pg/clickhouse/sqlalchemy).

    A native engine has no external cluster, dynamic catalog, or watchdog, so the lifecycle hooks
    are no-ops. Introspection uses the engine's own native runtime (DuckDB/ClickHouse). The live
    ENGINE-terminal execution binding for native engines is separate feature work — ``execute``
    raises until an engine wires it, rather than silently falling back to another engine.
    """

    #: REQ-1922: whether this engine reads another region's replicas in place
    #: (``region_read_address``). The model's ``engine_kinds.REGION_READERS`` is held equal to it.
    reads_other_regions = False

    # `native_store` (engine.py's generic `_RDB_KINDS` loop) is the SQLAlchemy URL scheme name —
    # the identity used for landing/storage-backend comparisons (materialization.py,
    # query_residency.py) — which is not always SQLGlot's own dialect name for that same product
    # (SUPPORTED_DIALECTS in transpiler/transpile.py enumerates SQLGlot's actual names). Verified
    # live (REQ-1730 engine-swap harness, 2026-09-20): registering the generic "mssql" engine and
    # querying it raised `sqlglot.errors.ParseError: Unknown dialect 'mssql'. Did you mean mysql?`
    # — SQLGlot names Microsoft SQL Server's dialect "tsql", not "mssql". Translate only at this
    # transpile-target seam, never `native_store` itself, so every other native_store comparison
    # keeps naming the actual storage backend. Only mssql is mapped here (the one mismatch actually
    # reproduced); the other generic `_RDB_KINDS` entries whose SQLAlchemy scheme differs from any
    # SQLGlot dialect name (mariadb/greenplum/cockroachdb/yugabytedb/opengauss/tidb/vertica/
    # saphana/sapase/sqlanywhere/monetdb/db2/firebird) are tracked, not guessed at, in a filed
    # issue — SQLGlot compatibility per product needs verifying, not assuming.
    _SQLGLOT_DIALECT_ALIASES: dict[str, str] = {"mssql": "tsql"}

    def __init__(self, engine: FederationEngine) -> None:
        self.engine = engine
        # (source_id, table_name) -> why its replica could not be reconciled to the registered
        # shape. A replicated table in this state has no replica a read may be answered from, so
        # a read that names it raises this instead of reading it. Cleared when a reconcile
        # succeeds.
        self._unreconciled: dict[tuple[str, str], BaseException] = {}

    @property
    def unreconciled(self) -> dict[tuple[str, str], BaseException]:
        """(source_id, table_name) -> why its replica could not be reconciled (REQ-826). A read
        that names such a table is refused where its tables are addressed
        (``replica_address.address_replicas``) — per table: a sibling table of the same source
        is read as usual."""
        return self._unreconciled

    @property
    def dialect(self) -> str:
        """The physical SQL dialect the engine speaks (transpile target). Named by native_store
        for native engines (duckdb → ``duckdb``); falls back to the engine name."""
        store = self.engine.native_store or self.engine.name
        return self._SQLGLOT_DIALECT_ALIASES.get(store, store)

    def transpile_physical(self, pg_sql: str) -> str:
        """Transpile governed PostgreSQL-dialect SQL to the engine's physical dialect. Native
        engines use the plain SQLGlot transpile to their dialect; the Trino backend overrides with
        its dialect-specific rewrite pipeline. Callers reach this through the seam so they never
        hardcode a specific engine's dialect."""
        from provisa.transpiler.transpile import transpile

        return transpile(pg_sql, self.dialect)

    @property
    def has_otel_catalog(self) -> bool:
        """Whether the engine exposes the ``otel`` telemetry catalog (``otel.signals.*``).

        ``otel`` is a Trino dynamic catalog — an Iceberg catalog over object storage with a JDBC
        metastore (core/trino_system_catalogs.py). A native engine has no such catalog: its
        telemetry lands in the dedicated ops store instead. Seeding the ops signals tables anyway
        put ``otel.signals.traces`` (and the rest) into every role's compilation context on the
        native tier, so an unlabeled Cypher ``MATCH (n)`` — which unions every label — and a plain
        ``SELECT * FROM ops.traces`` both compiled to a catalog the engine does not have and failed
        with ``Catalog "otel" does not exist``. Registration follows this flag."""
        return False

    # -- lifecycle -------------------------------------------------------------

    def is_connected(self, state: Any) -> bool:
        """Native engines run in-process — always connected once built."""
        return True

    def provision(self, state: Any, ops_views: list) -> None:
        """No external terminal to connect; telemetry lands in the dedicated ops store."""

    def connect_terminal(self, state: Any) -> None:  # REQ-1900
        """No external terminal to connect."""

    async def provision_infra(self, state: Any) -> None:
        """No Arrow-Flight proxy / results schema for a native engine, so nothing to do at boot.
        The redirect results bucket (REQ-171) is ensured by the first redirect that needs it
        (``redirect.ensure_results_bucket_sync``), not here."""
        del state

    async def reconcile_landed_tables(self, state: Any) -> list[tuple[str, str]]:
        """Schema-currency reconcile of MATERIALIZED landing tables (REQ-846/932). No-op on the base
        engine; NativeEngineBackend converges + attaches, TrinoBackend converges the store table the
        engine's source catalog already resolves."""
        del state
        return []

    async def refresh_landed_views(self, state: Any) -> None:  # REQ-1730
        """Optional post-reconcile hook: pick up schema/table changes ``reconcile_landed_tables``
        just wrote where the ENGINE's own catalog metadata is cached rather than live.

        No-op on the base engine and on DuckDB (its connection sees an ATTACHed catalog's state
        directly — nothing to refresh). Trino's ``provisa_admin`` catalog is a Postgres CONNECTOR
        with its own metadata cache, so a schema/table ``reconcile_landed_tables`` wrote by dialing
        Postgres directly (never through Trino) needs an explicit reload before Trino's own
        listing — and so the compiler's resolved ``catalog.schema.table`` reference — sees it."""
        del state

    def replica_address(
        self,
        state: Any,
        *,
        source_id: str,
        schema_name: str,
        table_name: str,
        region: str | None = None,
    ) -> ReplicaAddress:
        """Where the replica of a source table is written in this engine's store (REQ-1912): the
        replicas schema of the org and environment being served, under the one replica name. The
        same on every engine — no engine places a replica at its table's registered address.
        ``region`` names another region's replica: where that region wrote it (REQ-1922)."""
        from provisa.federation.replica_address import active_org_id, replica_address

        return replica_address(
            org_id=active_org_id(state),
            source_id=source_id,
            schema_name=schema_name,
            table_name=table_name,
            region=region,
        )

    def export_view_address(
        self, state: Any, *, source_id: str, schema_name: str, table_name: str
    ) -> ReplicaAddress:
        """Where a store that publishes a view of each replica for its catalog export puts the
        view of this table's replica (REQ-1912): the export schema of the org and environment
        being served. Never the table's registered address."""
        from provisa.federation.replica_address import active_org_id, export_view_address

        return export_view_address(
            org_id=active_org_id(state),
            source_id=source_id,
            schema_name=schema_name,
            table_name=table_name,
        )

    # -- replica builds (REQ-1915): this engine and its store as parties to a build ----------

    def replica_engine(
        self, state: Any, source: Any, table: Any, *, address: ReplicaAddress, args: Any
    ) -> Any:
        """This engine's part in building the replica of ``table`` at ``address``: what it
        declares it can do and, where it can copy, the copy. The base engine copies nothing
        itself and reads the finished replica from its store."""
        del source, table, address, args
        from provisa.federation.replica_parties import StoreReadingEngine

        return StoreReadingEngine(self, state)

    def replica_target(self, state: Any, *, address: ReplicaAddress, args: Any, engine: Any) -> Any:
        """The write face of the replica at ``address`` in this engine's store, chosen by the
        store. ``engine`` is this engine's party to the build (``replica_engine``).

        The base engine's replicas are in its materialization store, reached by that store's
        DSN; the store's own kind (not the engine's) names the face. An engine that is its own
        store overrides this."""
        del state
        from sqlalchemy import make_url

        from provisa.federation.data_replicator import EngineRun
        from provisa.federation.replica_parties import store_target

        dsn = self.engine.materialize_store()
        return store_target(
            make_url(dsn).get_backend_name(),
            dsn,
            address=address,
            columns=args.columns,
            pk_columns=list(args.pk_columns or ()),
            engine_writes_store=EngineRun.STATEMENT in engine.caps.runs,
        )

    async def after_replica_swap(self, state: Any) -> None:
        """What this engine must do once a new replica stands in its store. Nothing for an
        engine that reads its store's tables as they are."""
        del state

    def replica_read_catalog(self, state: Any) -> str | None:
        """The catalog a statement names this engine's store by when it reads a replica, or None
        on an engine whose SQL has no catalog (the replicas schema is then addressed alone).

        A single-catalog engine (a warehouse) reads its store under that one catalog; any other
        reads it under the catalog its materialized views are read under."""
        if not self.engine.catalog_qualified:
            return None
        from provisa.federation.engine import fixed_catalog_for
        from provisa.federation.replica_address import active_org_id

        fixed = fixed_catalog_for(self.engine)
        if fixed:
            return fixed
        return self._store_catalog(state, active_org_id(state))

    def _store_catalog(self, state: Any, org_id: str) -> str:
        """The catalog this engine names its materialization store by: that of its MV target."""
        return self.materialize_store_target(state, org_id)[0]

    def region_read_address(
        self, state: Any, region: Any, schema: str, table: str
    ) -> tuple[str | None, str, str]:
        """Where a statement reads ``schema.table`` of another region's replicas store
        (REQ-1922): a name only — the read map is published without dialing any other region.
        ``attach_region_read`` makes it readable when a read finds that replica built.
        ``region`` is a ``region_stores.ForeignRegion``. An engine with no way to attach another
        store refuses, naming itself — it is never read live in its place."""
        del state, schema, table
        raise EngineReadsNoOtherRegion(self.engine.name, region.id)

    def attach_region_read(
        self, state: Any, region: Any, schema: str, table: str, build: object
    ) -> None:
        """Make ``schema.table`` of another region's replicas store readable at
        ``region_read_address`` (REQ-1922), for the build of it ``build`` identifies (a replica
        rebuilt with other columns is attached again). Called by a read that found the replica
        built there. Raises ``RegionStoreUnreachable`` when that store cannot be attached."""
        del state, schema, table, build
        raise EngineReadsNoOtherRegion(self.engine.name, region.id)

    def pending_lands(
        self,
        sources: list,
        *,
        is_stale: Any,
        replicated_of: Any = None,
        load_protected_of: Any = None,
        resident_of: Any = None,
        materialization_backend: str | None = None,
        freshness_subject_of: Any = None,
        now: float | None = None,
    ) -> list:
        """The residency prep steps a read of ``sources`` needs on this engine: one per source that
        federates MATERIALIZED here and that its staleness oracle (or REQ-860 gate, or REQ-1141
        first-load rule) says must land first. Pure — it reads no store. It is the decision
        the read backstop (``query_residency.ensure_resident``) acts on, and the one the query path asks before it reads any
        freshness state: with ``is_stale`` answering True for everything, an empty result means
        the engine reads every one of these sources in place and none of them ever lands."""
        from provisa.federation.plan import build_execution_plan

        return build_execution_plan(
            sources,
            self.engine,
            is_stale,
            replicated_of=replicated_of,
            load_protected_of=load_protected_of,
            resident_of=resident_of,
            materialization_backend=materialization_backend,
            freshness_subject_of=freshness_subject_of,
            now=now,
        ).prep

    async def land_source_table(
        self,
        state: Any,
        *,
        schema: str,
        table: str,
        columns: list[tuple[str, str]],
        rows: list[dict],
        change_signal: str = "ttl",
        watermark_column: str | None = None,
        pk_columns: list[str] | None = None,
        match_floor: float = 0.0,
        shape: str | None = None,
    ) -> str:
        """Land a source's ``rows`` into the materialization store (the per-fire refresh write
        face). Base default writes through ``store_writer`` against the engine's own store DSN — the
        universal path every backend supports. A native engine whose store is embedded and shares a
        single connection (DuckDB, REQ-989) overrides this to write through that connection instead."""
        del state
        from provisa.federation import store_writer

        return await store_writer.land(
            self.engine.materialize_store(),
            schema=schema,
            table=table,
            columns=columns,
            rows=rows,
            change_signal=change_signal,
            watermark_column=watermark_column,
            pk_columns=pk_columns,
            match_floor=match_floor,
            shape=shape,
        )

    async def apply_cdc_events(
        self,
        state: Any,
        *,
        schema: str,
        table: str,
        columns: list[tuple[str, str]],
        pk_columns: list[str],
        events: list,
    ) -> dict[str, int]:
        """Apply CDC change events (insert/update -> upsert by PK, delete -> tombstone) to a landed
        table (REQ-1733). Base default writes through ``store_writer``'s per-call connection against
        the engine's own store DSN — the universal path every backend supports. A native engine
        whose store is embedded and shares a single connection (DuckDB, REQ-989) overrides this to
        write through that connection instead, mirroring ``land_source_table``'s own dispatch."""
        del state
        from sqlalchemy.schema import CreateSchema

        from provisa.federation.materialize_exec import apply_cdc, build_table
        from provisa.federation.store_writer import store_connection

        tbl = build_table(schema, table, columns, tuple(pk_columns))
        async with store_connection(self.engine.materialize_store()) as conn:
            if schema and conn.capabilities.schemas:
                await conn.execute_core(CreateSchema(schema, if_not_exists=True))
            return await apply_cdc(conn, tbl, pk_columns, events)

    async def analyze_landed_table(
        self, state: Any, *, catalog: str | None, schema: str, table: str
    ) -> None:  # REQ-280, REQ-1688
        """Collect planner statistics on a table landed in the materialization store.

        Base default: through the store's OWN connection, in the store's dialect — the engine's
        attach is a read view and its ANALYZE is the engine's, not the store's (DuckDB implements
        ANALYZE as VACUUM and refuses it on an attached Postgres table). A store dialect with no
        statistics statement is skipped by name. ``catalog`` is the engine's attach name, which
        the Trino backend uses because there the engine IS the analyzer; it is None on an engine
        whose SQL has no catalog.
        """
        del state, catalog
        from provisa.federation import store_writer

        dsn = self.engine.materialize_store()
        async with store_writer.store_connection(dsn) as conn:
            dialect = conn.capabilities.dialect
            if dialect == "postgresql":
                await conn.execute(f'ANALYZE "{schema}"."{table}"')
            elif dialect in ("mysql", "mariadb"):
                await conn.execute(f"ANALYZE TABLE `{schema}`.`{table}`")
            else:
                _log.info(
                    "statistics for %s.%s skipped: store dialect %r collects none",
                    schema,
                    table,
                    dialect,
                )

    async def reconcile_mv_table(
        self,
        state: Any,
        *,
        schema: str,
        table: str,
        columns: list[tuple[str, str]],
        pk_columns: list[str] | None = None,
    ) -> str:
        """Converge an MV's OWN store table to its output ``columns`` (REQ-970). Base default writes
        through ``store_writer``; a native engine with an embedded single-connection store overrides
        this to converge through that connection instead."""
        del state
        from provisa.federation import store_writer

        return await store_writer.reconcile_table(
            self.engine.materialize_store(),
            schema=schema,
            table=table,
            columns=columns,
            pk_columns=pk_columns,
        )

    async def reconcile_mv_metadata(
        self, state: Any, *, schema: str, table: str, pk_columns: list[str] | None = None
    ) -> None:
        """Converge an MV store table's keys, descriptions and tags (REQ-1652/1654/1655). No-op on
        the base engine: its stores enforce constraints, where a FOREIGN KEY would refuse every
        REPLACE land. A native engine whose store holds informational constraints overrides this."""
        del state, schema, table, pk_columns

    def mv_store_broker(self, state: Any) -> Any:
        """A broker the MV refresh must write through instead of engine SQL (REQ-1901). The base
        engine writes its MV store table as engine SQL, so it has none; a native backend whose
        runtime holds an unattached embedded store overrides this."""
        del state
        return None

    async def persist_mv_table(
        self,
        state: Any,
        *,
        schema: str,
        table: str,
        columns: list[tuple[str, str]],
        rows: list[dict],
        persist: str,
        pk_columns: list[str] | None = None,
        match_floor: float = 0.0,
    ) -> str:
        """Land an MV's recomputed ``rows`` into its OWN store table under the declared PERSISTENCE
        outcome (REQ-965). Base default writes through ``store_writer``; a native engine with an
        embedded single-connection store overrides this to write through that connection instead."""
        del state
        from provisa.federation import store_writer

        return await store_writer.persist_land(
            self.engine.materialize_store(),
            schema=schema,
            table=table,
            columns=columns,
            rows=rows,
            persist=persist,
            pk_columns=pk_columns,
            match_floor=match_floor,
        )

    async def watchdog(self, state: Any) -> None:
        """No external process to watch."""

    async def reload_catalog(self, state: Any, catalog: str, ops_views: list) -> dict:
        return {
            "success": False,
            "errors": [f"engine {self.engine.name!r} has no reloadable catalog"],
        }

    def classify_error(self, exc: Exception) -> str | None:
        return None

    # -- federation lifecycle (REQ-825) ----------------------------------------
    # The boot sequence drives every engine through the same lifecycle:
    #   write_config → configure_session → provision → provision_infra → (serve) → close
    # plus the ongoing watchdog. Each phase is engine-agnostic at the call site; an engine slots its
    # own behavior into a phase, and a phase it doesn't need is a no-op (never a name-branch caller).

    def write_config(self, state: Any, config_path: str) -> None:
        """Lifecycle: render the engine's cluster config from platform config (Trino jvm.config /
        config.properties). In-process engines have no external cluster to configure — no-op."""

    def configure_session(self, state: Any, server_cfg: dict) -> None:
        """Lifecycle: set engine session hints (e.g. Trino fault-tolerant execution) on ``state``.
        Native engines have no per-session cluster tuning — no-op."""

    def polling_provider(
        self, state: Any, catalog: str, schema: str, table: str, watermark_column: str
    ) -> Any:
        """A change-data polling provider for the engine, or ``None`` when the engine offers no
        catalog-polling transport (native engines poll their source directly instead). Return type is
        Any so a subclass (TrinoBackend) can return a concrete provider without an incompatible-override."""
        return None

    def bind_terminal(self, state: Any) -> None:
        """Lifecycle (REQ-1043/REQ-1244): resolve and store the terminal's connection parameters
        WITHOUT connecting, so a sleeping cluster (idle-stopped between sessions) is only woken by
        the first real query. In-process engines have no remote terminal to defer — no-op."""

    def close(self, state: Any) -> None:
        """Lifecycle: tear down the engine terminal. Native engines close with the process — no-op."""

    def register_kafka_catalog(self, state: Any, kafka_source: dict) -> None:
        """Register a Kafka source as an engine catalog. Native engines reach Kafka through their
        own connector (or not at all) — no-op."""

    def reseed_ops(self, state: Any, ops_views: list) -> None:
        """Idempotently re-seed the OTel ops store (self-heal if boot seeding raced). No-op for a
        native engine, whose telemetry lives in the dedicated ops store."""

    def cluster_diagnostics(self, state: Any) -> tuple[bool, int, int]:
        """Engine health for the admin system-health view: ``(connected, worker_count,
        active_workers)``. A native in-process engine has no worker cluster."""
        return (self.is_connected(state), 0, 0)

    #: The formats this engine writes to an object store itself (REQ-1194): a statement's
    #: result goes from the engine to the store and no row passes through Provisa. Empty: the
    #: engine has no such write, and a result is delivered another way.
    result_formats: frozenset[str] = frozenset()

    def writes_results_now(self) -> bool:
        """Whether the deployment has given the engine what it writes results with. True for an
        engine that needs nothing beyond its connection and the results store's own keys."""
        return True

    def ctas_redirect(
        self, state: Any, physical_sql: str, output_format: str, params: list | None
    ) -> dict:
        """Run the statement with its bound values (``params``) and have the engine write the
        result to the results object store: ``{s3_prefix, row_count}`` (REQ-1194). An engine
        declares the formats it writes in ``result_formats``; one that declares none has no
        such write."""
        raise NotImplementedError(
            f"engine {self.engine.name!r} does not write results to an object store"
        )

    # -- source lifecycle ------------------------------------------------------

    def register_source(
        self, state: Any, source: Any, resolved_password: str, catalog_name: str | None = None
    ) -> None:
        """Native engines attach lazily at query time — nothing to provision here.

        ``catalog_name`` (REQ-1266) is the org-prefixed physical catalog name; native engines
        attach by source id at query time and ignore it (the ATTACH-alias namespacing lives in
        the native runtime), but the seam signature stays uniform across backends."""

    def drop_source(self, state: Any, source_id: str, catalog_name: str | None = None) -> None:
        """No dynamic catalog to drop."""

    def analyze(
        self, state: Any, source: Any, tables: list, catalog_name: str | None = None
    ) -> None:
        """Native engines gather statistics implicitly or not at all."""

    # -- connections -----------------------------------------------------------

    @contextmanager
    def isolated_sync(self, state: Any) -> Iterator[EngineSession | StoreBrokerSession]:
        """Native engines share the bound in-process connection. Yields an :class:`EngineSession`,
        never the raw physical-driver connection."""
        from provisa.executor.session import EngineSession

        yield EngineSession(state.engine_conn, dialect=self.dialect)

    def cache_catalog(self, state: Any) -> str | None:
        """Catalog the API-result cache lives in for THIS engine — the reference to its materialization
        store. Generic across every engine: it always resolves through ``_materialize_store_ref``."""
        return self._materialize_store_ref(state)

    def materialize_store_target(self, state: Any, org_id: str) -> tuple[str, str]:
        """The (catalog, schema) an MV materializes into for THIS engine.

        An OWN-store engine (a Postgres store-engine, ``_materialize_store_ref`` → None) writes into
        its own catalog under the org-scoped MV-cache schema. A native engine that ATTACHES its store
        (DuckDB exposes it under the ``mat_store`` alias) overrides this to return the attached
        store's catalog + its store schema, so the MV target matches where source-landing actually
        writes — otherwise the refresh targets a catalog the engine has never heard of (the observed
        "Catalog with name postgresql does not exist" on a DuckDB deployment).
        """
        # REQ-1623: the environment's own cache schema, not the org's -- an MV refreshed in one
        # environment must not land in the schema another environment reads.
        from provisa.core.environments import active_org_schema

        return "postgresql", active_org_schema(org_id, "_mv_cache")

    def _materialize_store_ref(self, state: Any) -> str | None:
        """The catalog under which this engine references its materialization store (attaching it on
        first use). ``None`` means the engine materializes into its OWN persistent store — its source
        catalogs are already durable (a broad federator, or a store-engine). An engine whose source
        exposure is EPHEMERAL (DuckDB in-memory) overrides this to attach the store and return its
        catalog. A missing store is a hard error at attach time (never a fallback)."""
        return None

    # -- introspection ---------------------------------------------------------

    def introspect_by_catalog(
        self, state: Any, catalog: str, schema: str, table: str
    ) -> dict[str, str]:
        """Native engines have no live physical-catalog information_schema to read at compile time."""
        return {}

    @staticmethod
    def _merged_source(source: Any, schema_name: str, table_name: str) -> Any:
        """The connection-resolved view of a source the DuckDB runtime attaches from: every
        ``${env:..}``/``${secret:..}`` in the connection fields resolved, plus the table address."""
        from types import SimpleNamespace

        from provisa.core.secrets import resolve_secrets

        def _rs(v: Any) -> Any:
            return resolve_secrets(v) if isinstance(v, str) else v

        return SimpleNamespace(
            id=source.id,
            type=source.type,
            host=_rs(getattr(source, "host", None)),
            port=getattr(source, "port", None),
            # REQ-1693: the endpoint-style connectors (sharepoint's siteUrl, splunk's url) read
            # base_url and fall back to host. Omitting it here raised AttributeError inside the
            # connector, which surfaced as an empty Register Table schema list.
            base_url=_rs(getattr(source, "base_url", None)),
            database=_rs(getattr(source, "database", None)),
            username=_rs(getattr(source, "username", None)),
            password=_rs(getattr(source, "password", None)),
            path=_rs(getattr(source, "path", None)),
            federation_hints=getattr(source, "federation_hints", {}) or {},
            mapping=getattr(source, "mapping", {}) or {},
            # REQ-788: carry the source's file_glob table specs so an introspection- or
            # execution-time attach builds the file adapter's merged glob table (the endpoint is
            # built once per source and cached, so a merge that drops them poisons later reads).
            file_glob_tables=getattr(source, "file_glob_tables", []) or [],
            schema_name=schema_name,
            table_name=table_name,
        )

    def _attaches_live(self, source: Any) -> bool:
        """Whether the native engine reads this source by ATTACHing it (so its database can be
        listed) rather than landing it from an adapter or direct driver.

        Deliberately excludes SCAN (csv/parquet/gsheets/iceberg/delta-style single-view scanners):
        each maps to exactly ONE view with no nested schema/table hierarchy to list — DuckDBRuntime's
        own _attached_alias already returns None for a "view_ddl" connector for this reason, so
        including SCAN here would reach the same `[]` by a longer path, not a real fix (see
        native_schemas's csv/parquet branch, REQ-1732, for the actual schema/table answer these
        types need)."""
        from provisa.federation.connector import Mechanism

        if self.engine.native_store is None:
            return False
        return self.engine.connector_for(source.type.value).mechanism in (
            Mechanism.ATTACH_RW,
            Mechanism.ATTACH_R,
        )

    def introspect_schemas(self, state: Any, source: Any) -> list[str] | None:
        """REQ-1673: the schemas of an ATTACH source's database, without a registered table — the
        DuckDB runtime attaches the raw source and reads its information_schema. ``None`` when this
        engine has no such seam (a federator lists through its own catalog SQL instead)."""
        if self.engine.native_store != "duckdb" or not self._attaches_live(source):
            return None
        return self._duckdb_introspect(
            source, "schemas", lambda rt, s: rt.introspect_schemas(s), schema_name="", table_name=""
        )

    def introspect_tables(self, state: Any, source: Any, schema_name: str) -> list[str] | None:
        """REQ-1673: the tables of one schema of an ATTACH source's database (see introspect_schemas)."""
        if self.engine.native_store != "duckdb" or not self._attaches_live(source):
            return None
        return self._duckdb_introspect(
            source,
            f"tables of {schema_name}",
            lambda rt, s: rt.introspect_tables(s, schema_name),
            schema_name=schema_name,
            table_name="",
        )

    def _duckdb_introspect(
        self, source: Any, what: str, read: Any, *, schema_name: str, table_name: str
    ) -> list[str]:
        import duckdb

        from provisa.federation.duckdb_runtime import DuckDBFederationRuntime

        runtime = DuckDBFederationRuntime()
        try:
            return read(runtime, self._merged_source(source, schema_name, table_name))
        except duckdb.Error:
            # The engine cannot reach the source right now (extension install offline, source
            # down): the listing is empty and the cause is in the log, never a silent [].
            _log.warning("duckdb introspection of %s for %r failed", what, source.id, exc_info=True)
            return []
        finally:
            runtime.close()

    def introspect_columns(
        self, state: Any, source: Any, schema_name: str, table_name: str
    ) -> dict[str, str]:
        """Column types as the native engine reports them (DuckDB DESCRIBE / ClickHouse DESCRIBE).
        Returns ``{column_name: type_name}``; ``{}`` when the engine cannot introspect live."""
        # A native-store engine exposes a source by ATTACHing it, then DESCRIBEs the attached
        # relation. A non-attachable remote source (openapi/graphql_remote/grpc/NoSQL/…) instead
        # LANDs into the materialization store — there is nothing attached to DESCRIBE until its
        # rows are materialized on demand. Decide this ONCE here, engine-agnostically, so no engine
        # runtime has to: keep the config-declared column types rather than dispatching an attach
        # that would fail. (Pure federators like Trino never attach at introspect — they fall
        # through to the {} contract below and don't consult connector_for.)
        if self.engine.native_store is not None:
            from provisa.federation.connector import Mechanism

            if self.engine.connector_for(source.type.value).mechanism in (
                Mechanism.DIRECT,
                Mechanism.FETCH,
            ):
                return {}
        if self.engine.native_store == "duckdb":
            from provisa.federation.duckdb_runtime import DuckDBFederationRuntime

            merged = self._merged_source(source, schema_name, table_name)
            import duckdb

            runtime = DuckDBFederationRuntime()
            try:
                return runtime.introspect_columns(merged)
            except duckdb.Error:
                # Engine can't reach the source right now (e.g. offline extension install,
                # source down): keep declared types. Introspection only augments — logged.
                _log.warning(
                    "duckdb introspection of %s.%s failed; keeping declared types",
                    schema_name,
                    table_name,
                    exc_info=True,
                )
                return {}
            finally:
                runtime.close()
        if self.engine.native_store == "clickhouse":  # REQ-909 / REQ-912
            from types import SimpleNamespace

            from provisa.core.secrets import resolve_secrets

            # The ClickHouse ENGINE backend (server via clickhouse://, or embedded chdb via chdb://)
            # comes from the configured engine URL ($PROVISA_ENGINE_URL or the persisted config);
            # without it the engine cannot introspect live — keep declared types (seam contract).
            from provisa.federation.engine import configured_engine_url

            dsn = configured_engine_url()
            if not dsn:
                return {}

            def _rs(v: Any) -> Any:  # resolve ${env:..}/${secret:..} in connection strings
                return resolve_secrets(v) if isinstance(v, str) else v

            merged = SimpleNamespace(
                id=source.id,
                # REQ-1266/1529: what the engine keeps for the source is named after its catalog.
                catalog=state.source_catalogs[source.id],
                type=source.type,
                host=_rs(getattr(source, "host", None)),
                port=getattr(source, "port", None),
                database=_rs(getattr(source, "database", None)),
                username=_rs(getattr(source, "username", None)),
                password=_rs(getattr(source, "password", None)),
                path=_rs(getattr(source, "path", None)),
                federation_hints=getattr(source, "federation_hints", {}),
                file_glob_tables=getattr(source, "file_glob_tables", []) or [],  # REQ-788
                schema_name=schema_name,
                table_name=table_name,
            )
            from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime

            runtime = ClickHouseFederationRuntime.from_url(dsn)
            try:
                return runtime.introspect_columns(merged)
            except Exception:
                # Engine can't reach the source right now (server down, private bucket, Mongo needs
                # a column list): keep declared types. Introspection only augments — logged.
                _log.warning(
                    "clickhouse introspection of %s.%s failed; keeping declared types",
                    schema_name,
                    table_name,
                    exc_info=True,
                )
                return {}
            finally:
                runtime.close()
        return {}

    # -- execution -------------------------------------------------------------

    async def execute(
        self,
        state: Any,
        sql: str,
        params: list | None = None,
        *,
        session_hints: dict[str, str] | None = None,
        fresh: bool = False,
        conn_kwargs: dict | None = None,
        span_attrs: dict[str, str] | None = None,
        extra_table_attrs: list[dict[str, str]] | None = None,
    ) -> QueryResult:
        raise NotImplementedError(
            f"live ENGINE-terminal execution for engine {self.engine.name!r} is not wired "
            "(native-runtime execution binding is separate feature work)"
        )

    def borrow_raw_pg_connection(self, state: Any) -> Any:
        """One pooled Postgres connection for pgwire's raw-DataRow passthrough (REQ-1863). An
        engine that is not Postgres has none: the passthrough does not apply to it."""
        from provisa.pgwire.pg_passthrough import PassthroughError

        del state
        raise PassthroughError(f"engine {self.engine.name!r} is not a Postgres connection pool")

    def execute_sync(
        self,
        state: Any,
        sql: str,
        params: list | None = None,
        *,
        session_hints: dict[str, str] | None = None,
    ) -> ResultStream:
        raise NotImplementedError(
            f"live ENGINE-terminal execution for engine {self.engine.name!r} is not wired"
        )

    def describe_sync(
        self, state: Any, sql: str, params: list | None = None
    ) -> ResultStream | None:
        """Describe a statement's result shape without running it (REQ-589). ``None``: this
        engine has no describe-without-running (Trino, the Arrow warehouse engines — the pgwire
        Describe then runs the statement, as it did before)."""
        del state, sql, params
        return None

    # -- engine-specific transports (Arrow) ------------------------------------

    def execute_arrow(self, state: Any, sql: str, params: list | None = None):
        raise NotImplementedError(
            f"engine {self.engine.name!r} does not implement an Arrow transport"
        )

    def execute_stream(self, state: Any, sql: str, params: list | None = None):
        raise NotImplementedError(
            f"engine {self.engine.name!r} does not implement an Arrow stream transport"
        )


class TrinoBackend(EngineBackend):
    """The Trino engine's backend — the ONE backend that references Trino. Delegates to the Trino
    implementation modules (trino_lifecycle / core.catalog / compiler.introspect / executor.trino)."""

    reads_other_regions = True  # REQ-1922: a catalog of that region's store

    @property
    def dialect(self) -> str:
        return "trino"

    @property
    def has_otel_catalog(self) -> bool:
        """Trino owns the ``otel`` catalog — ensure_system_catalogs registers it on the
        coordinator (provision/reseed_ops), so ``otel.signals.*`` resolves."""
        return True

    def transpile_physical(self, pg_sql: str) -> str:
        from provisa.transpiler.transpile import (
            rewrite_prometheus_labels_for_trino,
            transpile_to_trino,
        )

        sql = transpile_to_trino(pg_sql)
        from provisa.api.app import state

        label_columns = getattr(state, "prometheus_label_columns", None)
        if label_columns:
            sql = rewrite_prometheus_labels_for_trino(sql, label_columns)
        return sql

    def _store_catalog_named(self, state: Any, name: str, dsn: str) -> str:
        """Register (once per process) and return the catalog Trino reads the store ``dsn`` under
        (REQ-1048, REQ-1922): an org's own store, or another region's replicas store."""
        import re

        from provisa.core.trino_system_catalogs import (
            one_registrar,
            register_catalog,
            store_catalog_spec,
        )

        name = re.sub(r"[^a-z0-9_]", "_", name.lower())
        registered: dict[str, str] = self.__dict__.setdefault("_store_catalogs", {})
        if registered.get(name) == dsn:
            return name
        with self._provisioning_conn(state) as conn:
            if conn is None:
                raise RuntimeError(
                    f"no Trino terminal to register store catalog {name!r} on; the engine is "
                    "not bound"
                )
            # One process of the deployment registers at a time (drop-then-create interleaves).
            with one_registrar(state.tenant_engine.url):
                register_catalog(conn, store_catalog_spec(name, dsn))
        registered[name] = dsn
        return name

    def region_read_address(
        self, state: Any, region: Any, schema: str, table: str
    ) -> tuple[str | None, str, str]:
        """REQ-1922: another region's replicas store, as catalog ``org_<org>__region_<id>``."""
        return self._region_catalog(state, region), schema, table

    def attach_region_read(
        self, state: Any, region: Any, schema: str, table: str, build: object
    ) -> None:
        """REQ-1922: the catalog of that region's store, registered once per process (it reads
        the store's tables as they are, so a rebuild needs nothing more)."""
        import trino.exceptions

        del schema, table, build
        try:
            self._store_catalog_named(
                state, self._region_catalog(state, region), region.replicas_url
            )
        except (trino.exceptions.Error, OSError) as exc:
            raise RegionStoreUnreachable(str(exc)) from exc

    @staticmethod
    def _region_catalog(state: Any, region: Any) -> str:
        from provisa.federation.replica_address import region_read_name

        return region_read_name(state, region)

    def materialize_store_target(self, state: Any, org_id: str) -> tuple[str, str]:
        """Trino reaches its materialization store through the ``provisa_admin`` catalog.

        The base default names the catalog ``postgresql``, which is a *store-engine's* own catalog
        name — Trino has no such catalog. ``register_system_catalogs`` registers the control-plane
        Postgres (the same database ``materialize_store()`` lands into) as ``provisa_admin``, which
        is also what ``resolved_cache_catalog`` names for the API-result cache. Inheriting the base
        default made every MV sweep and refresh on Trino ask for
        ``"postgresql"."org_<org>_mv_cache"`` and answer CATALOG_NOT_FOUND.
        """
        from provisa.core.environments import active_org_schema  # REQ-1623
        from provisa.core.trino_system_catalogs import PROVISA_ADMIN_CATALOG

        from provisa.storage.byo import org_store_dsn

        # REQ-1048 / REQ-1922: an org with a store of its own (brought, or its region's) keeps its
        # replicas and views there, where store_writer writes them (engine.materialize_store), so
        # Trino reads them through that store's catalog, not the control plane's.
        own = org_store_dsn(org_id)
        if own is not None:
            catalog = self._store_catalog_named(state, f"org_{org_id}__store", own)
            return catalog, active_org_schema(org_id, "_mv_cache")
        return PROVISA_ADMIN_CATALOG, active_org_schema(org_id, "_mv_cache")

    # -- replicas --------------------------------------------------------------

    async def reconcile_landed_tables(self, state: Any) -> list[tuple[str, str]]:
        """Converge each MATERIALIZED source's store table to its registered shape (REQ-846/932).

        DDL only — no rows (that is the refresh's job). Trino needs this for the same reason the
        native engines do: a poll node probes its table's watermark BEFORE the first land, and an
        unresolvable relation would fail the probe and so prevent the land that would have created
        it. The store write face is ``store_writer`` against the engine's own store DSN — the Postgres
        Trino reads replicas from, in its replicas schema (REQ-1912)."""
        from provisa.federation import store_writer
        from provisa.federation.replica_routing import landing_worklist

        reconciled: list[tuple[str, str]] = []
        for src, schema_name, table_name, columns, pk_columns in await landing_worklist(
            self.engine, state
        ):
            address = self.replica_address(
                state, source_id=src.id, schema_name=schema_name, table_name=table_name
            )
            await store_writer.reconcile_table(
                self.engine.materialize_store(),
                schema=address.schema,
                table=address.table,
                columns=columns,
                pk_columns=pk_columns,
            )
            reconciled.append((src.id, table_name))
        return reconciled

    async def after_replica_swap(self, state: Any) -> None:
        """Trino reads the store through its ``provisa_admin`` catalog, whose metadata it may
        cache: flush it, so the swapped-in table is the one the next statement resolves."""
        await self.refresh_landed_views(state)

    async def refresh_landed_views(self, state: Any) -> None:  # REQ-1730
        """Flush Trino's ``provisa_admin`` catalog metadata cache after ``reconcile_landed_tables``
        writes a schema/table by dialing its Postgres materialize store directly (``store_writer``,
        never through Trino itself). ``provisa_admin``'s ``postgresql`` connector sets no
        ``metadata.cache-ttl`` today (control_plane_spec, trino_system_catalogs.py) — the default
        is 0 (no caching) — so this is currently a no-op in practice; it is cheap insurance against
        that property ever being set, not a fix for an active bug. Best-effort: a coordinator that
        cannot take the call right now will simply see the change on ITS next natural TTL refresh.

        Also ensures the org's API-result cache schema exists — Trino's ``postgresql`` connector
        raises NOT_SUPPORTED on ``CREATE SCHEMA`` (unlike DuckDB's, which allows it), so
        ``engine_cache.ensure_cache_schema`` can never create it THROUGH Trino once an
        adapter-fetched source's query tries to cache there. Created directly against the same
        Postgres ``materialize_store()`` DSN provisa_admin itself reads, the same way
        ``reconcile_landed_tables`` writes the landed tables — bypassing the connector limitation
        entirely rather than working around it query-by-query. Done BEFORE the flush below, so
        the flush (when the coordinator's cache-ttl is ever non-zero) picks up a schema that
        already exists rather than caching its ABSENCE moments before it is created."""
        try:
            from provisa.core.environments import active_org_schema
            from provisa.core.request_context import require_current_org
            from provisa.federation.store_writer import store_connection
            from sqlalchemy.schema import CreateSchema

            org_id = require_current_org()
            cache_schema = active_org_schema(org_id, "_api_cache")
            async with store_connection(self.engine.materialize_store()) as store_conn:
                await store_conn.execute_core(CreateSchema(cache_schema, if_not_exists=True))
        except Exception:
            _log.warning("ensuring the API-result cache schema failed (non-fatal)", exc_info=True)

        from provisa.core.trino_system_catalogs import PROVISA_ADMIN_CATALOG

        with self._provisioning_conn(state) as conn:
            if conn is None:
                return
            try:
                cur = conn.cursor()
                cur.execute(f"CALL {PROVISA_ADMIN_CATALOG}.system.flush_metadata_cache()")
                cur.fetchall()
            except Exception:
                _log.warning(
                    "flush_metadata_cache on %s failed (non-fatal)",
                    PROVISA_ADMIN_CATALOG,
                    exc_info=True,
                )

    # -- lifecycle -------------------------------------------------------------

    def is_connected(self, state: Any) -> bool:
        return state.engine_conn is not None

    def provision(self, state: Any, ops_views: list) -> None:
        from provisa.federation import trino_lifecycle

        trino_lifecycle.provision(state, ops_views)

    def connect_terminal(self, state: Any) -> None:  # REQ-1900
        from provisa.federation import trino_lifecycle

        trino_lifecycle.connect_terminal(state)

    def bind_terminal(self, state: Any) -> None:
        from provisa.federation import trino_lifecycle

        trino_lifecycle.bind_terminal(state)

    async def provision_infra(self, state: Any) -> None:
        from provisa.federation import trino_lifecycle

        await trino_lifecycle.connect_infra(state)

    async def watchdog(self, state: Any) -> None:
        from provisa.federation import trino_lifecycle

        await trino_lifecycle.watchdog(state)

    async def reload_catalog(self, state: Any, catalog: str, ops_views: list) -> dict:
        from provisa.federation import trino_lifecycle

        return await trino_lifecycle.reload_catalog(state, catalog, ops_views)

    def classify_error(self, exc: Exception) -> str | None:
        from provisa.federation import trino_lifecycle

        return trino_lifecycle.classify_error(exc)

    def write_config(self, state: Any, config_path: str) -> None:
        from provisa.federation import trino_lifecycle

        trino_lifecycle.write_config(config_path)

    def configure_session(self, state: Any, server_cfg: dict) -> None:
        from provisa.federation import trino_lifecycle

        trino_lifecycle.configure_session(state, server_cfg)

    def polling_provider(
        self, state: Any, catalog: str, schema: str, table: str, watermark_column: str
    ):
        from provisa.federation import trino_lifecycle

        return trino_lifecycle.polling_provider(state, catalog, schema, table, watermark_column)

    def close(self, state: Any) -> None:
        if getattr(state, "flight_client", None) is not None:
            state.flight_client.close()
        for _gen, client in getattr(state, "flight_clients", {}).values():
            client.close()
        getattr(state, "flight_clients", {}).clear()
        if state.engine_conn is not None:
            state.engine_conn.close()

    def register_kafka_catalog(self, state: Any, kafka_source: dict) -> None:
        from provisa.federation import trino_lifecycle

        trino_lifecycle.register_kafka_catalog(state, kafka_source)

    def reseed_ops(self, state: Any, ops_views: list) -> None:
        """Register the Provisa-owned catalogs and seed the ops views on THIS org's terminal.

        REQ-1043/REQ-1244/REQ-1427: gating on ``engine_conn`` alone made this a silent no-op on
        every isolated- or external-engine org, whose terminal is bound with kwargs and no
        connection. ``provision()`` — the only other caller of ``register_system_catalogs`` — runs
        for the deployment's shared engine, so an org moved to its own coordinator had
        ``provisa_admin``, ``otel`` and ``results`` on the coordinator it had just left: the admin
        reports read ``otel.signals.*`` and failed CATALOG_NOT_FOUND, and no amount of telemetry
        compaction could put rows in front of that org. Use the same provisioning connection source
        registration uses, which honours the wake-on-traffic contract.
        """
        with self._provisioning_conn(state) as conn:
            if conn is None:
                return
            from provisa.core.trino_system_catalogs import ensure_system_catalogs
            from provisa.observability.ops_trino import seed_ops_trino

            # Ordering as in provision(): seed_ops_trino writes into `otel`.
            assert state.tenant_engine is not None
            # REQ-1429: ensure, not re-register — dropping the deployment-scoped `otel`/`results`
            # for one org takes them away from every other org on a shared coordinator.
            ensure_system_catalogs(conn, state.tenant_engine.url, state.org_id)
            seed_ops_trino(conn, ops_views)

    def cluster_diagnostics(self, state: Any) -> tuple[bool, int, int]:
        conn = state.engine_conn
        if conn is None:
            return (False, 0, 0)
        worker_count = 0
        active_workers = 0
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            connected = True
            cursor.execute("SELECT state, count(*) FROM system.runtime.nodes GROUP BY state")
            for row in cursor.fetchall():
                node_state, cnt = row[0], int(row[1])
                worker_count += cnt
                if node_state == "active":
                    active_workers = cnt
        except Exception as exc:
            # Surface connection/config errors instead of masking them as an unhealthy cluster.
            raise RuntimeError(f"Trino cluster diagnostics probe failed: {exc}") from exc
        return (connected, worker_count, active_workers)

    result_formats = frozenset({"parquet", "orc"})

    def ctas_redirect(
        self, state: Any, physical_sql: str, output_format: str, params: list | None
    ) -> dict:
        from provisa.executor import redirect, trino_write

        # REQ-171: the coordinator writes the CTAS result into the results schema, whose location
        # is the results bucket, so the first CTAS redirect of this process makes sure of both, in
        # that order (neither is done at boot). Either failing raises to this redirect.
        redirect.ensure_results_bucket_sync(redirect.RedirectConfig.from_env())
        trino_write.ensure_results_schema(state.engine_conn)
        return trino_write.execute_ctas_redirect(
            state.engine_conn, physical_sql, output_format, params
        )

    # -- source lifecycle ------------------------------------------------------

    @contextmanager
    def _provisioning_conn(self, state: Any):
        """The connection catalog DDL is issued on, honoring the wake-on-traffic contract.

        REQ-1043/REQ-1244/REQ-1427: an isolated org's terminal is bound with kwargs and NO
        connection so its dedicated coordinator can sleep. Source provisioning is real traffic —
        gating it on ``engine_conn`` alone made registration a silent no-op, so the org's catalogs
        were never issued onto its own coordinator and every query failed CATALOG_NOT_FOUND.
        Connect from the stored kwargs (waking the cluster) and close that connection after.
        Yields ``None`` only when the terminal has NEITHER a connection nor kwargs.
        """
        conn = state.engine_conn
        if conn is not None:
            yield conn
            return
        conn_kwargs = state.engine_conn_kwargs
        if not conn_kwargs:
            yield None
            return
        from provisa.federation import trino_lifecycle

        conn = trino_lifecycle.connect(conn_kwargs)
        try:
            yield conn
        finally:
            conn.close()

    def register_source(
        self, state: Any, source: Any, resolved_password: str, catalog_name: str | None = None
    ) -> None:
        """Register the source's catalog — its live attach. A source the operator floors
        (REQ-030/826/1141) is read only from its replica, so it has no catalog at all (REQ-1912):
        one left from before the setting was turned on is dropped. Nothing can then read the
        source through the engine, whatever a statement names."""
        from provisa.core import catalog
        from provisa.core.operator_floor import floor_setting

        with self._provisioning_conn(state) as conn:
            if conn is None:
                return
            if floor_setting(source) is not None:
                catalog.drop_catalog(conn, source.id, catalog_name=catalog_name)
                return
            catalog.create_catalog(conn, source, resolved_password, catalog_name=catalog_name)

    def drop_source(self, state: Any, source_id: str, catalog_name: str | None = None) -> None:
        with self._provisioning_conn(state) as conn:
            if conn is not None:
                from provisa.core import catalog

                catalog.drop_catalog(conn, source_id, catalog_name=catalog_name)

    def analyze(
        self, state: Any, source: Any, tables: list, catalog_name: str | None = None
    ) -> None:
        with self._provisioning_conn(state) as conn:
            if conn is not None:
                from provisa.core import catalog

                catalog.analyze_source_tables(conn, source, tables, catalog_name=catalog_name)

    async def analyze_landed_table(
        self, state: Any, *, catalog: str | None, schema: str, table: str
    ) -> None:  # REQ-280, REQ-1688
        """On Trino the engine IS the analyzer of its catalogs: ANALYZE through the coordinator when
        the catalog's connector collects statistics, else skip by name (REQ-636)."""
        assert catalog is not None  # Trino's SQL is catalog-qualified: every table has one
        with self._provisioning_conn(state) as conn:
            if conn is None:
                return
            from provisa.core import catalog as _catalog

            if catalog not in _catalog.analyze_capable_catalogs(conn):
                _log.info(
                    "statistics for %s.%s.%s skipped: connector collects none",
                    catalog,
                    schema,
                    table,
                )
                return
            cur = conn.cursor()
            cur.execute(f'ANALYZE {catalog}.{schema}."{table}"')
            cur.fetchall()

    # -- connections -----------------------------------------------------------

    @contextmanager
    def isolated_sync(self, state: Any):
        """A fresh, thread-isolated Trino dbapi connection, closed on exit. Yields an
        :class:`EngineSession`, never the raw ``trino.dbapi`` connection."""
        from provisa.executor.session import EngineSession
        from provisa.federation import trino_lifecycle

        conn = trino_lifecycle.connect(state.engine_conn_kwargs)
        session = EngineSession(conn, dialect=self.dialect)
        try:
            yield session
        finally:
            session.close()

    # -- introspection ---------------------------------------------------------

    def introspect_by_catalog(
        self, state: Any, catalog: str, schema: str, table: str
    ) -> dict[str, str]:
        if state.engine_conn is None:
            return {}
        from provisa.compiler.introspect import introspect_column_types

        return introspect_column_types(state.engine_conn, catalog, schema, table)

    def introspect_columns(
        self, state: Any, source: Any, schema_name: str, table_name: str
    ) -> dict[str, str]:
        conn = state.engine_conn
        if conn is None:
            return {}
        import trino.exceptions

        from provisa.compiler.introspect import introspect_column_types

        try:
            return introspect_column_types(
                conn, state.catalog_for(source.id), schema_name, table_name
            )
        except trino.exceptions.Error as exc:
            # REQ-636/REQ-251 contract: introspection is best-effort type enrichment. A source
            # whose catalog is unavailable (not registered, unreachable, table absent) yields {}
            # so the caller keeps the YAML-declared types and startup stays resilient to a down
            # source. Transient SERVER_STARTING_UP is already retried inside introspect_column_types
            # before it can reach here; only a genuinely unresolvable engine error degrades.
            _log.debug(
                "introspect_columns degraded to {} for %s.%s.%s: %s",
                source.id,
                schema_name,
                table_name,
                exc,
            )
            return {}

    # -- execution -------------------------------------------------------------

    async def execute(
        self,
        state: Any,
        sql: str,
        params: list | None = None,
        *,
        session_hints: dict[str, str] | None = None,
        fresh: bool = False,
        conn_kwargs: dict | None = None,
        span_attrs: dict[str, str] | None = None,
        extra_table_attrs: list[dict[str, str]] | None = None,
    ) -> QueryResult:
        from provisa.executor.trino import execute_trino

        if fresh and conn_kwargs is None:
            conn_kwargs = state.engine_conn_kwargs
        conn = state.engine_conn
        # REQ-1043/REQ-1244: a bound-but-unconnected terminal (bind_terminal — sleeping cluster,
        # wake-on-traffic) has kwargs and no conn; execute_trino's liveness path connects from
        # state.engine_conn_kwargs on this first query. Only a terminal with NEITHER is an error.
        if conn is None and conn_kwargs is None and not state.engine_conn_kwargs:
            raise RuntimeError(f"engine {self.engine.name!r} connection not available")
        _conn = cast("Any", conn)
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            lambda: execute_trino(
                _conn,
                sql,
                params=params,
                session_hints=session_hints,
                conn_kwargs=conn_kwargs,
                span_attrs=span_attrs,
                extra_table_attrs=extra_table_attrs,
            ),
        )

    def execute_sync(
        self,
        state: Any,
        sql: str,
        params: list | None = None,
        *,
        session_hints: dict[str, str] | None = None,
    ) -> QueryResult:
        from provisa.executor.trino import execute_trino

        conn = state.engine_conn
        # Same wake-on-traffic contract as execute(): kwargs-only means execute_trino connects.
        if conn is None and not state.engine_conn_kwargs:
            raise RuntimeError(f"engine {self.engine.name!r} connection not available")
        return execute_trino(cast("Any", conn), sql, params=params, session_hints=session_hints)

    # -- engine-specific transports (Arrow via Zaychik Flight SQL proxy) --------

    def _flight_transport(self, state: Any) -> Any:
        from provisa.federation import k8s_provisioner as k8s

        if k8s.provisioning_available():
            return self._shard_flight_transport(state)

        client = state.flight_client
        if client is None:
            raise RuntimeError(
                f"engine {self.engine.name!r} Arrow Flight transport is not configured "
                "(set ZAYCHIK_HOST/ZAYCHIK_PORT and ensure the proxy is running)"
            )
        return client

    def _shard_flight_transport(self, state: Any) -> Any:
        """REQ-1518: the Flight connection for the shard the ACTIVE org queries, not the boot one.

        The proxy is a sidecar in the shard's pod, so its address is that pod's and it dies with it.
        A single connection built at boot against ``boot_shard()`` therefore had two defects: an
        isolated org's Arrow/stream query drained the SHARED shard's proxy — voiding the isolation
        the org is invoiced for — and after any shard restart the connection held a released pod
        address. Both are resolved the same way the SQL terminal resolves its endpoint: per shard,
        per coordinator generation. ``ensure_engine_awake`` has already run at the top of the ONE
        pipeline, so the shard is serving and its address is recorded before this is asked for.
        """
        from provisa.executor.trino_flight import create_flight_connection
        from provisa.federation.engine_wake import active_shard, generation
        from provisa.federation.k8s_provisioner import shard_flight_endpoint

        shard = active_shard(state)
        if shard is None:
            raise RuntimeError(
                f"engine {self.engine.name!r} Arrow Flight transport cannot be resolved: the "
                "active org runs a coordinator this control plane does not provision, so it has "
                "no Zaychik sidecar to dial (REQ-1518)"
            )
        gen = generation(shard)
        cached = state.flight_clients.get(shard)
        if cached is not None:
            cached_gen, client = cached
            if cached_gen == gen:
                return client
            # The pod that held this connection is gone; closing it is what stops the next Arrow
            # query from re-using a socket to a released address.
            client.close()
            del state.flight_clients[shard]

        host, port = shard_flight_endpoint(shard)
        client = create_flight_connection(host=host, port=port)
        state.flight_clients[shard] = (gen, client)
        return client

    def execute_arrow(self, state: Any, sql: str, params: list | None = None):
        from provisa.executor.trino_flight import execute_trino_flight_arrow

        return execute_trino_flight_arrow(self._flight_transport(state), sql, params)

    def execute_stream(self, state: Any, sql: str, params: list | None = None):
        from provisa.executor.trino_flight import execute_trino_flight_stream

        return execute_trino_flight_stream(self._flight_transport(state), sql, params)
