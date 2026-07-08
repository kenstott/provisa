# Copyright (c) 2026 Kenneth Stott
# Canary: 8f8ec523-0921-4866-889d-9a3f38256e46
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Startup seed helpers extracted from app.py (cohesive cluster of async seeding functions).

Importing is deferred: app.py must import names from this module ONLY inside function bodies
(lazy imports) to avoid a circular-import at load time.  This module's own top-level imports
from app.py are safe because this module is never loaded at app.py module-initialisation time.
"""

# complexity-gate: allow-ble=1 reason="grandfathered bare-except in _resolve_pk_from_sources relocated from app.py; PK resolution is best-effort and logs exc_info on failure, never crashing startup"

# Requirements: REQ-012, REQ-016, REQ-057, REQ-510, REQ-695, REQ-837

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from sqlalchemy import delete as _delete, func as _sa_func, select, update

from provisa.api._meta_views import _META_TABLE_VIEWS
from provisa.core.control_plane import bring_up_platform
from provisa.core.database import Connection, Database, create_engine_from_url
from provisa.core.db import init_schema
from provisa.core.schema_org import (
    registered_tables as _registered_tables_t,
    relationships as _relationships_t,
    sources as _sources_t,
    table_columns as _table_columns_t,
)
from provisa.observability.ops_schema import OPS_TABLES as _OPS_TABLES

# Circular-import guard: app.py never imports this module at module level.
# By the time any function here is called, app.py is fully initialised and
# `state`, `_META_TABLE_ALIAS`, `_META_TABLES` are bound in its namespace.
from provisa.api.app import state, _META_TABLE_ALIAS, _META_TABLES  # noqa: E402

# Views registered in the ops domain alongside the raw Iceberg tables.
# Each entry: (view_name, [(col_name, data_type, is_pk)], ddl_sql)
_OPS_VIEWS: list[tuple[str, list[tuple[str, str, bool]], str]] = [
    (
        "queries",
        [
            ("trace_id", "text", True),
            ("span_id", "text", False),
            ("parent_span_id", "text", False),
            ("span_name", "text", False),
            ("service_name", "text", False),
            ("timestamp", "bigint", False),
            ("end_timestamp", "bigint", False),
            ("duration", "bigint", False),
            ("status_code", "integer", False),
            ("table_name", "text", False),
            ("domain_id", "text", False),
            ("role_id", "text", False),
            ("query_text", "text", False),
            ("_date", "date", False),
        ],
        """\
CREATE OR REPLACE VIEW otel.signals.queries AS
SELECT
    trace_id,
    span_id,
    parent_span_id,
    span_name,
    service_name,
    "timestamp",
    end_timestamp,
    duration,
    status_code,
    table_name,
    domain_id,
    role_id,
    query_text,
    _date
FROM otel.signals.traces
WHERE span_name LIKE 'provisa.query%'
""",
    ),
]


async def _seed_meta_domain(
    conn: "Connection", org_id: str = "default"
) -> None:  # REQ-012, REQ-016, REQ-695
    """Register admin tables in the built-in meta domain (idempotent)."""
    schema_name = f"org_{org_id}"
    for ddl in _META_TABLE_VIEWS.values():
        await conn.execute(ddl)

    # Remove any stale view-named entries left by older code versions.
    for view_name in _META_TABLE_ALIAS.values():
        await conn.execute_core(
            _delete(_registered_tables_t).where(
                _registered_tables_t.c.source_id == "provisa-admin",
                _registered_tables_t.c.schema_name == schema_name,
                _registered_tables_t.c.table_name == view_name,
            )
        )

    for tbl in _META_TABLES:
        table_id = await conn.upsert_returning(
            _registered_tables_t,
            {
                "source_id": "provisa-admin",
                "domain_id": "meta",
                "schema_name": schema_name,
                "table_name": tbl,
            },
            index_elements=["source_id", "schema_name", "table_name"],
            returning="id",
            update_columns=["domain_id"],
        )
        # Portable reflection (SQLAlchemy Inspector) instead of information_schema. The org
        # schema is honoured only on schema-capable backends — the abstraction decides.
        pk_cols = {
            c["column_name"]
            for c in await conn.reflect_columns(tbl, schema=schema_name)
            if c["is_primary_key"]
        }
        # Use the view name when available so column list reflects the exposed schema.
        view_name = _META_TABLE_ALIAS.get(tbl, tbl)
        cols = await conn.reflect_columns(view_name, schema=schema_name)
        col_names = {col["column_name"] for col in cols}
        # Remove stale columns that no longer appear in the view.
        await conn.execute_core(
            _delete(_table_columns_t).where(
                _table_columns_t.c.table_id == table_id,
                _table_columns_t.c.column_name.not_in(list(col_names)),
            )
        )
        for col in cols:
            await conn.upsert(
                _table_columns_t,
                {
                    "table_id": table_id,
                    "column_name": col["column_name"],
                    "visible_to": [],
                    "data_type": col["data_type"],
                    "is_primary_key": col["column_name"] in pk_cols,
                },
                index_elements=["table_id", "column_name"],
                update_columns=[],
            )

    # Register registered_tables → table_columns relationship (table_id FK)
    _rt_id = (
        await conn.execute_core(
            select(_registered_tables_t.c.id).where(
                _registered_tables_t.c.source_id == "provisa-admin",
                _registered_tables_t.c.table_name == "registered_tables",
            )
        )
    ).scalar()
    _tc_id = (
        await conn.execute_core(
            select(_registered_tables_t.c.id).where(
                _registered_tables_t.c.source_id == "provisa-admin",
                _registered_tables_t.c.table_name == "table_columns",
            )
        )
    ).scalar()
    if _rt_id is not None and _tc_id is not None:
        await conn.upsert(
            _relationships_t,
            {
                "id": "meta:registered_tables:table_columns",
                "source_table_id": _rt_id,
                "target_table_id": _tc_id,
                "source_column": "id",
                "target_column": "table_id",
                "cardinality": "one-to-many",
            },
            index_elements=["id"],
            update_columns=[],
        )


async def _seed_meta_relationships(conn: "Connection") -> None:
    """Seed FK relationships between meta and ops tables (idempotent, runs after both seeds)."""
    from provisa.api._meta_seed import META_RELATIONSHIPS

    for (
        _rid,
        _src_source,
        _src_table,
        _src_col,
        _tgt_col,
        _card,
        _tgt_source,
        _tgt_table,
        _alias,
        _gql_alias,
    ) in META_RELATIONSHIPS:
        _src_id = (
            await conn.execute_core(
                select(_registered_tables_t.c.id)
                .where(
                    _registered_tables_t.c.source_id == _src_source,
                    _registered_tables_t.c.table_name == _src_table,
                )
                .limit(1)
            )
        ).scalar()
        _tgt_id = (
            await conn.execute_core(
                select(_registered_tables_t.c.id)
                .where(
                    _registered_tables_t.c.source_id == _tgt_source,
                    _registered_tables_t.c.table_name == _tgt_table,
                )
                .limit(1)
            )
        ).scalar()
        # Empty cross join in the former INSERT...SELECT produced no row; mirror that skip.
        if _src_id is None or _tgt_id is None:
            continue
        await conn.upsert(
            _relationships_t,
            {
                "id": _rid,
                "source_table_id": _src_id,
                "target_table_id": _tgt_id,
                "source_column": _src_col,
                "target_column": _tgt_col,
                "cardinality": _card,
                "alias": _alias,
                "graphql_alias": _gql_alias,
            },
            index_elements=["id"],
            update_columns=[
                "source_table_id",
                "target_table_id",
                "source_column",
                "target_column",
                "cardinality",
                "alias",
                "graphql_alias",
            ],
        )


async def _compute_and_store_clusters(conn: "Connection") -> int:  # REQ-510
    """Run Louvain on the schema graph and write l1/l2/l3_cluster onto registered_tables."""
    from provisa.schema_clusters import compute_clusters

    rows = (await conn.execute_core(select(_registered_tables_t.c.id))).fetchall()
    table_ids = [r[0] for r in rows]

    rel_rows = (
        await conn.execute_core(
            select(
                _relationships_t.c.source_table_id,
                _relationships_t.c.target_table_id,
            ).where(
                _relationships_t.c.source_table_id.is_not(None),
                _relationships_t.c.target_table_id.is_not(None),
            )
        )
    ).fetchall()
    edges = [(r[0], r[1]) for r in rel_rows]

    if not table_ids:
        return 0

    clusters = compute_clusters(table_ids, edges)

    for tid, (l1, l2, l3) in clusters.items():
        await conn.execute_core(
            update(_registered_tables_t)
            .where(_registered_tables_t.c.id == tid)
            .values(
                l1_cluster=l1,
                l2_cluster=l2,
                l3_cluster=l3,
                clusters_computed_at=_sa_func.now(),
            )
        )
    return len(clusters)


async def _seed_ops_pg(conn: "Connection") -> None:  # REQ-016
    """Register ops tables/views in PG registered_tables + table_columns (idempotent)."""

    async def _seed_cols(table_id: Any, cols: list) -> None:
        for col_name, pg_type, is_pk in cols:
            await conn.upsert(
                _table_columns_t,
                {
                    "table_id": table_id,
                    "column_name": col_name,
                    "visible_to": [],
                    "data_type": pg_type,
                    "is_primary_key": is_pk,
                },
                index_elements=["table_id", "column_name"],
                update_columns=[],
            )

    for tbl_name, cols in _OPS_TABLES.items():
        table_id = await conn.upsert_returning(
            _registered_tables_t,
            {
                "source_id": "provisa-otel",
                "domain_id": "ops",
                "schema_name": "signals",
                "table_name": tbl_name,
            },
            index_elements=["source_id", "schema_name", "table_name"],
            returning="id",
            update_columns=["domain_id"],
        )
        await _seed_cols(table_id, cols)
    for view_name, cols, _ in _OPS_VIEWS:
        table_id = await conn.upsert_returning(
            _registered_tables_t,
            {
                "source_id": "provisa-otel",
                "domain_id": "ops",
                "schema_name": "signals",
                "table_name": view_name,
            },
            index_elements=["source_id", "schema_name", "table_name"],
            returning="id",
            update_columns=["domain_id"],
        )
        await _seed_cols(table_id, cols)


async def _init_control_planes(
    config_path: str | None,
) -> tuple[str, int, str, str]:  # REQ-057, REQ-837
    """Bring up both control planes from config and init tenant schema + audit.

    Returns the tenant DB connection parts (host, port, database, user) for the
    engine self-catalog. All connection details come from the config layer
    (``control_plane``), which is the only place the environment is read — both
    planes are driven purely by SQLAlchemy, each by its own URI."""
    from provisa.core.config_loader import load_control_plane

    cp = load_control_plane(config_path)
    org_id = cp.resolved_org_id()
    state.org_id = org_id

    # Tenant plane: schema-scoped to ``org_<id>`` via search_path (the tenant-scope
    # mechanism). Platform plane (bring_up_platform): global registry + billing,
    # never org-scoped. Two independent engines, each its own SQLAlchemy URI.
    tenant_engine = create_engine_from_url(
        cp.resolved_tenant_url(), pool_size=cp.pool_max, max_overflow=cp.max_overflow
    )
    state.tenant_db = Database(tenant_engine, name="org", search_path=f"org_{org_id}")
    state.admin_db = await bring_up_platform(
        cp.resolved_platform_url(), pool_size=cp.pool_max, pool_min=cp.pool_min
    )

    schema_sql_path = Path(__file__).parent.parent / "core" / "schema.sql"
    if schema_sql_path.exists():
        await init_schema(state.tenant_db, schema_sql_path.read_text(), org_id=org_id)

    from provisa.audit.query_log import init_audit_schema

    await init_audit_schema(state.tenant_db, org_id=org_id)

    host, port, database, username, _pw = cp.tenant_parts()
    # Every backend identifies a database (a PG database name, a SQLite file path, …).
    # Host/user are backend-specific and validated at connect time, not asserted here.
    assert database, "control_plane.tenant_url must specify a database"
    return host, port, database, username


async def _seed_built_in_sources(  # REQ-012, REQ-016, REQ-510
    pg_host: str, pg_port: int, pg_database: str, pg_user: str
) -> None:
    """Seed provisa-admin, provisa-otel, and __provisa__ source rows; seed meta domain and ops; compute clusters.

    The provisa-admin source is the control-plane self-catalog; its ``type``/``dialect`` follow the
    control plane's actual backend (``postgresql`` for PG, ``sqlite`` for the file-based demo)."""
    assert state.tenant_db is not None
    cp_dialect = state.tenant_db.dialect
    from provisa.federation.engine import configured_engine_endpoint

    engine_host_early, engine_port_early = configured_engine_endpoint()
    async with state.tenant_db.acquire() as _conn:
        _admin_desc = (
            "Provisa internal administration database — stores source registrations, table "
            "metadata, relationships, roles, and governance configuration"
        )
        await _conn.upsert(
            _sources_t,
            {
                "id": "provisa-admin",
                "type": cp_dialect,
                "host": pg_host or "",
                "port": pg_port,
                "database": pg_database,
                "username": pg_user or "",
                "dialect": cp_dialect,
                "description": _admin_desc,
            },
            index_elements=["id"],
            update_columns=[],
            set_extra={
                "description": _sa_func.coalesce(
                    _sa_func.nullif(_sources_t.c.description, ""), _admin_desc
                )
            },
        )
        _engine_name = state.federation_engine.name
        _otel_desc = (
            "Observability telemetry store — OpenTelemetry spans and traces collected from "
            "Provisa query execution, used for performance monitoring and query analytics"
        )
        await _conn.upsert(
            _sources_t,
            {
                "id": "provisa-otel",
                "type": "iceberg",
                "host": engine_host_early,
                "port": engine_port_early,
                "database": "otel",
                "username": "provisa",
                "dialect": _engine_name,
                "description": _otel_desc,
            },
            index_elements=["id"],
            update_columns=[],
            set_extra={
                "description": _sa_func.coalesce(
                    _sa_func.nullif(_sources_t.c.description, ""), _otel_desc
                )
            },
        )
        await _conn.upsert(
            _sources_t,
            {
                "id": "__provisa__",
                "type": _engine_name,
                "description": (
                    "Provisa-managed virtual views — cross-source SQL views defined and "
                    "published by the data team as governed data products"
                ),
            },
            index_elements=["id"],
            update_columns=[],
        )
        await _seed_meta_domain(_conn, org_id=state.org_id)
        await _seed_ops_pg(_conn)
        await _seed_meta_relationships(_conn)
        needs_clusters = (
            await _conn.execute_core(
                select(_sa_func.count())
                .select_from(_registered_tables_t)
                .where(_registered_tables_t.c.l1_cluster.is_(None))
            )
        ).scalar()
        if needs_clusters:
            await _compute_and_store_clusters(_conn)


async def _resolve_pk_from_sources() -> None:
    """Second pass — resolve PRIMARY KEYs from each native RDBMS source's information_schema."""
    assert state.tenant_db is not None
    _startup_log = logging.getLogger("uvicorn.error")
    _PK_RDBMS_TYPES = ("postgresql", "mysql", "mariadb", "singlestore", "sqlserver", "redshift")
    _PK_SOURCE_TYPES = _PK_RDBMS_TYPES + ("sqlite",)
    async with state.tenant_db.acquire() as _pk_conn:
        _pk_rows = [
            dict(_r._mapping)
            for _r in (
                await _pk_conn.execute_core(
                    select(
                        _registered_tables_t.c.id,
                        _registered_tables_t.c.source_id,
                        _registered_tables_t.c.schema_name,
                        _registered_tables_t.c.table_name,
                        _sources_t.c.type.label("source_type"),
                    )
                    .select_from(
                        _registered_tables_t.join(
                            _sources_t, _sources_t.c.id == _registered_tables_t.c.source_id
                        )
                    )
                    .where(_sources_t.c.type.in_(list(_PK_SOURCE_TYPES)))
                )
            ).fetchall()
        ]
        for _pk_t in _pk_rows:
            _sch = _pk_t["schema_name"].replace("'", "''")
            _tbl = _pk_t["table_name"].replace("'", "''")
            _pk_sql = (
                "SELECT kcu.column_name "
                "FROM information_schema.table_constraints tc "
                "JOIN information_schema.key_column_usage kcu "
                "  ON tc.constraint_name = kcu.constraint_name "
                "  AND tc.table_schema = kcu.table_schema "
                "WHERE tc.constraint_type = 'PRIMARY KEY' "
                f"  AND tc.table_schema = '{_sch}' AND tc.table_name = '{_tbl}'"
            )
            try:
                if _pk_t["source_id"] == "provisa-admin":
                    # Control-plane self-catalog: its tables live in the CP connection, so reflect
                    # there portably (the abstraction ignores the schema on schema-less backends).
                    _pk_cols = [
                        c["column_name"]
                        for c in await _pk_conn.reflect_columns(
                            _pk_t["table_name"], schema=_pk_t["schema_name"]
                        )
                        if c["is_primary_key"]
                    ]
                elif _pk_t["source_type"] == "sqlite":
                    # External SQLite file sources have no information_schema and are not in the CP
                    # connection; their PKs are resolved by the engine during schema rebuild.
                    continue
                elif state.source_pools.has(_pk_t["source_id"]):
                    _pk_res = await state.source_pools.execute(_pk_t["source_id"], _pk_sql, None)
                    _pk_cols = [_row[0] for _row in _pk_res.rows]
                else:
                    continue
            except Exception:
                _startup_log.warning(
                    "PK resolve failed for %s.%s.%s",
                    _pk_t["source_id"],
                    _pk_t["schema_name"],
                    _pk_t["table_name"],
                    exc_info=True,
                )
                continue
            if _pk_cols:
                await _pk_conn.execute_core(
                    update(_table_columns_t)
                    .where(
                        _table_columns_t.c.table_id == _pk_t["id"],
                        _table_columns_t.c.column_name.in_(_pk_cols),
                    )
                    .values(is_primary_key=True)
                )
