# Copyright (c) 2026 Kenneth Stott
# Canary: 2e199759-cf1e-4674-a74b-5fcfa66a9929
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
import re
from pathlib import Path
from typing import Any

from sqlalchemy import (
    delete as _delete,
    func as _sa_func,
    literal as _sa_literal,
    or_ as _sa_or,
    select,
    update,
)

from provisa.api._meta_views import (
    _META_TABLE_VIEWS,
    _OPS_LOG_TABLE_ALIAS,
    _OPS_LOG_TABLE_VIEWS,
    _OPS_REPORT_VIEWS,
    _ops_table_usage_ddl,
)
from provisa.core.control_plane import bring_up_platform
from provisa.core.database import Connection, Database, create_engine_from_url
from provisa.api._catalog_descriptions import (
    COLUMN_DESCRIPTIONS as _COL_DESC,
    TABLE_DESCRIPTIONS as _TBL_DESC,
)
from provisa.core.db import init_schema
from provisa.core.environments import org_schema
from provisa.core.repositories import source as _source_repo
from provisa.core.repositories import table as _table_repo
from provisa.core.schema_org import (
    domains as _domains_t,
    registered_tables as _registered_tables_t,
    relationships as _relationships_t,
    sources as _sources_t,
    table_columns as _table_columns_t,
)
from provisa.observability.ops_schema import OPS_TABLES as _OPS_TABLES

# Circular-import guard: app.py never imports this module at module level.
# By the time any function here is called, app.py is fully initialised and
# `state`, `_META_TABLE_ALIAS`, `_META_TABLES` are bound in its namespace.
from provisa.api.app import state  # noqa: E402
from provisa.api.app_loaders import _META_TABLE_ALIAS, _META_TABLES  # noqa: E402
from provisa.core.models import DERIVED_SOURCE_ID

# REQ-1900: arbitrary fixed key for the Postgres session advisory lock that serializes
# _seed_built_in_sources' meta/ops-domain schema phase across concurrent `--workers N` processes
# (see the lock's own call site for why). Any distinct bigint works; this one is just
# "PROVISA1" packed as ASCII bytes, chosen so it reads as clearly app-specific in pg_locks output.
_SEED_DOMAIN_ADVISORY_LOCK_KEY = 0x50524F5649534131

# Views registered in the ops domain alongside the raw Iceberg tables.
# Each entry: (view_name, [(col_name, data_type, is_pk)], ddl_sql)
#
# REQ-1910: none today. The `queries` report used to be a view here, over the trace table's
# provisa.query.* spans; it now reads query_audit_log (``_meta_views._OPS_REPORT_VIEWS``), the one
# home of a statement's table, role, transport, status, duration and text, and `traces` holds one
# row per request with what only the span has (stage timings, route, engine, cache result, rows).
_OPS_VIEWS: list[tuple[str, list[tuple[str, str, bool]], str]] = []


_CREATE_OR_REPLACE_VIEW_RE = re.compile(r"CREATE\s+OR\s+REPLACE\s+VIEW\s+(\w+)\s+AS", re.IGNORECASE)


def _adapt_view_ddl(ddl: str, dialect: str) -> str:
    """Rewrite ``CREATE OR REPLACE VIEW name AS`` to ``DROP VIEW IF EXISTS`` + ``CREATE VIEW``.

    SQLite has no ``CREATE OR REPLACE VIEW`` at all (syntax error near "OR"), and PostgreSQL's
    accepts only a superset of the existing column list — dropping or renaming a column raises
    ``cannot drop columns from view``, so a startup that narrows a view (the ops reports losing
    ``tenant_id``) would fail against the previous release's view. Dropping first makes the seed
    idempotent for ANY shape change rather than only widening ones.

    The result is a multi-statement DDL string; ``Connection.execute`` detects multiple
    statements and routes to ``execute_script``, which runs each statement individually.

    PostgreSQL refuses to drop a view another view selects from, so the drop CASCADEs: the seed
    recreates the whole ops graph in dependency order (log views, then the ``ops_table_usage``
    spine, then the report views) immediately afterwards, so a cascaded dependent is rebuilt on
    the same pass.
    """
    m = _CREATE_OR_REPLACE_VIEW_RE.search(ddl)
    if not m:
        return ddl
    view_name = m.group(1)
    select_sql = ddl[m.end() :].strip()
    cascade = " CASCADE" if dialect in ("postgresql", "duckdb") else ""
    return f"DROP VIEW IF EXISTS {view_name}{cascade};\nCREATE VIEW {view_name} AS {select_sql}"


async def _seed_view(conn: "Connection", ddl: str, dialect: str) -> None:
    """Create/refresh one built-in view, without the DROP...CASCADE's blast radius when nothing
    about the view actually changed.

    REQ-1900's session advisory lock (see ``_seed_built_in_sources``) only serializes this seed
    phase against OTHER workers ALSO running it — it says nothing about a peer worker that has
    already finished its own pass and is now serving ordinary read traffic against these same
    views. ``_adapt_view_ddl``'s DROP VIEW ... CASCADE + recreate ran unconditionally on EVERY
    boot, even a warm restart where nothing changed, and CASCADE takes an AccessExclusiveLock
    across the whole dependent-view chain in one go. A concurrent reader holding an
    AccessShareLock on one of those dependents, in the opposite acquisition order, deadlocks
    against it. Confirmed live: `asyncpg.exceptions.DeadlockDetectedError` inside
    `_seed_meta_domain` on an 8-worker cold boot immediately after a clean 1-worker warm-up —
    which had already left every view in its final shape, so the CASCADE recreate a moment later
    was pure overhead, not a real reconciliation.

    Postgres's plain ``CREATE OR REPLACE VIEW`` locks only the one view (no CASCADE, no
    dependent chain) and is a true no-op when the definition is unchanged — the common case on
    every ordinary restart. It only fails with ``cannot drop columns from view`` when the shape
    genuinely narrowed (a real upgrade), which is the one case the CASCADE fallback below still
    exists for. SQLite/DuckDB have no ``CREATE OR REPLACE VIEW`` (see ``_adapt_view_ddl``'s own
    docstring) and no multi-worker story either, so they skip straight to the existing path.
    """
    if dialect == "postgresql":
        try:
            async with conn.transaction():
                await conn.execute(ddl)
            return
        except Exception as exc:
            if "cannot drop columns from view" not in str(exc):
                raise
    await conn.execute(_adapt_view_ddl(ddl, dialect))


def _keep_edited_description(column: Any, seeded: str | None) -> dict[str, Any]:
    """Set assignments that fill a blank description with the seeded text and leave an edited one.

    The built-in meta/ops tables have no registering author to write their descriptions, so the
    platform supplies them (:mod:`provisa.api._catalog_descriptions`) — but a steward may improve
    any of them in the UI, and a plain update would overwrite that on the next boot. A column with
    no seeded text is left NULL, which is exactly what the REQ-609 ``stale_metadata`` report exists
    to surface; :mod:`tests.unit.test_catalog_descriptions` is what keeps that set empty.
    """
    if seeded is None:
        return {}
    return {"description": _sa_func.coalesce(_sa_func.nullif(column, ""), _sa_literal(seeded))}


# REQ-1467: the starter entity types for the `entity` tag. Seeded once, then owned by the org —
# the list is maintainer-editable and this seed never runs again against a non-empty list, so a
# type a maintainer deleted stays deleted and one they added is not competing with code.
_ENTITY_TAG_STARTER_VALUES: tuple[tuple[str, str], ...] = (
    ("account", "A held account — the party a balance or contract belongs to"),
    ("counterparty", "The other side of a trade, contract, or settlement"),
    ("customer", "A party that buys"),
    ("employee", "A person employed by the organization"),
    ("location", "A named place — site, facility, region"),
    ("organization", "A company or institution in no more specific role"),
    ("person", "A named individual in no more specific role"),
    ("product", "A sold or manufactured item"),
    ("project", "A named body of work"),
    ("vendor", "A party that supplies"),
)


async def _seed_tag_param_values(conn: "Connection") -> None:  # REQ-1467
    """Seed the starter parameter values for parameterized system tags, first install only.

    Values are data, not definition: `entity` stays code-defined and unstored like every other
    system tag, while which entity types the org recognises lives in tag_param_values and is
    theirs to edit. Skipping a tag that already has any row is what keeps this a seed rather
    than a redefinition — re-upserting on every boot would resurrect deleted types.
    """
    from provisa.core.repositories import tag as tag_repo
    from provisa.core.models import TagParamValue

    if await tag_repo.list_param_values(conn, "entity"):
        return
    for value, description in _ENTITY_TAG_STARTER_VALUES:
        await tag_repo.upsert_param_value(
            conn, TagParamValue(tag_id="entity", value=value, description=description)
        )


async def _drop_sibling_environment_registrations(
    conn: "Connection", domain_id: str, org_id: str, schema_name: str
) -> None:  # REQ-1488
    """Delete this org's ``provisa-admin`` registrations that name a DIFFERENT environment.

    The two seeds below are the only writers of the control plane's self-catalog rows, and each runs
    inside a runtime already scoped to one environment's schema. A row naming another environment of
    the same org is therefore not data to preserve — it is a row an earlier version of this code
    wrote at the wrong address, before the seeds knew environments existed. It is also fatal rather
    than untidy: the branch then holds two registrations of every meta table, and
    ``_assert_domain_table_unique`` refuses the whole runtime on the first request made to it.

    Scoped to THIS org's schemas on purpose. A portable (non-schema) control plane keeps every org's
    rows in one namespace distinguished only by ``schema_name``, so deleting on "not my schema"
    alone would take another org's catalog with it.

    Same reasoning as the stale view-name cleanup already in ``_seed_meta_domain``: the seed owns
    these rows, so the seed retires the ones it should never have written.
    """
    prod_schema = org_schema(org_id)
    await _table_repo.remove_registrations(
        conn,
        _registered_tables_t.c.source_id == "provisa-admin",
        _registered_tables_t.c.domain_id == domain_id,
        _registered_tables_t.c.schema_name != schema_name,
        _sa_or(
            _registered_tables_t.c.schema_name == prod_schema,
            _registered_tables_t.c.schema_name.like(f"{prod_schema}_env_%"),
        ),
    )


async def _seed_meta_domain(
    conn: "Connection", org_id: str = "default", env: str | None = None
) -> None:  # REQ-012, REQ-016, REQ-695
    """Register admin tables in the built-in meta domain (idempotent).

    REQ-1488: the schema is the ENVIRONMENT's, not the org's. This seed runs once per runtime, and
    a branch's runtime is scoped to its own schema — naming ``org_<id>`` here registered the
    branch's meta tables at prod's address, next to the branch's own copies of the same rows, and
    every request to that runtime then failed the (domain, table) uniqueness assertion.
    """
    schema_name = org_schema(org_id, env)
    for ddl in _META_TABLE_VIEWS.values():
        await _seed_view(conn, ddl, conn.capabilities.dialect)

    await _drop_sibling_environment_registrations(conn, "meta", org_id, schema_name)

    # Remove any stale view-named entries left by older code versions.
    for view_name in _META_TABLE_ALIAS.values():
        await _table_repo.remove_registrations(
            conn,
            _registered_tables_t.c.source_id == "provisa-admin",
            _registered_tables_t.c.schema_name == schema_name,
            _registered_tables_t.c.table_name == view_name,
        )

    for tbl in _META_TABLES:
        table_id = await conn.upsert_returning(
            _registered_tables_t,
            {
                "origin": "seed",  # REQ-1919: written when the row is created
                "source_id": "provisa-admin",
                "domain_id": "meta",
                "schema_name": schema_name,
                "table_name": tbl,
                "description": _TBL_DESC[tbl],
            },
            index_elements=["source_id", "schema_name", "table_name"],
            returning="id",
            update_columns=["domain_id"],
            set_extra=_keep_edited_description(_registered_tables_t.c.description, _TBL_DESC[tbl]),
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
                    "domain_id": "meta",
                    "column_name": col["column_name"],
                    "visible_to": [],
                    "data_type": col["data_type"],
                    "is_primary_key": col["column_name"] in pk_cols,
                    "description": _COL_DESC[tbl].get(col["column_name"]),
                },
                index_elements=["table_id", "column_name"],
                update_columns=[],
                set_extra=_keep_edited_description(
                    _table_columns_t.c.description, _COL_DESC[tbl].get(col["column_name"])
                ),
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


async def _seed_ops_domain(
    conn: "Connection", org_id: str = "default", env: str | None = None
) -> None:  # REQ-884
    """Expose internal operational logs (query_audit_log, …) as first-class tables in
    the built-in ``ops`` domain, reusing the meta-domain view+seed mechanism.

    Each internal log gets a curated view (safe columns only — the encrypted
    ``query_text_enc`` is excluded per REQ-689) registered under source
    ``provisa-admin`` / domain ``ops``, so it routes through the same role + domain
    access control as any business table. Adding another log is a registry entry in
    ``_OPS_LOG_TABLE_VIEWS`` — not a new subsystem.

    REQ-1386: also creates and registers the management report views
    (``_OPS_REPORT_VIEWS``) on the same path — always seeded on install, not demo
    data — and designates ``org_admin`` as the ops-domain steward so the domain
    never surfaces as a REQ-609 PENDING stewardship gap."""
    schema_name = org_schema(org_id, env)  # REQ-1488: the environment's schema, not the org's
    await _drop_sibling_environment_registrations(conn, "ops", org_id, schema_name)
    for ddl in _OPS_LOG_TABLE_VIEWS.values():
        await _seed_view(conn, ddl, conn.capabilities.dialect)

    for tbl, view_name in _OPS_LOG_TABLE_ALIAS.items():
        table_id = await conn.upsert_returning(
            _registered_tables_t,
            {
                "origin": "seed",  # REQ-1919: written when the row is created
                "source_id": "provisa-admin",
                "domain_id": "ops",
                "schema_name": schema_name,
                "table_name": tbl,
                "description": _TBL_DESC[tbl],
            },
            index_elements=["source_id", "schema_name", "table_name"],
            returning="id",
            update_columns=["domain_id"],
            set_extra=_keep_edited_description(_registered_tables_t.c.description, _TBL_DESC[tbl]),
        )
        # PKs from the physical table; exposed columns from the curated view.
        pk_cols = {
            c["column_name"]
            for c in await conn.reflect_columns(tbl, schema=schema_name)
            if c["is_primary_key"]
        }
        cols = await conn.reflect_columns(view_name, schema=schema_name)
        col_names = {col["column_name"] for col in cols}
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
                    "domain_id": "ops",
                    "column_name": col["column_name"],
                    # ops is a _LOCKDOWN_DOMAINS domain (schema_gen.py): visible_to=[]
                    # means visible to NO role there, not "unrestricted" as elsewhere.
                    # org_admin is the ops-domain steward (below) and must retain the
                    # explicit grant or it loses its own steward access (REQ-1386).
                    "visible_to": ["org_admin"],
                    "data_type": col["data_type"],
                    "is_primary_key": col["column_name"] in pk_cols,
                    "description": _COL_DESC[tbl].get(col["column_name"]),
                },
                index_elements=["table_id", "column_name"],
                update_columns=[],
                set_extra=_keep_edited_description(
                    _table_columns_t.c.description, _COL_DESC[tbl].get(col["column_name"])
                ),
            )

    # REQ-1386: management report views. The unnest spine first (the report views
    # reference it), then each report view, registered like any ops table. Every
    # report view exposes ``id`` as its primary key.
    await _seed_view(
        conn, _ops_table_usage_ddl(conn.capabilities.dialect), conn.capabilities.dialect
    )
    for view_name, ddl in _OPS_REPORT_VIEWS.items():
        await _seed_view(conn, ddl, conn.capabilities.dialect)
        table_id = await conn.upsert_returning(
            _registered_tables_t,
            {
                "origin": "seed",  # REQ-1919: written when the row is created
                "source_id": "provisa-admin",
                "domain_id": "ops",
                "schema_name": schema_name,
                "table_name": view_name,
                "description": _TBL_DESC[view_name],
            },
            index_elements=["source_id", "schema_name", "table_name"],
            returning="id",
            update_columns=["domain_id"],
            set_extra=_keep_edited_description(
                _registered_tables_t.c.description, _TBL_DESC[view_name]
            ),
        )
        cols = await conn.reflect_columns(view_name, schema=schema_name)
        col_names = {col["column_name"] for col in cols}
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
                    "domain_id": "ops",
                    "column_name": col["column_name"],
                    # ops is a _LOCKDOWN_DOMAINS domain (schema_gen.py): visible_to=[]
                    # means visible to NO role there, not "unrestricted" as elsewhere.
                    # org_admin is the ops-domain steward (below) and must retain the
                    # explicit grant or it loses its own steward access (REQ-1386).
                    "visible_to": ["org_admin"],
                    "data_type": col["data_type"],
                    "is_primary_key": col["column_name"] == "id",
                    "description": _COL_DESC[view_name].get(col["column_name"]),
                },
                index_elements=["table_id", "column_name"],
                set_extra=_keep_edited_description(
                    _table_columns_t.c.description, _COL_DESC[view_name].get(col["column_name"])
                ),
                update_columns=[],
            )

    # REQ-1386: org_admin stewards the ops domain (REQ-609 — never PENDING).
    await conn.execute_core(
        update(_domains_t)
        .where(_domains_t.c.id == "ops", _domains_t.c.steward.is_(None))
        .values(steward="org_admin")
    )


async def _ensure_ops_steward_grant(conn: "Connection") -> None:  # REQ-1386, REQ-1608
    """Give the ops stewards (``org_admin``, ``sandbox``) read access to every ops column.

    The two ops seeds insert their columns once (``update_columns=[]``) so a later grant made
    through the admin UI survives a restart. That also means a column first written without the
    steward grant keeps it forever, and in a lockdown domain that column is in no role's schema —
    the report's ``ops.<table>`` reference then reaches the engine unrewritten. This adds the
    stewards to any ops column missing them and touches nothing else, so REQ-1133 grants to other
    roles are preserved.

    ``sandbox`` is included so its illustration of admin capabilities (REQ-1608) covers ops the
    same as a real org_admin's would; it is otherwise a read-only, capability-denylisted role
    (REQ-1597) and this grant does not change that.
    """
    rows = (
        await conn.execute_core(
            select(_table_columns_t.c.id, _table_columns_t.c.visible_to)
            .select_from(
                _table_columns_t.join(
                    _registered_tables_t, _table_columns_t.c.table_id == _registered_tables_t.c.id
                )
            )
            .where(_registered_tables_t.c.domain_id == "ops")
        )
    ).all()
    for col_id, visible_to in rows:
        # List membership, not a role-id comparison: this constructs the grant the ops stewards
        # must hold (REQ-1337 forbids a role id in a GATE, not in a grant being written). A list,
        # not a set, keeps the appended order deterministic across runs.
        missing = [s for s in ("org_admin", "sandbox") if s not in visible_to]
        if not missing:
            continue
        await conn.execute_core(
            update(_table_columns_t)
            .where(_table_columns_t.c.id == col_id)
            .values(visible_to=[*visible_to, *missing])
        )


async def _seed_meta_relationships(conn: "Connection") -> None:
    """Seed the meta/ops relationship rows (idempotent, runs after both seeds).

    One loop for both edge shapes: an FK/PK-backed edge and a junction-backed one (REQ-1586) are
    the same ``relationships`` row, differing only in whether the via_* columns are filled, so the
    glossary's edges go in the same way a modeller-declared junction does.
    """
    from provisa.api._meta_seed import RELATIONSHIP_SEED_COLUMNS, meta_relationship_seed_rows

    async def _table_id(source_id: str, table_name: str) -> int | None:
        return (
            await conn.execute_core(
                select(_registered_tables_t.c.id)
                .where(
                    _registered_tables_t.c.source_id == source_id,
                    _registered_tables_t.c.table_name == table_name,
                )
                .limit(1)
            )
        ).scalar()

    for (_src_source, _src_table), (
        _tgt_source,
        _tgt_table,
    ), _via_table, _cols in meta_relationship_seed_rows():
        _src_id = await _table_id(_src_source, _src_table)
        _tgt_id = await _table_id(_tgt_source, _tgt_table)
        # The junction lives in the same control-plane source as the endpoints it joins.
        _via_id = None if _via_table is None else await _table_id(_src_source, _via_table)
        # Empty cross join in the former INSERT...SELECT produced no row; mirror that skip. A
        # declared junction that is not registered yet has no edge to seed either.
        if _src_id is None or _tgt_id is None or (_via_table is not None and _via_id is None):
            continue
        await conn.upsert(
            _relationships_t,
            {
                **_cols,
                "source_table_id": _src_id,
                "target_table_id": _tgt_id,
                "via_table_id": _via_id,
            },
            index_elements=["id"],
            update_columns=list(RELATIONSHIP_SEED_COLUMNS),
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
    """Register ops tables/views in PG registered_tables + table_columns (idempotent).

    The rows point at ``provisa-otel`` / schema ``signals`` — the ``otel`` Iceberg catalog, which
    only the Trino engine has (EngineBackend.has_otel_catalog). On a native engine they name a
    catalog the engine cannot reach, so registering them there advertises unreachable labels: an
    unlabeled Cypher ``MATCH (n)`` unions every label and compiled ``FROM "otel"."signals"."traces"``,
    failing the whole query with ``Catalog "otel" does not exist``. This seed is the only writer of
    those rows, so on an engine without the catalog it removes them — a deployment re-pinned from
    trino to a native engine carries them forward otherwise."""
    if not state.federation_engine.has_otel_catalog:
        await _table_repo.remove_registrations(
            conn, _registered_tables_t.c.source_id == "provisa-otel"
        )
        return

    async def _seed_cols(table_id: Any, table_name: str, cols: list) -> None:
        for col_name, pg_type, is_pk in cols:
            seeded = _COL_DESC[table_name].get(col_name)
            await conn.upsert(
                _table_columns_t,
                {
                    "table_id": table_id,
                    "domain_id": "ops",
                    "column_name": col_name,
                    # ops is a _LOCKDOWN_DOMAINS domain (schema_gen.py): visible_to=[]
                    # means visible to NO role, so a telemetry table seeded empty was in
                    # no role's compilation context and its "ops"."<table>" reference was
                    # never rewritten to a physical ref — the engine then rejected the
                    # report with SCHEMA_NOT_FOUND on schema 'ops'. org_admin stewards ops
                    # and holds the same grant the ops-domain views get (REQ-1386).
                    "visible_to": ["org_admin"],
                    "data_type": pg_type,
                    "is_primary_key": is_pk,
                    "description": seeded,
                },
                index_elements=["table_id", "column_name"],
                update_columns=[],
                set_extra=_keep_edited_description(_table_columns_t.c.description, seeded),
            )

    for tbl_name, cols in _OPS_TABLES.items():
        table_id = await conn.upsert_returning(
            _registered_tables_t,
            {
                "origin": "seed",  # REQ-1919: written when the row is created
                "source_id": "provisa-otel",
                "domain_id": "ops",
                "schema_name": "signals",
                "table_name": tbl_name,
                "description": _TBL_DESC[tbl_name],
            },
            index_elements=["source_id", "schema_name", "table_name"],
            returning="id",
            update_columns=["domain_id"],
            set_extra=_keep_edited_description(
                _registered_tables_t.c.description, _TBL_DESC[tbl_name]
            ),
        )
        await _seed_cols(table_id, tbl_name, cols)
    for view_name, cols, _ in _OPS_VIEWS:
        table_id = await conn.upsert_returning(
            _registered_tables_t,
            {
                "origin": "seed",  # REQ-1919: written when the row is created
                "source_id": "provisa-otel",
                "domain_id": "ops",
                "schema_name": "signals",
                "table_name": view_name,
                "description": _TBL_DESC[view_name],
            },
            index_elements=["source_id", "schema_name", "table_name"],
            returning="id",
            update_columns=["domain_id"],
            set_extra=_keep_edited_description(
                _registered_tables_t.c.description, _TBL_DESC[view_name]
            ),
        )
        await _seed_cols(table_id, view_name, cols)


async def _init_control_planes(
    config_path: str | None,
    initialise: bool = True,
) -> tuple[str, int, str, str]:  # REQ-057, REQ-837
    """Bring up both control planes from config and init tenant schema + audit.

    ``initialise=False`` (REQ-1900) is a worker whose launch has already created the schemas
    (see ``provisa.core.boot_lock``): it builds this process's pools and runs no DDL.

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
    # REQ-1316: every later org runtime reuses THIS engine (see build_org_runtime) — one tenant
    # pool for the whole process, orgs separated by the per-checkout search_path.
    state.tenant_engine = tenant_engine
    state.tenant_db = Database(tenant_engine, name="org", search_path=f"org_{org_id}")
    state.admin_db = await bring_up_platform(
        cp.resolved_platform_url(),
        pool_size=cp.pool_max,
        pool_min=cp.pool_min,
        org_id=org_id,
        initialise=initialise,
    )
    # REQ-1900: settings changed through the admin API are rows in this control plane; every
    # reader in this process resolves them from it (provisa/core/deployment_settings.py).
    from provisa.core import deployment_settings

    deployment_settings.bind(state.admin_db)

    # schema.sql ships in the wheel (pyproject package-data). It is REQUIRED: the PG path runs it
    # verbatim, and on SQLite its presence gates the portable create_all bootstrap. A missing file
    # means a broken package — fail loud here rather than start with an empty control plane (the
    # native runtime once shipped without it and crashed later with "no such table: sources").
    schema_sql_path = Path(__file__).parent.parent / "core" / "schema.sql"
    if not schema_sql_path.exists():
        raise RuntimeError(f"control-plane schema.sql missing from the package: {schema_sql_path}")
    if initialise:
        await init_schema(state.tenant_db, schema_sql_path.read_text(), org_id=org_id)

        from provisa.audit.query_log import init_audit_schema

        await init_audit_schema(state.tenant_db, org_id=org_id)

    host, port, database, username, _pw = cp.tenant_parts()
    # Every backend identifies a database (a PG database name, a SQLite file path, …).
    # Host/user are backend-specific and validated at connect time, not asserted here.
    assert database, "control_plane.tenant_url must specify a database"
    return host or "", port, database, username or ""


async def _seed_built_in_sources(  # REQ-012, REQ-016, REQ-510
    pg_host: str,
    pg_port: int,
    pg_database: str,
    pg_user: str,
    org_id: str | None = None,
    env: str | None = None,
    engine_addressable: bool = True,
) -> None:
    """Seed provisa-admin, provisa-otel, and __derived__ source rows; seed meta domain and ops; compute clusters.

    The provisa-admin source is the control-plane self-catalog; its ``type``/``dialect`` follow the
    control plane's actual backend (``postgresql`` for PG, ``sqlite`` for the file-based demo).

    ``org_id`` scopes the seeded meta/ops domain rows to the org being built (REQ-1266). It defaults
    to ``state.org_id`` (the default/bootstrap org) for the single-org startup path; the per-org
    builder passes the new org's id so meta/ops belong to that org, not the default.

    ``engine_addressable=False`` (REQ-1619) says the coordinator has no address this boot, because
    the shard could not be allocated. The only thing that reads it is provisa-otel's recorded
    host/port, so those two columns are left as the last boot wrote them rather than the seed
    inventing an address: everything the seed exists for — the source rows, the meta domain, ops —
    is control-plane state and is written either way."""
    eff_org = org_id or state.org_id
    assert state.tenant_db is not None
    cp_dialect = state.tenant_db.dialect
    from provisa.federation.engine import configured_engine_endpoint

    engine_endpoint = configured_engine_endpoint() if engine_addressable else None
    # Every column below except `description` is derived from the deployment itself — the control
    # plane's backend and address, the configured engine's name and endpoint — so it is the boot's
    # answer that is authoritative, not whatever a previous boot wrote. Leaving them out of
    # `update_columns` (as this did) pinned the bootstrap org to the deployment it was FIRST
    # started as: re-pinning PROVISA_ENGINE from duckdb to trino left `__derived__.type` and
    # `provisa-otel.dialect` reading duckdb, while an org created after the re-pin read trino — one
    # shared engine described two ways. `description` stays excluded on purpose: it is
    # user-editable, and the set_extra coalesce below restores the seed text only when it is blank.
    _DERIVED_FROM_DEPLOYMENT = ["type", "host", "port", "database", "username", "dialect"]

    async with state.tenant_db.acquire() as _conn:
        _admin_desc = (
            "Provisa internal administration database — stores source registrations, table "
            "metadata, relationships, roles, and governance configuration"
        )
        await _conn.upsert(
            _sources_t,
            {
                "origin": "seed",  # REQ-1919: written when the row is created
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
            update_columns=_DERIVED_FROM_DEPLOYMENT,
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
        _otel_row: dict[str, Any] = {
            "id": "provisa-otel",
            "type": "iceberg",
            "database": "otel",
            "username": "provisa",
            "dialect": _engine_name,
            "description": _otel_desc,
            "origin": "seed",  # REQ-1919: written when the row is created
        }
        if engine_endpoint is not None:
            _otel_row["host"], _otel_row["port"] = engine_endpoint
        # REQ-1619: an address the boot does not have is not a column it can update — naming it in
        # update_columns without supplying it is an update of a value this statement never wrote.
        _otel_update = [c for c in _DERIVED_FROM_DEPLOYMENT if c in _otel_row]
        await _conn.upsert(
            _sources_t,
            _otel_row,
            index_elements=["id"],
            update_columns=_otel_update,
            set_extra={
                "description": _sa_func.coalesce(
                    _sa_func.nullif(_sources_t.c.description, ""), _otel_desc
                )
            },
        )
        await _conn.upsert(
            _sources_t,
            {
                "origin": "seed",  # REQ-1919: written when the row is created
                "id": DERIVED_SOURCE_ID,
                "type": _engine_name,
                "description": (
                    "Provisa-managed virtual views — cross-source SQL views defined and "
                    "published by the data team as governed data products"
                ),
            },
            index_elements=["id"],
            # `type` alone: the sentinel has no address of its own, so the rest of
            # _DERIVED_FROM_DEPLOYMENT is not in the inserted row and naming it here would be
            # an update of a value this statement never supplies.
            update_columns=["type"],
        )
        # The virtual-views sentinel was renamed __provisa__ -> __derived__ (REQ-1328). The
        # rename only changed what this seed writes, so every schema seeded before it still
        # carries the retired id: it is not in _SYSTEM_SOURCE_IDS, so replace-mode cleanup
        # would drop it, but a patch deploy never runs that cleanup and an org schema cloned
        # from one of those carries it forward. It surfaces on the Sources screen as a source
        # the deployment has no engine for, stamped with whatever federation engine was
        # configured the last time it was written. Nothing registers tables against it — this
        # seed is the only writer either id ever had — so retiring the row here is the rename
        # finishing, not a data migration.
        await _source_repo.remove_where(_conn, _sources_t.c.id == "__provisa__")
        # REQ-1900: `--workers N` runs this whole boot sequence in N processes concurrently
        # against the SAME control-plane Postgres, each with its own connection/session. Every
        # step below (view DDL, then reflect_columns on that same view to register its columns)
        # is autocommit-per-statement (Connection._commit_if_autocommit), so a peer worker's
        # concurrent DDL on the SAME view can commit — or transiently rebuild it — in the tiny
        # window between one worker's own CREATE and its own reflect, since Postgres catalog
        # visibility is a property of the OTHER session's DDL, not this session's ordering.
        # Confirmed live under `--workers 8`: `sqlalchemy.exc.NoSuchTableError` on
        # `org_default.usage_ranking`/`org_default.pii_access` from exactly this reflect step.
        # A session advisory lock serializes the whole phase across every worker regardless of
        # the exact interleaving — only one worker's pass ever runs at a time, so by the time any
        # OTHER worker's turn starts, every view this phase creates is fully committed and stable;
        # every worker's own pass is already idempotent (CREATE OR REPLACE VIEW, upserts), so a
        # later worker's now-serialized pass is just a fast no-op confirmation. Session-scoped
        # (`pg_advisory_lock`, unlocked in `finally`), not `pg_advisory_xact_lock`, because these
        # statements autocommit individually rather than running inside one wrapping transaction.
        # Sqlite (the embedded demo tier) has no multi-worker story and no advisory-lock function.
        _seed_lock_held = False
        if cp_dialect == "postgresql":
            await _conn.execute("SELECT pg_advisory_lock($1)", _SEED_DOMAIN_ADVISORY_LOCK_KEY)
            _seed_lock_held = True
        try:
            await _seed_tag_param_values(_conn)  # REQ-1467
            await _seed_meta_domain(_conn, org_id=eff_org, env=env)
            await _seed_ops_pg(_conn)
            await _seed_ops_domain(_conn, org_id=eff_org, env=env)  # REQ-884
            await _ensure_ops_steward_grant(_conn)  # REQ-1386
            await _seed_meta_relationships(_conn)
        finally:
            if _seed_lock_held:
                await _conn.execute("SELECT pg_advisory_unlock($1)", _SEED_DOMAIN_ADVISORY_LOCK_KEY)
        needs_clusters = (
            await _conn.execute_core(
                select(_sa_func.count())
                .select_from(_registered_tables_t)
                .where(_registered_tables_t.c.l1_cluster.is_(None))
            )
        ).scalar()
        if needs_clusters:
            await _compute_and_store_clusters(_conn)
    # REQ-1301: the org registry is a dataset of the root org only — it describes the deployment,
    # and no tenant may read another tenant's roster. Runs outside the connection above because it
    # opens both planes.
    if eff_org == state.org_id:
        await seed_org_registry_view()


async def seed_org_registry_view() -> bool:  # REQ-1301
    """Build the root org's org-registry view and register it in the meta domain. Returns whether
    it landed.

    A deployment whose two control planes live in separate databases (or on a non-PostgreSQL
    backend) cannot carry this view — PostgreSQL has no cross-database join. That topology is
    supported and must still boot, so the reason is logged and startup continues; every other
    failure propagates.
    """
    from provisa.core.org_registry_view import (
        VIEW_NAME,
        RegistryViewUnavailable,
        refresh_org_registry_view,
        root_schema_name,
    )

    assert state.admin_db is not None
    tenant_db = state.tenant_db
    assert tenant_db is not None
    try:
        await refresh_org_registry_view(tenant_db=tenant_db, admin_db=state.admin_db)
        # The registration must name the schema the view was actually built in, which is the root
        # connection's own scope — not a name recomposed from the org id.
        schema_name = root_schema_name(tenant_db)
    except RegistryViewUnavailable as exc:
        logging.getLogger(__name__).info(
            "org registry view not available on this deployment: %s", exc
        )
        return False

    async with tenant_db.acquire() as conn:
        table_id = await conn.upsert_returning(
            _registered_tables_t,
            {
                "origin": "seed",  # REQ-1919: written when the row is created
                "source_id": "provisa-admin",
                "domain_id": "meta",
                "schema_name": schema_name,
                "table_name": VIEW_NAME,
                "description": _TBL_DESC[VIEW_NAME],
            },
            index_elements=["source_id", "schema_name", "table_name"],
            returning="id",
            update_columns=["domain_id"],
            set_extra=_keep_edited_description(
                _registered_tables_t.c.description, _TBL_DESC[VIEW_NAME]
            ),
        )
        cols = await conn.reflect_columns(VIEW_NAME, schema=schema_name)
        for col in cols:
            await conn.upsert(
                _table_columns_t,
                {
                    "table_id": table_id,
                    "domain_id": "meta",
                    "column_name": col["column_name"],
                    "visible_to": [],
                    "data_type": col["data_type"],
                    # A view over the registry has no key of its own — one org appears once per
                    # org_admin — so nothing here is a primary key.
                    "is_primary_key": False,
                    "description": _COL_DESC[VIEW_NAME][col["column_name"]],
                },
                index_elements=["table_id", "column_name"],
                update_columns=["data_type"],
                set_extra=_keep_edited_description(
                    _table_columns_t.c.description, _COL_DESC[VIEW_NAME][col["column_name"]]
                ),
            )
    return True


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
                        _sources_t.c.path.label("source_path"),
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
                    # REQ-1651: a SQLite file has no information_schema and is not in the CP
                    # connection, and the engine's schema rebuild never wrote its keys either -- so
                    # every file-backed table (the demo's pets among them) landed without a
                    # PRIMARY KEY. PRAGMA table_info is the file's own constraint declaration.
                    from provisa.core.secrets import resolve_secrets
                    from provisa.federation.connector_sqlite import primary_key_columns

                    _pk_cols = primary_key_columns(
                        resolve_secrets(_pk_t["source_path"] or ""), _pk_t["table_name"]
                    )
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
