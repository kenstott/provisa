# Copyright (c) 2026 Kenneth Stott
# Canary: 2ae8ef6d-2550-4cb3-bd42-e938c6f76e26
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
#
"""Extracted bodies for the heaviest admin Mutation resolvers.

Strawberry cannot merge resolvers across base classes, so the ``Mutation`` type
stays one class in ``schema_mutation.py`` with thin delegator methods; the large
bodies live here as free functions to keep that module under the length gate.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from sqlalchemy import select, update

from provisa.core.schema_org import registered_tables, sources, table_meta_links
from provisa.api.admin.types import MutationResult, TableInput
from provisa.api.admin.schema_helpers import (
    _dataset_ownership_conflict,
    _domain_table_conflict,
    _get_pool,
    _maybe_migrate_sqlite,
    _rebuild_schemas,
)
from provisa.api.admin._table_ops import _build_columns_for_input
from provisa.api.admin._live_mappers import table_model_from_input as _table_model_from_input
from provisa.api.admin.schema_common import _queue_creation_request, _sync_view_mv

if TYPE_CHECKING:
    import asyncpg

    from strawberry.types.info import Info as StrawberryInfo

    from provisa.core.database import Connection


async def register_table(
    info: StrawberryInfo, input: TableInput
) -> MutationResult:  # REQ-013, REQ-016, REQ-252, REQ-366, REQ-413, REQ-432, REQ-433, REQ-434
    import logging

    logging.getLogger(__name__).warning(
        "[DEBUG] register_table called: table_name=%s, source_id=%s, domain_id=%s",
        input.table_name,
        input.source_id,
        input.domain_id,
    )
    from provisa.api.admin.capabilities import require_capability

    if input.view_sql:
        # view registration: create_view or query_development suffice
        from provisa.api.admin.capabilities import _identity_from_info, _resolved_capabilities
        from provisa.api.app import state as _cap_state

        identity = _identity_from_info(info)
        if identity is not None and getattr(identity, "user_id", "anonymous") != "anonymous":
            caps = _resolved_capabilities(identity, _cap_state)
            if not (caps & {"create_view", "query_development", "admin", "superadmin"}):
                # REQ-434/366: lacking view-create authority queues a request.
                return await _queue_creation_request(info, "view", "create_view", input)
    else:
        require_capability(info, "table_registration", domain_id=input.domain_id)
    from provisa.core.repositories import table as table_repo

    pool = await _get_pool()
    columns, _col_err = await _build_columns_for_input(pool, input)
    if _col_err is not None:
        return _col_err
    alias = input.alias or None
    if not alias:
        from provisa.compiler.naming import apply_convention

        async with pool.acquire() as conn:
            _sres = await conn.execute_core(
                select(sources.c.gql_naming_convention).where(sources.c.id == input.source_id)
            )
            src = _sres.fetchone()
        convention = (src.gql_naming_convention if src else None) or "apollo_graphql"
        alias = apply_convention(input.table_name, convention)

    from provisa.core.models import ColumnPreset as ColumnPresetModel

    presets = [
        ColumnPresetModel(
            column=cp.column,
            source=cp.source,
            name=cp.name,
            value=cp.value,
            data_type=cp.data_type,
        )
        for cp in input.column_presets
    ]
    model = _table_model_from_input(input, columns, presets, alias)
    async with pool.acquire() as conn:
        _conn = cast("Connection", conn)
        _conflict = await _domain_table_conflict(
            _conn, model.domain_id, model.table_name, model.source_id, model.schema_name, alias
        )
        if _conflict:
            return MutationResult(success=False, message=_conflict)
        _owner_conflict = await _dataset_ownership_conflict(
            _conn, model.source_id, model.table_name, model.domain_id
        )
        if _owner_conflict:
            return MutationResult(success=False, message=_owner_conflict)
        if input.source_id == "__provisa__":
            from provisa.api.app import state

            await _conn.upsert(
                sources,
                {
                    "id": "__provisa__",
                    "type": state.federation_engine.name,
                    "description": "Provisa-managed virtual views — cross-source SQL views defined and published by the data team as governed data products",
                },
                index_elements=["id"],
                update_columns=[],
            )
        table_id = await table_repo.upsert(_conn, model)
        _sres = await _conn.execute_core(
            select(sources.c.type, sources.c.path).where(sources.c.id == input.source_id)
        )
        _srow = _sres.fetchone()
        src_row = dict(_srow._mapping) if _srow is not None else None
        await _maybe_migrate_sqlite(
            src_row, _conn, input.source_id, input.table_name, input.schema_name
        )
        if input.domain_id != "meta":
            _mres = await _conn.execute_core(
                select(registered_tables.c.id).where(
                    registered_tables.c.source_id == "provisa-admin",
                    registered_tables.c.domain_id == "meta",
                    registered_tables.c.table_name == "registered_tables",
                )
            )
            meta_rt_id = _mres.scalar()
            if meta_rt_id:
                await _conn.upsert(
                    table_meta_links,
                    {"source_table_id": table_id, "target_table_id": meta_rt_id},
                    index_elements=["source_table_id"],
                    update_columns=[],
                )

        import os as _os

        if _os.environ.get("PROVISA_AUTO_TRACK_FK", "true").lower() not in ("0", "false", "no"):
            from provisa.discovery.fk_introspect import auto_register_fk_relationships
            from provisa.api.app import state as _state

            source_type = (src_row["type"] if src_row else None) or ""
            _naming_cfg = getattr(getattr(_state, "config", None), "naming", None)
            _v2_style = bool(getattr(_naming_cfg, "hasura_v2_relationship_style", False))
            fk_count = await auto_register_fk_relationships(
                _state.source_pools,
                source_type,
                input.source_id,
                input.schema_name,
                input.table_name,
                cast("asyncpg.Connection", _conn),  # provisa Connection proxies asyncpg calls
                hasura_v2_relationship_style=_v2_style,
            )
            if fk_count:
                import logging as _logging

                _logging.getLogger(__name__).info(
                    "Auto-tracked %d FK relationship(s) for %s.%s",
                    fk_count,
                    input.schema_name,
                    input.table_name,
                )

    if input.view_sql and input.materialize:
        _sync_view_mv(
            input.table_name, input.view_sql, input.mv_refresh_interval, input.change_signal
        )

    await _rebuild_schemas()
    # Schema-currency reconcile (REQ-846/932): a UI (re)registration re-enters the convergent
    # reconcile — the new/changed table's landing schema is created (or recreated on a column
    # drift) and its read view attached. Same primitive as the boot pass. Best-effort: a store
    # hiccup logs but does not fail the registration.
    try:
        from provisa.api.app import state as _rc_state

        await _rc_state.federation_engine.reconcile_landed_tables()
    except Exception:
        logging.getLogger(__name__).exception("landed-table reconcile after registration failed")
    return MutationResult(
        success=True,
        message=f"Table {input.table_name!r} registered (id={table_id})",
    )


async def deploy_view_to_db(info: StrawberryInfo, table_id: int) -> MutationResult:
    """Promote a virtual Provisa view to a real database view on its underlying native source."""
    from provisa.api.admin.capabilities import require_capability

    require_capability(info, "table_registration")

    from provisa.api.app import state
    from provisa.compiler.naming import domain_to_sql_name
    from provisa.transpiler.transpile import transpile

    pool = await _get_pool()
    async with pool.acquire() as conn:
        _res = await conn.execute_core(
            select(
                registered_tables.c.id,
                registered_tables.c.source_id,
                registered_tables.c.domain_id,
                registered_tables.c.schema_name,
                registered_tables.c.table_name,
                registered_tables.c.alias,
                registered_tables.c.view_sql,
            ).where(registered_tables.c.id == table_id)
        )
        row = _res.fetchone()
    if not row:
        return MutationResult(success=False, message=f"Table {table_id} not found")
    if row.source_id != "__provisa__":
        return MutationResult(
            success=False,
            message="Table is not a virtual Provisa view (source_id != '__provisa__')",
        )
    if not row.view_sql:
        return MutationResult(success=False, message="Table has no view_sql")

    view_sql = row.view_sql
    view_name = row.alias or row.table_name

    # Fetch all non-provisa registered tables with domain_id, source info
    async with pool.acquire() as conn:
        _ares = await conn.execute_core(
            select(
                registered_tables.c.id,
                registered_tables.c.source_id,
                registered_tables.c.domain_id,
                registered_tables.c.schema_name,
                registered_tables.c.table_name,
                registered_tables.c.alias,
                sources.c.type.label("source_type"),
            )
            .select_from(
                registered_tables.join(sources, sources.c.id == registered_tables.c.source_id)
            )
            .where(registered_tables.c.source_id != "__provisa__")
        )
        all_tables = [dict(r._mapping) for r in _ares.fetchall()]

    # Build replacement dict: virtual ref → physical ref, tracking source_ids hit
    # Sort by length descending so longest match wins
    replacements: list[
        tuple[str, str, str, str]
    ] = []  # (virtual_ref, physical_ref, source_id, schema_name)
    for t in all_tables:
        domain_sql = domain_to_sql_name(t["domain_id"])
        alias_or_name = t["alias"] or t["table_name"]
        virtual_ref = f'"{domain_sql}"."{alias_or_name}"'
        physical_ref = f'"{t["schema_name"]}"."{t["table_name"]}"'
        replacements.append((virtual_ref, physical_ref, t["source_id"], t["schema_name"]))

    # Apply replacements (longest virtual_ref first), track which sources are hit
    physical_sql = view_sql
    hit_sources: dict[str, str] = {}  # source_id → schema_name
    for virtual_ref, physical_ref, source_id, schema_name in sorted(
        replacements, key=lambda x: len(x[0]), reverse=True
    ):
        if virtual_ref in physical_sql:
            physical_sql = physical_sql.replace(virtual_ref, physical_ref)
            hit_sources[source_id] = schema_name

    if not hit_sources:
        return MutationResult(success=False, message="no recognized table references")
    if len(hit_sources) > 1:
        return MutationResult(
            success=False,
            message=f"view spans multiple sources: {', '.join(sorted(hit_sources))}",
        )

    target_source_id = next(iter(hit_sources))
    target_schema = hit_sources[target_source_id]

    if not state.source_pools.has(target_source_id):
        return MutationResult(
            success=False, message=f"source {target_source_id!r} has no active connection"
        )

    dialect = state.source_pools.dialect_for(target_source_id) or "postgres"
    native_sql = transpile(physical_sql, dialect)

    ddl = f'CREATE OR REPLACE VIEW "{target_schema}"."{view_name}" AS {native_sql}'
    await state.source_pools.execute_ddl(target_source_id, ddl)

    async with pool.acquire() as conn:
        await conn.execute_core(
            update(registered_tables)
            .where(registered_tables.c.id == table_id)
            .values(source_id=target_source_id, schema_name=target_schema, view_sql=None)
        )

    await _rebuild_schemas()
    return MutationResult(
        success=True,
        message=f"View '{view_name}' deployed to {target_source_id!r} schema '{target_schema}'",
    )
