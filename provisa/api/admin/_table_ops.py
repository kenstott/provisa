# Copyright (c) 2026 Kenneth Stott
# Canary: 2ae8ef6d-2550-4cb3-bd42-e938c6f76e26
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Column-resolution and table-registration helpers extracted from schema.py (REQ-016, REQ-252)."""

# complexity-gate: allow-ble=1 reason="REQ-252 discovery adapter errors are heterogeneous; broad catch is mandated by adapter contract"

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import or_, select

from provisa.core.schema_org import registered_tables, roles, sources, table_columns

if TYPE_CHECKING:
    from provisa.core.database import Database


async def _get_pool() -> "Database":
    from provisa.api.app import state

    assert state.tenant_db is not None
    return state.tenant_db


def _build_column_models(columns: list) -> list:
    from provisa.core.models import Column as ColumnModel

    return [
        ColumnModel(
            name=c.name,
            visible_to=c.visible_to,
            writable_by=c.writable_by,
            unmasked_to=c.unmasked_to,
            mask_type=c.mask_type,
            mask_pattern=c.mask_pattern,
            mask_replace=c.mask_replace,
            mask_value=c.mask_value,
            mask_precision=c.mask_precision,
            alias=c.alias,
            description=c.description,
            data_type=getattr(c, "data_type", None),
            native_filter_type=c.native_filter_type,
            is_primary_key=c.is_primary_key,
            is_foreign_key=c.is_foreign_key,
            is_alternate_key=c.is_alternate_key,
            scope=getattr(c, "scope", "domain"),
        )
        for c in columns
    ]


async def _discover_columns_for_registration(source_id: str, table_name: str) -> list[dict]:
    """REQ-252: infer columns from a live NoSQL source via its adapter discover_schema.

    Reuses the same dispatch as the admin discovery endpoint. The table name is the target
    index/collection/keyspace. Raises (HTTPException or transport error) on failure so the
    caller can refuse to register an empty schema.
    """
    from provisa.api.admin.discovery_schema import DiscoverRequest, _call_discover
    from provisa.source_adapters.registry import get_adapter

    pool = await _get_pool()
    async with pool.acquire() as conn:
        _res = await conn.execute_core(select(sources).where(sources.c.id == source_id))
        _r = _res.fetchone()
    row = dict(_r._mapping) if _r is not None else None
    if row is None:
        raise ValueError(f"source {source_id!r} not found for discovery")
    adapter = get_adapter(row["type"])
    hints = DiscoverRequest(
        collection=table_name, index=table_name, keyspace=table_name, table=table_name
    )
    return _call_discover(adapter, row["type"], row, hints)


async def _resolve_ref_type_map(conn, ref_names: set[str]) -> dict[str, str]:
    """Map column_name -> data_type for columns of registered tables referenced by name/alias."""
    type_map: dict[str, str] = {}
    if ref_names:
        _res = await conn.execute_core(
            select(table_columns.c.column_name, table_columns.c.data_type)
            .select_from(
                table_columns.join(
                    registered_tables, registered_tables.c.id == table_columns.c.table_id
                )
            )
            .where(
                or_(
                    registered_tables.c.table_name.in_(list(ref_names)),
                    registered_tables.c.alias.in_(list(ref_names)),
                ),
                table_columns.c.data_type.is_not(None),
            )
        )
        for r in _res.fetchall():
            type_map.setdefault(r.column_name, r.data_type)
    return type_map


async def _introspect_view_columns(conn, view_sql: str, default_roles: list[str]) -> list:
    """Derive a view's columns from its SQL when the caller supplies none.

    Output column names come from the SELECT projection (SQLGlot). Each column's
    data_type is resolved from the stored columns of the registered tables the view
    references (by name match), falling back to varchar for expressions/aggregates the
    type can't be traced to. visible_to defaults to all roles. This makes a view's
    schema self-describing — the view SQL is the source of truth for its columns.
    """
    import sqlglot
    import sqlglot.errors
    import sqlglot.expressions as exp

    from provisa.core.models import Column as ColumnModel

    try:
        tree = sqlglot.parse_one(view_sql, read="postgres")
    except sqlglot.errors.ParseError:
        return []
    output_names = list(getattr(tree, "named_selects", []) or [])
    if not output_names:
        return []

    ref_names = {t.name for t in tree.find_all(exp.Table) if t.name}
    type_map = await _resolve_ref_type_map(conn, ref_names)

    return [
        ColumnModel(name=n, data_type=type_map.get(n, "varchar"), visible_to=list(default_roles))
        for n in output_names
    ]


async def _ensure_view_column_types(conn, view_sql: str, columns: list) -> list:
    """Fill any null/empty data_type on caller-supplied view columns.

    The admin UI snapshots a view's columns by running its SQL; a column whose type
    can't be traced (e.g. it references a source not yet introspected) arrives with
    data_type=None. introspect_tables requires every SQL-catalog column to have a
    type, so resolve nulls the same way _introspect_view_columns does — from the
    referenced tables, else varchar — so a view can never be persisted schema-broken.
    """
    if not any(getattr(c, "data_type", None) in (None, "") for c in columns):
        return columns
    import sqlglot
    import sqlglot.errors
    import sqlglot.expressions as exp

    try:
        tree = sqlglot.parse_one(view_sql, read="postgres")
    except sqlglot.errors.ParseError:
        tree = None
    ref_names = {t.name for t in tree.find_all(exp.Table) if t.name} if tree else set()
    type_map = await _resolve_ref_type_map(conn, ref_names)
    for c in columns:
        if getattr(c, "data_type", None) in (None, ""):
            c.data_type = type_map.get(c.name, "varchar")
    return columns


async def _build_columns_for_input(pool, input) -> tuple[list, object]:
    """Resolve the effective column list for a table registration or update.

    Handles three mutually exclusive column-source paths:
      1. view_sql with no caller columns  → introspect from SQL
      2. view_sql with caller columns     → fill missing data_types
      3. discover=True (NoSQL sources)    → merge discovered schema onto provided columns

    Returns (columns, None) on success or ([], MutationResult) on discovery failure.
    """
    from provisa.api.admin.types import MutationResult

    columns = _build_column_models(input.columns)
    if input.view_sql and not columns:
        async with pool.acquire() as _vc:
            _roles = [r.id for r in (await _vc.execute_core(select(roles.c.id))).fetchall()]
            columns = await _introspect_view_columns(_vc, input.view_sql, _roles or ["admin"])
    elif input.view_sql and columns:
        async with pool.acquire() as _vc:
            columns = await _ensure_view_column_types(_vc, input.view_sql, columns)
    elif getattr(input, "discover", False):
        from provisa.api.admin.types import ColumnInput as _ColInput
        from provisa.discovery.column_inference import merge_discovered_columns

        try:
            discovered = await _discover_columns_for_registration(input.source_id, input.table_name)
        except Exception as e:
            return [], MutationResult(success=False, message=f"Schema discovery failed: {e}")
        discovered_models = _build_column_models(
            [_ColInput(name=d["name"], visible_to=[]) for d in discovered]
        )
        columns = merge_discovered_columns(columns, discovered_models)
    return columns, None
