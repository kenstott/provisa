# Copyright (c) 2026 Kenneth Stott
# Canary: f829b2d8-06bc-4381-80e7-768bf0650a60
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Table repository — CRUD for registered tables and columns, via SQLAlchemy Core (dialect-portable)."""

# Requirements: REQ-013, REQ-014, REQ-016, REQ-133, REQ-155, REQ-156, REQ-260, REQ-334, REQ-393, REQ-399

from typing import TYPE_CHECKING

from sqlalchemy import delete as _delete, select, update

from provisa.core import domain_policy
from provisa.core.models import Table
from provisa.core.repositories import data_product as data_product_repo
from provisa.core.repositories import glossary as glossary_repo
from provisa.core.repositories.integrity import (
    Dependent,
    ObjectRef,
    column_dependents,
    guard,
    remove_parts,
    view_loop,
)
from provisa.core.repositories.origin import require as require_origin
from provisa.core.repositories.origin import take_over
from provisa.core.schema_org import registered_tables, roles, table_columns, tag_assignments
from provisa.security.rights import Capability

if TYPE_CHECKING:
    from provisa.core.database import Connection


async def _control_plane_role_ids(conn: "Connection") -> set[str]:  # REQ-1337
    """The ids of every role in THIS org's schema holding ``cross_org`` — the CONTROL-PLANE roles.

    Read from the ``roles`` table on the caller's own connection, never from ``state.roles``: the
    in-memory map is populated by build_org_runtime AFTER the config load has already registered
    every table, so an in-memory read during the load resolves an empty map and strips nothing —
    which is how a tenant org came up with ``platform_admin`` on its column grants. schema.sql
    seeds the roles table before any table registration runs, so the DB answer is always resolved.
    """
    rows = (await conn.execute_core(select(roles.c.id, roles.c.capabilities))).fetchall()
    return {r[0] for r in rows if Capability.CROSS_ORG.value in (r[1] or [])}


def _ungrant_control_plane(role_ids: list[str] | None, control_plane: set[str]) -> list[str]:
    """REQ-1297/REQ-1337: a CONTROL-PLANE role can never appear in a column grant.

    This is the single write path for ``table_columns``, so every registration source — config load,
    admin GraphQL, introspection, view creation, the modeling registrar — passes through here. The
    shipped install config named platform_admin in every column's ``visible_to``, which is what put
    it on the column chips of a tenant org's tables. A column grant is a data-plane right; a role
    holding ``cross_org`` is control-plane only and holds none. The membership test is that RIGHT,
    never a role name.
    """
    return [r for r in (role_ids or []) if r not in control_plane]


_COLUMN_PROJECTION = [
    table_columns.c.column_name,
    table_columns.c.domain_id,
    table_columns.c.data_type,
    table_columns.c.alias,
    table_columns.c.description,
    table_columns.c.visible_to,
    table_columns.c.writable_by,
    table_columns.c.unmasked_to,
    table_columns.c.mask_type,
    table_columns.c.mask_pattern,
    table_columns.c.mask_replace,
    table_columns.c.mask_value,
    table_columns.c.mask_precision,
    table_columns.c.native_filter_type,
    table_columns.c.is_primary_key,
    table_columns.c.is_foreign_key,
    table_columns.c.is_alternate_key,
    table_columns.c.object_fields,
    table_columns.c.scope,
    table_columns.c.gql_selection,
    table_columns.c.epoch_unit,
]


async def _load_columns(conn: "Connection", table_id: int) -> list[dict]:
    result = await conn.execute_core(
        select(*_COLUMN_PROJECTION)
        .where(table_columns.c.table_id == table_id)
        .order_by(table_columns.c.id)
    )
    return [dict(r._mapping) for r in result.fetchall()]


class TableDeleteRefused(Exception):
    """A table or view that may not be deleted because other objects refer to it;
    ``dependents`` lists them."""

    def __init__(self, table_id: int, name: str, dependents: list[Dependent]) -> None:
        self.table_id = table_id
        self.name = name
        self.dependents = dependents
        named = ", ".join(f"{d.ref.kind} {d.ref.id}" for d in dependents)
        super().__init__(f"Table {name!r} is still referred to by: {named}")


class ColumnDropRefused(ValueError):
    """A re-registration that would drop columns other objects refer to; ``columns`` maps each
    such column to its dependents. Nothing was changed."""

    def __init__(self, table_name: str, columns: dict[str, list[Dependent]]) -> None:
        self.table_name = table_name
        self.columns = columns
        named = "; ".join(
            f"{column} (referred to by: "
            + ", ".join(f"{d.ref.kind} {d.ref.id}" for d in dependents)
            + ")"
            for column, dependents in columns.items()
        )
        super().__init__(
            f"Table {table_name!r} cannot drop column(s) that are still referred to: {named}"
        )

    def report(self) -> dict[str, list[dict]]:
        return {
            column: [d.as_dict() for d in dependents] for column, dependents in self.columns.items()
        }


class ViewLoopRefused(ValueError):
    """A view whose SQL would read the view itself, directly or through other views;
    ``loop`` is the view names in reading order, starting and ending at the view."""

    def __init__(self, loop: list[str]) -> None:
        self.loop = loop
        super().__init__(
            f"View {loop[0]!r} would read itself: {' -> '.join(loop)}. A view cannot read "
            "itself through other views; change one of them."
        )


async def upsert(
    conn: "Connection", table: Table, *, origin: str
) -> int | None:  # REQ-013, REQ-016, REQ-133, REQ-155, REQ-156, REQ-260, REQ-334, REQ-393, REQ-399
    """Upsert a registered table and its columns. Returns the table row id.

    REQ-1919: ``origin`` says where the table comes from (``repositories.origin``). It is
    written when the table is CREATED and left alone after, except that a config load takes over
    a table made through the admin.

    REQ-1914: one transaction. The table row, the wholesale column replace and the glossary refs
    commit together, so the config stamp they advance is seen only with the finished table —
    another worker never reloads a table whose columns are deleted and not yet re-inserted.

    REQ-1918: a view whose SQL would read the view itself through other views is refused
    (:class:`ViewLoopRefused`) — it cannot be evaluated, and it is the one way objects could come
    to block each other's deletion in a circle. A registration that would drop a column other
    objects refer to is refused (:class:`ColumnDropRefused`). This is the write path the admin
    mutations and the config loader share, so both refuse them."""
    async with conn.transaction():
        view_sql = getattr(table, "view_sql", None)
        if view_sql:
            loop = await view_loop(conn, table.table_name, view_sql)
            if loop:
                raise ViewLoopRefused(loop)
        return await _upsert(conn, table, require_origin(origin))


async def _upsert(conn: "Connection", table: Table, origin: str) -> int | None:
    domain_id = domain_policy.resolve_domain_id(table.domain_id)
    if getattr(table, "row_materialize", False):
        # A table a materialized view reads may not become row-level: refused here, the last
        # write gate, naming the views (provisa/mv/readable_inputs.py).
        from provisa.mv.readable_inputs import require_row_level_switch_allowed

        await require_row_level_switch_allowed(conn, table)
    # REQ-1634: a DataProduct's member tables must all share its domain_id — a table cannot
    # reference a DataProduct in a different domain. Enforced at the last write gate so every
    # caller (config load, admin GraphQL, introspection) is covered, not only the picker UI.
    product_id = getattr(table, "product_id", None)
    if product_id is not None:
        # A parameterized table (native-filter query/path-param column) is a function f(args) ->
        # rows with no snapshot (provisa/events/boot.py) — it never lands and so can never back a
        # Share/listing export. Membership must be rejected here, the same last-write gate that
        # enforces domain matching, so every caller is covered.
        if any(getattr(c, "native_filter_type", None) is not None for c in table.columns):
            raise ValueError(
                f"table {table.table_name} has a native-filter (parameterized) column and cannot "
                f"be a member of data product {product_id!r} — it is never landed"
            )
        product = await data_product_repo.get(conn, product_id)
        if product is None:
            raise ValueError(f"data product {product_id!r} does not exist")
        if product["domain_id"] != domain_id:
            raise ValueError(
                f"table {table.table_name} is in domain {domain_id!r} but data product "
                f"{product_id!r} belongs to domain {product['domain_id']!r}"
            )
    # JSON columns take Python objects directly — SQLAlchemy serializes per dialect.
    values = {
        "source_id": table.source_id,
        "domain_id": domain_id,
        "schema_name": table.schema_name,
        "table_name": table.table_name,
        "alias": getattr(table, "alias", None),
        "description": getattr(table, "description", None),
        "watermark_column": getattr(table, "watermark_column", None),
        "column_presets": [p.model_dump() for p in getattr(table, "column_presets", [])],
        "unique_constraints": [
            u.model_dump() for u in getattr(table, "unique_constraints", [])
        ],  # REQ-1093
        "view_sql": getattr(table, "view_sql", None),
        "dq_contract": getattr(table, "dq_contract", None),  # REQ-1443
        "view_metrics": (
            vm.model_dump() if (vm := getattr(table, "view_metrics", None)) else None
        ),  # REQ-1318
        "product_id": getattr(table, "product_id", None),  # REQ-1634
        "materialize": getattr(table, "materialize", False),
        "mv_refresh_interval": getattr(table, "mv_refresh_interval", 300),
        "mv_debounce_quiet": getattr(table, "mv_debounce_quiet", 0.0),  # REQ-963
        "mv_debounce_max_delay": getattr(table, "mv_debounce_max_delay", 5.0),  # REQ-963
        "push_debounce_quiet": getattr(table, "push_debounce_quiet", 0.0),  # REQ-1733
        "push_debounce_max_delay": getattr(table, "push_debounce_max_delay", 5.0),  # REQ-1733
        "mv_consistency": getattr(table, "mv_consistency", "shared"),  # REQ-879
        "mv_preprocess": getattr(table, "mv_preprocess", None),  # REQ-957
        "mv_bitemporal_mode": getattr(table, "mv_bitemporal_mode", None),  # REQ-1162
        "mv_bitemporal_key": getattr(table, "mv_bitemporal_key", []),  # REQ-1162
        "mv_persist": getattr(table, "mv_persist", "replace"),  # REQ-965
        "mv_primary_key": getattr(table, "mv_primary_key", []),  # REQ-970
        "mv_incremental": getattr(table, "mv_incremental", False),  # REQ-969
        "mv_calendar": getattr(table, "mv_calendar", None),  # REQ-962
        "mv_grain": getattr(table, "mv_grain", None),  # REQ-962/1168
        "mv_allowed_lateness": getattr(table, "mv_allowed_lateness", 0.0),  # REQ-961
        "mv_expected_events": getattr(table, "mv_expected_events", None),  # REQ-961
        "mv_business_day_grain": getattr(table, "mv_business_day_grain", False),  # REQ-962
        "modeling_role": getattr(table, "modeling_role", None),  # REQ-1320
        "modeling_history": getattr(table, "modeling_history", None),  # REQ-1320
        "enable_aggregates": getattr(table, "enable_aggregates", False),
        "enable_group_by": getattr(table, "enable_group_by", False),
        "live": table.live.model_dump() if table.live else None,
        "change_signal": getattr(table, "change_signal", None),
        "probe_query": getattr(table, "probe_query", None),
        "probe_type": getattr(table, "probe_type", None),
        "load_protected": getattr(table, "load_protected", None),  # REQ-1141
        "replicate": getattr(table, "replicate", None),  # REQ-826
        "off_peak_window": getattr(table, "off_peak_window", None),  # REQ-1141
        "off_peak_tz": getattr(table, "off_peak_tz", None),  # REQ-1141
        # REQ-1865: never written here despite fetch_tables() SELECTing both columns and
        # _validate_row_materialize gating registration on them -- every config-declared
        # row_materialize=True table silently persisted as False, so the row-level cache could
        # never engage no matter what the read side (registry_view.py, ensure_resident,
        # ensure_rows_resident) correctly did with it. Confirmed live: querying the control-plane
        # DB directly showed row_materialize='f'/cache_ttl=NULL for 5 tables fragment.yaml
        # declared row_materialize: true, cache_ttl: 300 on.
        "row_materialize": getattr(table, "row_materialize", False),
        "cache_ttl": getattr(table, "cache_ttl", None),
        "role_ttl": dict(table.role_ttl),  # REQ-1907
    }
    _update_columns = [
        "domain_id",
        "alias",
        "description",
        "watermark_column",
        "column_presets",
        "unique_constraints",  # REQ-1093
        "view_sql",
        "dq_contract",  # REQ-1443
        "view_metrics",  # REQ-1318
        "product_id",
        "materialize",
        "mv_refresh_interval",
        "mv_debounce_quiet",
        "mv_debounce_max_delay",
        "push_debounce_quiet",
        "push_debounce_max_delay",
        "mv_consistency",
        "mv_preprocess",
        "mv_bitemporal_mode",  # REQ-1162
        "mv_bitemporal_key",  # REQ-1162
        "mv_persist",  # REQ-965
        "mv_primary_key",  # REQ-970
        "mv_incremental",  # REQ-969
        "mv_calendar",  # REQ-962
        "mv_grain",  # REQ-962/1168
        "mv_allowed_lateness",  # REQ-961
        "mv_expected_events",  # REQ-961
        "mv_business_day_grain",  # REQ-962
        "modeling_role",  # REQ-1320
        "modeling_history",  # REQ-1320
        "enable_aggregates",
        "enable_group_by",
        "live",
        "change_signal",
        "probe_query",
        "probe_type",
        "load_protected",  # REQ-1141
        "replicate",  # REQ-826
        "off_peak_window",  # REQ-1141
        "off_peak_tz",  # REQ-1141
        "row_materialize",  # REQ-1865
        "cache_ttl",  # REQ-1865
        "role_ttl",  # REQ-1907
    ]
    table_id = await conn.upsert_returning(
        registered_tables,
        {**values, "origin": origin},  # REQ-1919: on INSERT only — not among the update columns
        index_elements=["source_id", "schema_name", "table_name"],
        returning="id",
        update_columns=_update_columns,
    )
    await take_over(
        conn,
        registered_tables,
        (registered_tables.c.id == table_id,),
        kind="table",
        ident=f"{table.source_id}.{table.schema_name}.{table.table_name}",
        origin=origin,
    )

    # Column data_type is resolved at registration (design time) and PERSISTS: a column type once
    # resolved (by the type-introspection user-assist) survives a config reload even though the YAML
    # carries no type. Capture the currently-stored types before the column replace and reuse any
    # that the incoming config leaves unset (REQ-471) — never null a resolved type back out.
    _existing_types = {
        r.column_name: r.data_type
        for r in (
            await conn.execute_core(
                select(table_columns.c.column_name, table_columns.c.data_type).where(
                    table_columns.c.table_id == table_id
                )
            )
        ).fetchall()
    }
    # REQ-1918: a column this registration no longer lists is dropped. While a relationship is
    # keyed on it, or a view, materialized view or metric names it, the registration is refused
    # naming them; the transaction around this call undoes the table row's update. A dropped
    # column's tag assignments are its parts and go with it.
    _dropped = sorted(set(_existing_types) - {col.name for col in table.columns})
    _referred: dict[str, list[Dependent]] = {}
    for _column in _dropped:
        _dependents = await column_dependents(conn, table_id, _column)
        if _dependents:
            _referred[_column] = _dependents
    if _referred:
        raise ColumnDropRefused(table.table_name, _referred)
    if _dropped:
        await conn.execute_core(
            _delete(tag_assignments).where(
                tag_assignments.c.table_id == table_id,
                tag_assignments.c.column_name.in_(_dropped),
            )
        )
    # Replace columns: delete existing, insert new
    _control_plane = await _control_plane_role_ids(conn)
    await conn.execute_core(_delete(table_columns).where(table_columns.c.table_id == table_id))
    for col in table.columns:
        object_fields_raw = getattr(col, "object_fields", [])
        object_fields = [
            f.model_dump() if hasattr(f, "model_dump") else f for f in object_fields_raw
        ]
        _data_type = getattr(col, "data_type", None) or _existing_types.get(col.name)
        # REQ-1426: a column without a type is not registrable. Every writer resolves the type
        # before it gets here (source introspection, SQLGlot annotation for views, the remote
        # mappers' own type maps); persisting NULL made the catalog render "unknown" and left the
        # SQL layer with no type to compile against. Refuse loudly at the last gate.
        if not _data_type:
            raise ValueError(
                f"column {table.table_name}.{col.name} has no data_type; "
                "resolve the type at registration — an untyped column cannot be persisted"
            )
        await conn.execute_core(
            table_columns.insert().values(
                table_id=table_id,
                domain_id=domain_id,
                column_name=col.name,
                visible_to=_ungrant_control_plane(col.visible_to, _control_plane),
                writable_by=_ungrant_control_plane(getattr(col, "writable_by", []), _control_plane),
                unmasked_to=_ungrant_control_plane(getattr(col, "unmasked_to", []), _control_plane),
                mask_type=getattr(col, "mask_type", None),
                mask_pattern=getattr(col, "mask_pattern", None),
                mask_replace=getattr(col, "mask_replace", None),
                mask_value=getattr(col, "mask_value", None),
                mask_precision=getattr(col, "mask_precision", None),
                alias=getattr(col, "alias", None),
                description=getattr(col, "description", None),
                data_type=_data_type,
                path=getattr(col, "path", None),
                native_filter_type=getattr(col, "native_filter_type", None),
                is_primary_key=getattr(col, "is_primary_key", False),
                is_foreign_key=getattr(col, "is_foreign_key", False),
                is_alternate_key=getattr(col, "is_alternate_key", False),
                object_fields=object_fields,
                scope=getattr(col, "scope", "domain"),
                gql_selection=getattr(col, "gql_selection", None),
                epoch_unit=getattr(col, "epoch_unit", None),
            )
        )
    # REQ-1387: this is the single write path for table_columns, so the glossary term
    # lifecycle (create/link on add, remove-or-deprecate on departure) rides it here —
    # every registration source gets it and none can forget it. Native-filter pseudo-columns
    # (_nf_<arg> / native_filter_type) are query-parameter machinery, not business fields,
    # so they derive no terms; the table's business name qualifies TOO-GENERIC phrases.
    await glossary_repo.sync_table_refs(
        conn,
        table_id,
        # REQ-1581: the term derives from the column's BUSINESS name -- its alias where the
        # modeller set one, the physical name where they did not. Aliasing the column is what
        # makes the data self-describing everywhere it is read; the glossary follows it rather
        # than asking for the same correction a second time.
        [
            (c.name, getattr(c, "alias", None) or c.name)
            for c in table.columns
            if getattr(c, "native_filter_type", None) is None and not c.name.startswith("_nf_")
        ],
        table_context=getattr(table, "alias", None) or table.table_name,
    )
    return table_id


async def get(conn: "Connection", table_id: int) -> dict | None:  # REQ-013, REQ-393, REQ-399
    result = await conn.execute_core(
        select(registered_tables).where(registered_tables.c.id == table_id)
    )
    row = result.fetchone()
    if row is None:
        return None
    result_dict = dict(row._mapping)
    result_dict["columns"] = await _load_columns(conn, table_id)
    return result_dict


async def get_by_name(
    conn: "Connection", source_id: str, schema_name: str, table_name: str
) -> dict | None:  # REQ-013, REQ-155
    result = await conn.execute_core(
        select(registered_tables).where(
            registered_tables.c.source_id == source_id,
            registered_tables.c.schema_name == schema_name,
            registered_tables.c.table_name == table_name,
        )
    )
    row = result.fetchone()
    if row is None:
        return None
    result_dict = dict(row._mapping)
    result_dict["columns"] = await _load_columns(conn, result_dict["id"])
    return result_dict


async def find_by_table_name(
    conn: "Connection", table_name: str
) -> dict | None:  # REQ-014, REQ-155
    """Find a registered table by its virtual name.

    The virtual name is alias when set, otherwise table_name.
    Raises ValueError if multiple tables match.
    """
    result = await conn.execute_core(
        select(registered_tables).where(
            (registered_tables.c.alias == table_name)
            | (
                (registered_tables.c.alias.is_(None))
                & (registered_tables.c.table_name == table_name)
            )
        )
    )
    rows = result.fetchall()
    if not rows:
        return None
    if len(rows) > 1:
        sources = [r._mapping["source_id"] for r in rows]
        raise ValueError(
            f"Ambiguous table name {table_name!r}: found in sources {sources}. "
            f"Use source-qualified lookup instead."
        )
    return dict(rows[0]._mapping)


async def list_all(conn: "Connection") -> list[dict]:  # REQ-013, REQ-016
    result = await conn.execute_core(select(registered_tables).order_by(registered_tables.c.id))
    rows = result.fetchall()
    out = []
    for row in rows:
        r = dict(row._mapping)
        r["columns"] = await _load_columns(conn, r["id"])
        out.append(r)
    return out


async def delete(conn: "Connection", table_id: int) -> bool:  # REQ-014, REQ-1918
    """Delete one registered table or view: THE delete, for every surface. False when there is
    no such table.

    Refused (:class:`TableDeleteRefused`), naming each dependent, while a relationship takes
    part in it — at either end, or through it — a view or materialized view reads it, a metric's
    expression reads it, or a command or webhook returns it. When it may go, its parts go with
    it: its columns, the row filters defined on it, its tag assignments, glossary references,
    meta links, file mtimes, the discovery candidates naming it, and for a view its storage row
    with its refresh log and delta ledger. One transaction; no database cascade is relied on.

    Data kept for the table outside the control plane (replica, row-level rows, a view's
    storage relation, cache entries) is not removed here; the caller runs what exists for it.
    """
    ref = ObjectRef("table", table_id)
    async with conn.transaction():
        row = await get(conn, table_id)
        if row is None:
            return False
        blocking = await guard(conn, ref)
        if blocking:
            raise TableDeleteRefused(table_id, row["table_name"], blocking)
        # REQ-1591: before the refs go. A term's domains are derived by joining its refs to this
        # very table, so the sweep that follows has nothing left to read them from.
        domains_before = await glossary_repo.term_domains(conn)
        await discard(conn, table_id)
        # REQ-1387: settle the terms that lost their last ref (remove, or deprecate when an
        # abstract term hangs on them).
        await glossary_repo.sweep_refless_terms(conn, domains_before=domains_before)
    return True


async def rekey(conn: "Connection", table_id: int, schema_name: str) -> None:  # REQ-1919
    """Move a registered table to another schema of its source: the same table, with its id, its
    parts and everything that refers to it, under a new key. The config loader's, for a table
    whose schema the file corrected."""
    await conn.execute_core(
        update(registered_tables)
        .where(registered_tables.c.id == table_id)
        .values(schema_name=schema_name)
    )


async def discard(conn: "Connection", table_id: int) -> None:
    """Remove a table's parts and its row WITHOUT asking the guard: for a caller that has
    already established it may go — :func:`delete`, and the config loader once its own check of
    everything the file dropped has passed."""
    await remove_parts(conn, ObjectRef("table", table_id))
    await conn.execute_core(_delete(registered_tables).where(registered_tables.c.id == table_id))


async def retire_generated(  # REQ-1918
    conn: "Connection", source_id: str, schema_name: str, current: set[str]
) -> list[TableDeleteRefused]:
    """Remove the tables a source's generated set no longer contains, one at a time through
    :func:`delete`; returns the refusals of those that could not go.

    A remote source's tables are generated from its schema, and re-registering the source
    upserts each by its identity (source, schema, name), so a table that is still there keeps its
    id and everything that refers to it. A table the remote no longer has is deleted like any
    other — and when something depends on it, it is KEPT and reported, so the operator sees
    which tables are gone upstream and what still refers to them. ``current`` is the names in
    the set just registered.
    """
    rows = await conn.execute_core(
        select(registered_tables.c.id, registered_tables.c.table_name).where(
            registered_tables.c.source_id == source_id,
            registered_tables.c.schema_name == schema_name,
        )
    )
    kept: list[TableDeleteRefused] = []
    for table_id, name in sorted(rows.fetchall(), key=lambda r: r[1]):
        if name in current:
            continue
        try:
            await delete(conn, table_id)
        except TableDeleteRefused as refused:
            kept.append(refused)
    return kept


def kept_columns_report(refused: ColumnDropRefused, table_id: int | None) -> dict:
    """A generated table whose re-registration would have dropped columns something refers to,
    as a row of what the re-registration returns: the table was left as it was, and each such
    column is listed with what refers to it."""
    return {"id": table_id, "name": refused.table_name, "columns": refused.report()}


def kept_report(kept: list[TableDeleteRefused]) -> list[dict]:
    """:func:`retire_generated`'s refusals as the rows a re-registration returns: the table
    kept, and what still refers to it."""
    return [
        {
            "id": refused.table_id,
            "name": refused.name,
            "dependents": [d.as_dict() for d in refused.dependents],
        }
        for refused in kept
    ]


async def remove_registrations(conn: "Connection", *where) -> None:
    """Remove every registered table matching ``where`` (clauses on ``registered_tables``) —
    for the code that registers tables as a SET and replaces or retires that set: the seed
    retiring rows it wrote, the config loader dropping what the config no longer declares.

    It declares a set; it is not a deletion of one object, so it does not ask the dependency
    guard, as a full replace does not. What referred to those tables is left to the database, as
    it was when each caller issued this statement itself: removed by the schema's cascades on
    PostgreSQL, left in place on SQLite."""
    await conn.execute_core(_delete(registered_tables).where(*where))
