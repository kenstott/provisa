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

from sqlalchemy import delete as _delete, select

from provisa.core import domain_policy
from provisa.core.models import Table
from provisa.core.schema_org import registered_tables, table_columns

if TYPE_CHECKING:
    from provisa.core.database import Connection

_COLUMN_PROJECTION = [
    table_columns.c.column_name,
    table_columns.c.data_type,
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
]


async def _load_columns(conn: "Connection", table_id: int) -> list[dict]:
    result = await conn.execute_core(
        select(*_COLUMN_PROJECTION)
        .where(table_columns.c.table_id == table_id)
        .order_by(table_columns.c.id)
    )
    return [dict(r._mapping) for r in result.fetchall()]


async def upsert(
    conn: "Connection", table: Table
) -> int | None:  # REQ-013, REQ-016, REQ-133, REQ-155, REQ-156, REQ-260, REQ-334, REQ-393, REQ-399
    """Upsert a registered table and its columns. Returns the table row id."""
    domain_id = domain_policy.resolve_domain_id(table.domain_id)
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
        "data_product": getattr(table, "data_product", False),
        "materialize": getattr(table, "materialize", False),
        "mv_refresh_interval": getattr(table, "mv_refresh_interval", 300),
        "mv_debounce_quiet": getattr(table, "mv_debounce_quiet", 0.0),  # REQ-963
        "mv_debounce_max_delay": getattr(table, "mv_debounce_max_delay", 5.0),  # REQ-963
        "mv_consistency": getattr(table, "mv_consistency", "shared"),  # REQ-879
        "mv_preprocess": getattr(table, "mv_preprocess", None),  # REQ-957
        "mv_bitemporal_mode": getattr(table, "mv_bitemporal_mode", None),  # REQ-1162
        "mv_bitemporal_key": getattr(table, "mv_bitemporal_key", []),  # REQ-1162
        "enable_aggregates": getattr(table, "enable_aggregates", False),
        "enable_group_by": getattr(table, "enable_group_by", False),
        "live": table.live.model_dump() if table.live else None,
        "change_signal": getattr(table, "change_signal", None),
        "probe_query": getattr(table, "probe_query", None),
        "probe_type": getattr(table, "probe_type", None),
        "load_protected": getattr(table, "load_protected", None),  # REQ-1141
        "off_peak_window": getattr(table, "off_peak_window", None),  # REQ-1141
        "off_peak_tz": getattr(table, "off_peak_tz", None),  # REQ-1141
    }
    _update_columns = [
        "domain_id",
        "alias",
        "description",
        "watermark_column",
        "column_presets",
        "unique_constraints",  # REQ-1093
        "view_sql",
        "data_product",
        "materialize",
        "mv_refresh_interval",
        "mv_debounce_quiet",
        "mv_debounce_max_delay",
        "mv_consistency",
        "mv_preprocess",
        "mv_bitemporal_mode",  # REQ-1162
        "mv_bitemporal_key",  # REQ-1162
        "enable_aggregates",
        "enable_group_by",
        "live",
        "change_signal",
        "probe_query",
        "probe_type",
        "load_protected",  # REQ-1141
        "off_peak_window",  # REQ-1141
        "off_peak_tz",  # REQ-1141
    ]
    table_id = await conn.upsert_returning(
        registered_tables,
        values,
        index_elements=["source_id", "schema_name", "table_name"],
        returning="id",
        update_columns=_update_columns,
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
    # Replace columns: delete existing, insert new
    await conn.execute_core(_delete(table_columns).where(table_columns.c.table_id == table_id))
    for col in table.columns:
        object_fields_raw = getattr(col, "object_fields", [])
        object_fields = [
            f.model_dump() if hasattr(f, "model_dump") else f for f in object_fields_raw
        ]
        _data_type = getattr(col, "data_type", None) or _existing_types.get(col.name)
        await conn.execute_core(
            table_columns.insert().values(
                table_id=table_id,
                column_name=col.name,
                visible_to=col.visible_to,
                writable_by=getattr(col, "writable_by", []),
                unmasked_to=getattr(col, "unmasked_to", []),
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
            )
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


async def delete(conn: "Connection", table_id: int) -> bool:  # REQ-014
    result = await conn.execute_core(
        _delete(registered_tables).where(registered_tables.c.id == table_id)
    )
    return (result.rowcount or 0) > 0
