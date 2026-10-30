# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The indexes a materialized view carries (REQ-2015, REQ-2163).

A view stored where indexes are kept is indexed as a replica is, by the replica's own mechanism
(``provisa.federation.replica_indexes``): its key, enforced as the key, and the columns the
relationships declared on it join on. Each landing or refresh states what it asks for on the
view's build row; the replica runner's pass builds them after it, under the view's index lock,
through the indexer a replica in that store gets. A view's table is refreshed in place, so an
index already there is found, not built twice. Nothing here runs in the landing: a plan that cannot
be made is logged and the landing stands.

A versioned view's table is the log of its versions, which repeats its key on purpose, so its key
is an index and never a constraint.

A view of an ontology Class's union is also indexed on ``_source_table`` with the key of each of
the Class's tables, so a lookup by key reads one member's rows. A member key is the Property
columns its key columns are bound to: the union holds no others, so a member whose key is not bound
has none to index.

Provisa never indexes a source's own table: only a view's stored table in this engine's
materialization store is ever planned, and one whose name is a source's table is refused.
"""

# Requirements: REQ-2015, REQ-2163

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from provisa.core.models import DERIVED_SOURCE_ID
from provisa.core.schema_org import registered_tables
from provisa.federation import replica_indexes

if TYPE_CHECKING:
    from provisa.core.database import Connection, Database
    from provisa.mv.models import MVDefinition

log = logging.getLogger(__name__)


async def _source_holds(conn: "Connection", schema: str, table: str) -> bool:
    """Whether ``schema.table`` is a table some source registers, not a view's own."""
    t = registered_tables.c
    held = await conn.execute_core(
        select(t.id).where(
            t.schema_name == schema, t.table_name == table, t.source_id != DERIVED_SOURCE_ID
        )
    )
    return held.first() is not None


async def _joins(conn: "Connection", mv: "MVDefinition", pk: list[str]) -> list[tuple[str, ...]]:
    """The columns the relationships declared on ``mv``'s registered view join it on. A view
    configured on its own is no registered table, and no relationship names it."""
    if mv.view_table is None:
        return []
    t = registered_tables.c
    ids = await conn.execute_core(
        select(t.id).where(t.source_id == DERIVED_SOURCE_ID, t.table_name == mv.view_table)
    )
    return await replica_indexes.joins_of(conn, [r[0] for r in ids.fetchall()], pk)


def _member_key(binding: dict[str, Any], table: dict[str, Any]) -> tuple[str, ...] | None:
    """The Class columns holding the key of the member table ``binding`` reads, in the key's
    order; None when it declares no key, or a key column is bound to no single-valued Property."""
    key = [c["column_name"] for c in table["columns"] if c["is_primary_key"]]
    bound = {reads[0][0]: prop for prop, reads in binding["columns"].items() if len(reads) == 1}
    if not key or any(column not in bound for column in key):
        return None
    return tuple(bound[column] for column in key)


def _class_keys(mv: "MVDefinition", held: list[str], served: Any) -> list[tuple[str, ...]]:
    """REQ-2163: the indexes of a view of a Class's union: ``_source_table`` with each member's
    key. Nothing for a view reading no Class, or not keeping ``_source_table``."""
    from provisa.compiler.ontology_classes import SOURCE_TABLE_COLUMN
    from provisa.mv.view_read import views_read

    classes = served.class_tables
    sql = mv.semantic_sql or mv.sql
    if SOURCE_TABLE_COLUMN not in held or not classes or sql is None:
        return []
    tables = {table["id"]: table for table in served.tables}
    found: list[tuple[str, ...]] = []
    for view in views_read(sql, dict.fromkeys(classes, "")):
        for binding in classes[view]["class_bindings"]:
            key = _member_key(binding, tables[binding["table_id"]])
            if key is not None:
                found.append((SOURCE_TABLE_COLUMN, *key))
    return found


async def _plan(
    engine: Any, mv: "MVDefinition", model_db: "Database", state_db: "Database", served: Any
) -> None:
    from provisa.federation.execution_auth import SystemAuth, mint_system_token
    from provisa.mv.coordination import ensure_mv_row
    from provisa.mv.refresh import _target_columns
    from provisa.mv.store_shape import view_key

    schema, table = mv.target_schema, mv.target_table
    assert table is not None  # set by MVDefinition.__post_init__
    # An engine terminal without the seam (a test fake, a non-native engine) indexes nothing.
    if not hasattr(engine, "view_indexer") or engine.view_indexer(schema, table) is None:
        return
    catalog, _schema = engine.materialize_store_target(served.active_org_id)
    if mv.target_catalog != catalog:
        log.info("view %s is kept outside the materialization store: not indexed", mv.id)
        return
    held = await _target_columns(
        engine, mv, mv.stored_ref(), SystemAuth(mint_system_token(), reason=f"mv_indexes:{mv.id}")
    )
    if held is None:
        return
    # A versioned view's log repeats its key once per version.
    pk = [] if mv.bitemporal is not None else view_key(mv)
    versioned = [tuple(mv.bitemporal.key)] if mv.bitemporal is not None else []
    async with model_db.acquire() as conn:
        if await _source_holds(conn, schema, table):
            log.error(
                "view %s is stored at %s.%s, a source's own table: Provisa indexes none",
                mv.id,
                schema,
                table,
            )
            return
        joins = await _joins(conn, mv, pk)
    specs = replica_indexes.plan(
        f"{schema}.{table}", held, pk, [*versioned, *joins, *_class_keys(mv, held, served)]
    )
    await ensure_mv_row(model_db, state_db, mv)
    async with state_db.acquire() as conn:
        await replica_indexes.record_view_plan(
            conn, mv.id, datetime.now(UTC), specs, (schema, table)
        )


async def plan_view_indexes(
    engine: Any,
    mv: "MVDefinition",
    *,
    model_db: "Database",
    state_db: "Database",
    served: Any,
) -> None:
    """Record the indexes ``mv``'s landing asks for, for the runner to build after it. ``served``
    is the org's state: its Class tables, its tables and its id. A plan that cannot be made leaves
    the view without index builds until its next landing: logged, never undoing the landing."""
    try:
        await _plan(engine, mv, model_db, state_db, served)
    except Exception:  # allow-ble: the view is built and recorded; its indexes are its read speed, not its rows, and REQ-2015 has a failure here logged with its cause and never fail the landing
        log.exception("view %s is built, but its indexes were not planned", mv.id)
