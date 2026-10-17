# Copyright (c) 2026 Kenneth Stott
# Canary: 9c4e2a7f-1b6d-4f8a-b3e5-7d0c9a2e4f16
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Type an imported design from its sources at preview time (REQ-1691).

A Hasura export names columns and carries no types (REQ-1426), and a tracked table with no
permission carries no columns at all. The import preview is design time — the administrator is
authoring the design, and has just supplied the connection every source will use — so this is
where discovery runs, the same way the registration form shows discovered types. Every SQL source
the preview can reach types its tables from ``information_schema.columns``; a column-less table
takes every discovered column, visible to no role (Hasura exposed it to none). A source the
preview cannot reach, or a column the source does not have, is a warning, and the column stays
untyped for the administrator to finish.
"""

# Requirements: REQ-1426, REQ-1691

from __future__ import annotations
from provisa.compiler.sql_literals import sql_literal

from typing import Any

from provisa.api.admin.introspect import native_column_typings
from provisa.core.ir_types import ColumnTyping, column_typing, to_ir
from provisa.core.models import (
    Column,
    Function,
    FunctionArgument,
    ProvisaConfig,
    Source,
    WriteGrant,
)
from provisa.core.secrets import resolve_secrets
from provisa.import_shared.warnings import WarningCollector
from provisa.security.rights import ORG_ADMIN_ROLE

_SQL_KINDS = {"postgresql", "mysql"}

_COLUMNS_SQL = (
    "SELECT column_name, data_type FROM information_schema.columns "
    "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position"
)


def _quote(value: str, dialect: str) -> str:
    return sql_literal(value, dialect)


async def _discover(pool: Any, source: Source, schema: str, table: str) -> dict[str, ColumnTyping]:
    """What each column of a source table registers as. PostgreSQL is typed from its catalog, as
    registration types it (``native_column_typings``): information_schema alone names neither the
    element of an array nor a composite (REQ-1965)."""
    kind = source.type.value
    if kind == "postgresql":
        typings = await native_column_typings(source.id, kind, schema, table, pool)
        return dict(typings or [])
    dialect = source.dialect or "postgres"
    result = await pool.execute(
        source.id, _COLUMNS_SQL % (_quote(schema, dialect), _quote(table, dialect)), []
    )
    found: dict[str, ColumnTyping] = {}
    for name, native in result.rows:
        typing = column_typing(str(native), kind)
        found[str(name)] = typing if typing.native_type else ColumnTyping(to_ir(str(native), kind))
    return found


def _typed(column: Column, typing: ColumnTyping) -> None:
    column.data_type = typing.data_type
    column.array_depth = typing.array_depth
    column.element_type = typing.element_type
    column.native_type = typing.native_type
    column.native_platform = typing.native_platform


async def type_imported_columns(
    config: ProvisaConfig, collector: WarningCollector, pool_factory: Any = None
) -> None:
    """Fill every untyped column, and every column-less table, from the reachable SQL sources.

    ``pool_factory`` builds the temporary pool (tests inject one); the default is the executor's
    ``SourcePool``. The pool is private to this call and closed before it returns — nothing is
    registered until the administrator applies the design.
    """
    if pool_factory is None:
        from provisa.executor.pool import SourcePool

        pool_factory = SourcePool
    by_source = {s.id: s for s in config.sources if s.type.value in _SQL_KINDS}
    wanted = [
        t
        for t in config.tables
        if t.source_id in by_source
        and (not t.columns or any(c.data_type in (None, "") for c in t.columns))
    ]
    functions = list(collector.pending_functions)
    collector.pending_functions.clear()
    if not wanted and not functions:
        return
    pool = pool_factory()
    reachable: set[str] = set()
    try:
        for sid in {t.source_id for t in wanted} | {
            f["source_id"] for f in functions if f["source_id"] in by_source
        }:
            src = by_source[sid]
            try:
                await pool.add(
                    sid,
                    src.type.value,
                    resolve_secrets(src.host),
                    int(src.port),
                    resolve_secrets(src.database),
                    resolve_secrets(src.username),
                    resolve_secrets(src.password),
                    1,
                    1,
                )
                reachable.add(sid)
            except Exception as exc:  # allow-blind-except: the reason is reported, not swallowed
                collector.warn(
                    "sources",
                    f"Source {sid!r} is not reachable from the preview ({exc}); its columns stay "
                    "untyped and must be finished before apply",
                )
        for t in wanted:
            if t.source_id not in reachable:
                continue
            found = await _discover(pool, by_source[t.source_id], t.schema_name, t.table_name)
            if not found:
                collector.warn(
                    "tables",
                    f"{t.source_id}: {t.schema_name}.{t.table_name} has no columns in the source",
                )
                continue
            if not t.columns:
                # Hasura exposed this table to no role but its admin: land every column, visible
                # to org_admin alone (REQ-1684).
                t.columns = [
                    Column(
                        name=n,
                        data_type=ty.data_type,
                        array_depth=ty.array_depth,
                        element_type=ty.element_type,
                        native_type=ty.native_type,
                        native_platform=ty.native_platform,
                        visible_to=[ORG_ADMIN_ROLE],
                    )
                    for n, ty in found.items()
                ]
                # Hasura's admin writes every table by every operation (REQ-2074).
                t.write_grants = [
                    WriteGrant(role=ORG_ADMIN_ROLE, operation="insert", columns=list(found)),
                    WriteGrant(role=ORG_ADMIN_ROLE, operation="update", columns=list(found)),
                    WriteGrant(role=ORG_ADMIN_ROLE, operation="delete"),
                ]
                continue
            for c in t.columns:
                if c.data_type in (None, ""):
                    if c.name in found:
                        _typed(c, found[c.name])
                    else:
                        collector.warn(
                            "tables",
                            f"{t.source_id}: {t.schema_name}.{t.table_name}.{c.name} is not a "
                            "column of the source table; it stays untyped",
                        )
        for pending in functions:
            if pending["source_id"] not in reachable or (
                by_source[pending["source_id"]].type.value != "postgresql"
            ):
                collector.pending_function_to_redo(pending)
                continue
            fn = await _discover_function(pool, pending, config, collector)
            if fn is not None:
                config.functions.append(fn)
    finally:
        await pool.close_all()


_FUNCTION_SQL = (
    "SELECT rn.nspname, rc.relname, p.proargnames, "
    "ARRAY(SELECT format_type(t, NULL) FROM unnest(p.proargtypes) AS t) "
    "FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
    "JOIN pg_type rt ON rt.oid = p.prorettype "
    "LEFT JOIN pg_class rc ON rc.oid = rt.typrelid "
    "LEFT JOIN pg_namespace rn ON rn.oid = rc.relnamespace "
    "WHERE n.nspname = %s AND p.proname = %s"
)


async def _discover_function(
    pool: Any, pending: dict, config: ProvisaConfig, collector: WarningCollector
) -> Function | None:  # REQ-205
    """A tracked function as the command it is, its arguments and the table it returns read from
    the database's catalog: Hasura tracks a function returning rows of a tracked table."""
    sid, schema, name = pending["source_id"], pending["schema_name"], pending["name"]
    result = await pool.execute(
        sid, _FUNCTION_SQL % (_quote(schema, "postgres"), _quote(name, "postgres")), []
    )
    rows = list(result.rows)
    if len(rows) != 1:
        collector.warn(
            "functions",
            f"Function {schema}.{name} is "
            + ("not in the database" if not rows else f"overloaded {len(rows)} ways")
            + "; add it on the Commands page.",
        )
        return None
    ret_schema, ret_table, arg_names, arg_types = rows[0]
    registered = {(t.source_id, t.schema_name, t.table_name) for t in config.tables}
    if (sid, ret_schema, ret_table) not in registered:
        collector.warn(
            "functions",
            f"Function {schema}.{name} returns {'rows of ' + ret_table if ret_table else 'no table'}"
            ", which the import does not register; a command returns rows of a registered "
            "table. Add it on the Commands page.",
        )
        return None
    names = list(arg_names or [])
    arguments = [
        FunctionArgument(
            name=names[i] if i < len(names) and names[i] else f"arg{i + 1}",
            type=column_typing(str(native), "postgresql").data_type,
        )
        for i, native in enumerate(arg_types or [])
    ]
    collector.carry(
        "functions", f"{schema}.{name}", f"{pending['kind']} command returning {ret_table}"
    )
    return Function(
        name=name,
        source_id=sid,
        schema_name=schema,
        function_name=name,
        returns=f"{sid}.{ret_schema}.{ret_table}",
        arguments=arguments,
        visible_to=pending["visible_to"],
        domain_id=next(
            t.domain_id
            for t in config.tables
            if (t.source_id, t.schema_name, t.table_name) == (sid, ret_schema, ret_table)
        ),
        kind=pending["kind"],
    )
