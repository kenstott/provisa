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

from typing import Any

from provisa.core.ir_types import to_ir
from provisa.core.models import Column, ProvisaConfig, Source
from provisa.core.secrets import resolve_secrets
from provisa.import_shared.warnings import WarningCollector
from provisa.security.rights import ORG_ADMIN_ROLE

_SQL_KINDS = {"postgresql", "mysql"}

_COLUMNS_SQL = (
    "SELECT column_name, data_type FROM information_schema.columns "
    "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position"
)


def _quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


async def _discover(pool: Any, source: Source, schema: str, table: str) -> dict[str, str]:
    result = await pool.execute(source.id, _COLUMNS_SQL % (_quote(schema), _quote(table)), [])
    return {str(name): to_ir(str(native), source.type.value) for name, native in result.rows}


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
    if not wanted:
        return
    pool = pool_factory()
    reachable: set[str] = set()
    try:
        for sid in {t.source_id for t in wanted}:
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
                        data_type=ty,
                        visible_to=[ORG_ADMIN_ROLE],
                        writable_by=[ORG_ADMIN_ROLE],
                    )
                    for n, ty in found.items()
                ]
                continue
            for c in t.columns:
                if c.data_type in (None, ""):
                    if c.name in found:
                        c.data_type = found[c.name]
                    else:
                        collector.warn(
                            "tables",
                            f"{t.source_id}: {t.schema_name}.{t.table_name}.{c.name} is not a "
                            "column of the source table; it stays untyped",
                        )
    finally:
        await pool.close_all()
