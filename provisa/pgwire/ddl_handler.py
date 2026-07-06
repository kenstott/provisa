# Copyright (c) 2026 Kenneth Stott
# Canary: f6a7b8c9-d0e1-2345-f012-678901234567
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the COPYRIGHT holder.

"""DDL routing for the pgwire server.

Two execution paths:

  Trino path  — ddl_catalog is Iceberg/Hive or any non-registered catalog.
                Only CREATE TABLE / CREATE VIEW supported (Trino limit).
                Table name is qualified as catalog.schema.name.

  Direct path — ddl_catalog matches a registered source id.
                Full DDL passthrough: CREATE TABLE/VIEW/INDEX, ALTER TABLE,
                DROP, sequences, etc.  Executed via the source pool.
                CREATE TABLE/VIEW are schema-qualified (schema.name).
                All other DDL (ALTER, DROP, CREATE INDEX …) passed through
                as-is with the write schema set as the search_path on PG,
                or a USE statement on MySQL/MariaDB.

Requires role capability "ddl".
"""

# Requirements: REQ-042, REQ-060

from __future__ import annotations

import asyncio
import logging
import re

log = logging.getLogger(__name__)

_TABLE_RE = re.compile(
    r"^\s*CREATE\s+(?P<or_replace>OR\s+REPLACE\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    r"(?:(?P<schema>[^\s.(]+)\.)?(?P<name>[^\s.(]+)\s*(?P<rest>\(.*)",
    re.IGNORECASE | re.DOTALL,
)
_VIEW_RE = re.compile(
    r"^\s*CREATE\s+(?P<or_replace>OR\s+REPLACE\s+)?VIEW\s+"
    r"(?:(?P<schema>[^\s.(]+)\.)?(?P<name>[^\s.(]+)\s+(?P<rest>AS\s+.*)",
    re.IGNORECASE | re.DOTALL,
)
_CREATE_TABLE_OR_VIEW_RE = re.compile(
    r"^\s*CREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE|VIEW)\b", re.IGNORECASE
)


def _ddl_kind(sql: str) -> str:
    if re.match(r"^\s*CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\b", sql, re.IGNORECASE):
        return "VIEW"
    if re.match(r"^\s*CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\b", sql, re.IGNORECASE):
        return "TABLE"
    m = re.match(r"^\s*(CREATE|ALTER|DROP)\s+(\S+)", sql, re.IGNORECASE)
    if m:
        return f"{m.group(1).upper()} {m.group(2).upper()}"
    return "DDL"


def _command_tag(sql: str) -> str:
    """PG command-complete tag for a DDL statement."""
    kind = _ddl_kind(sql)
    return kind if kind not in ("TABLE", "VIEW") else f"CREATE {kind}"


state = None  # module-level reference; replaced by tests via patch()


class DdlHandler:  # REQ-042, REQ-060
    def __init__(self, handler):
        self._handler = handler

    def handle(self, ctx, sql: str) -> str:  # REQ-042, REQ-060
        """Execute DDL and return the PG command-complete tag."""
        import provisa.pgwire.ddl_handler as _m

        state = _m.state  # type: ignore[assignment]
        if state is None:
            from provisa.api.app import state  # type: ignore[assignment]

        role_id = ctx.session.role_id
        role = state.roles.get(role_id) or {}
        caps = role.get("capabilities") or []
        if "ddl" not in caps:
            raise PermissionError(f"Role {role_id!r} lacks 'ddl' capability")

        write_target = self._resolve_write_target(role_id, role, state)
        write_catalog, write_schema = write_target

        # Determine whether to use direct source pool or Trino
        source_id = _catalog_to_source_id(write_catalog, state)
        if source_id and state.source_types.get(source_id):
            self._exec_direct(ctx, sql, source_id, write_schema, role_id, state)
        else:
            if not _CREATE_TABLE_OR_VIEW_RE.match(sql):
                raise ValueError(
                    f"Only CREATE TABLE/VIEW is supported for Trino catalog {write_catalog!r}. "
                    "Use a registered source as ddl_catalog for full DDL support."
                )
            if state.federation_engine is None:
                raise RuntimeError("Query engine not available for DDL")
            self._exec_trino(ctx, sql, write_catalog, write_schema, role_id, state)

        return _command_tag(sql)

    def _resolve_write_target(self, role_id, role, state) -> tuple[str, str]:
        domain_ids = role.get("domain_access") or []
        for did in domain_ids:
            if did == "*":
                target = next(iter(state.domain_write_targets.values()), None)
                if target:
                    return target
                break
            target = state.domain_write_targets.get(did)
            if target:
                return target
        raise PermissionError(f"No ddl_catalog configured on domain for role {role_id!r}")

    def _exec_trino(self, _ctx, sql, write_catalog, write_schema, role_id, state):
        kind = _ddl_kind(sql)
        pattern = _VIEW_RE if kind == "VIEW" else _TABLE_RE
        m = pattern.match(sql)
        if not m:
            raise ValueError(f"Cannot parse DDL: {sql[:120]!r}")

        table_name = m.group("name")
        rest = m.group("rest")
        or_replace = "OR REPLACE " if m.group("or_replace") else ""
        verb = "VIEW" if kind == "VIEW" else "TABLE"
        qualified_sql = (
            f"CREATE {or_replace}{verb} {write_catalog}.{write_schema}.{table_name} {rest}"
        )
        log.info("DDL(trino) role=%r: %s", role_id, qualified_sql[:200])
        future = asyncio.run_coroutine_threadsafe(
            state.federation_engine.execute_engine(qualified_sql),
            self._handler._srv._loop,
        )
        future.result(timeout=60)
        _register_ddl_object(role_id, table_name, write_catalog, write_schema, kind)

    def _exec_direct(self, _ctx, sql, source_id, write_schema, role_id, state):
        # For CREATE TABLE/VIEW: qualify unqualified name with write_schema
        if _CREATE_TABLE_OR_VIEW_RE.match(sql):
            kind = _ddl_kind(sql)
            pattern = _VIEW_RE if kind == "VIEW" else _TABLE_RE
            m = pattern.match(sql)
            if m and not m.group("schema"):
                table_name = m.group("name")
                rest = m.group("rest")
                or_replace = "OR REPLACE " if m.group("or_replace") else ""
                verb = "VIEW" if kind == "VIEW" else "TABLE"
                sql = f"CREATE {or_replace}{verb} {write_schema}.{table_name} {rest}"
                log.info("DDL(direct) role=%r source=%r: %s", role_id, source_id, sql[:200])
                future = asyncio.run_coroutine_threadsafe(
                    _exec_direct_ddl_async(state.source_pools, source_id, sql),
                    self._handler._srv._loop,
                )
                future.result(timeout=60)
                _register_ddl_object(role_id, table_name, source_id, write_schema, kind)
                return

        # ALTER TABLE, DROP, CREATE INDEX, etc. — raw passthrough with schema context
        log.info("DDL(direct/passthrough) role=%r source=%r: %s", role_id, source_id, sql[:200])
        source_type = state.source_types.get(source_id, "")
        future = asyncio.run_coroutine_threadsafe(
            _exec_direct_ddl_with_schema_async(
                state.source_pools, source_id, source_type, write_schema, sql
            ),
            self._handler._srv._loop,
        )
        future.result(timeout=60)


def _catalog_to_source_id(catalog: str, state) -> str | None:
    """Return source_id if catalog name matches a registered source catalog, else None."""
    for sid, cat in state.source_catalogs.items():
        if cat == catalog:
            return sid
    # Also allow matching by source id directly
    if catalog in state.source_catalogs:
        return catalog
    return None


async def _exec_direct_ddl_async(source_pools, source_id: str, sql: str) -> None:
    conn = await source_pools.acquire(source_id)
    try:
        await conn.execute(sql)
    finally:
        await source_pools.release(source_id, conn)


async def _exec_direct_ddl_with_schema_async(
    source_pools, source_id: str, source_type: str, schema: str, sql: str
) -> None:
    conn = await source_pools.acquire(source_id)
    try:
        if source_type in ("postgresql", "sqlite"):
            await conn.execute(f"SET search_path TO {schema}")
        elif source_type in ("mysql", "mariadb"):
            await conn.execute(f"USE {schema}")
        await conn.execute(sql)
    finally:
        await source_pools.release(source_id, conn)


def _register_ddl_object(
    role_id: str,
    table_name: str,
    catalog: str,
    schema: str,
    kind: str,
) -> None:
    import provisa.pgwire.ddl_handler as _m

    state = _m.state  # type: ignore[assignment]
    if state is None:
        from provisa.api.app import state  # type: ignore[assignment]
    from provisa.compiler.sql_gen import TableMeta

    ctx = state.contexts.get(role_id)
    if ctx is None:
        return

    existing_ids = [m.table_id for m in ctx.tables.values()]
    new_id = max(existing_ids, default=0) + 1

    meta = TableMeta(
        table_id=new_id,
        field_name=table_name,
        type_name="".join(w.capitalize() for w in table_name.split("_")),
        source_id=catalog,
        catalog_name=catalog.replace("-", "_"),
        schema_name=schema,
        table_name=table_name,
    )
    ctx.tables[table_name] = meta
    log.info(
        "Registered %s %s.%s.%s into context for role %r",
        kind,
        catalog,
        schema,
        table_name,
        role_id,
    )
