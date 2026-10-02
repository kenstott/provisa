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

  the engine path  — ddl_catalog is Iceberg/Hive or any non-registered catalog.
                Only CREATE TABLE supported (the engine limit).
                Table name is qualified as catalog.schema.name.

  Direct path — ddl_catalog matches a registered source id.
                DDL passthrough: CREATE TABLE/INDEX, ALTER TABLE,
                DROP, sequences, etc.  Executed via the source pool.
                CREATE TABLE is schema-qualified (schema.name).
                All other DDL (ALTER, DROP, CREATE INDEX …) passed through
                as-is with the write schema set as the search_path on PG,
                or a USE statement on MySQL/MariaDB.

A VIEW is on neither path. A view is a model object: its definition belongs to the model and its
body is governed for whoever reads it. Sent to the engine or a source as written, the body would
read live addresses with no row filter, mask or column visibility applied, and the result would
be a relation the model does not know. ``CREATE [OR REPLACE] VIEW`` over pgwire is refused,
naming the admin action that creates one.

Requires role capability "ddl".
"""

# Requirements: REQ-042, REQ-060

from __future__ import annotations

from provisa.core.connection_loop import run_on_connection_loop
import logging
import re

log = logging.getLogger(__name__)

_TABLE_RE = re.compile(
    r"^\s*CREATE\s+(?P<or_replace>OR\s+REPLACE\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    r"(?:(?P<schema>[^\s.(]+)\.)?(?P<name>[^\s.(]+)\s*(?P<rest>\(.*)",
    re.IGNORECASE | re.DOTALL,
)
_CREATE_TABLE_RE = re.compile(r"^\s*CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\b", re.IGNORECASE)

# What may stand between CREATE and VIEW in a statement that creates a view.
_VIEW_MODIFIERS = frozenset(
    {"OR", "REPLACE", "TEMP", "TEMPORARY", "MATERIALIZED", "RECURSIVE", "GLOBAL", "LOCAL"}
)
_OPENS_AS_VIEW_RE = re.compile(
    r"^\s*CREATE\s+(?:(?:OR|REPLACE|TEMP|TEMPORARY|MATERIALIZED|RECURSIVE|GLOBAL|LOCAL)\s+)*VIEW\b",
    re.IGNORECASE,
)
# A statement with neither word cannot create a view: spares every other statement the tokenizer.
_MAY_CREATE_VIEW_RE = re.compile(r"\bCREATE\b.*\bVIEW\b", re.IGNORECASE | re.DOTALL)


class ViewNotCreatedOverPgwire(Exception):
    """``CREATE VIEW`` over pgwire: refused, naming the admin action (SQLSTATE 0A000)."""

    def __init__(self) -> None:
        super().__init__(
            "CREATE VIEW is not available over pgwire: a view is a model object, governed for "
            "whoever reads it. Register it with the admin registerTable mutation (viewSql) or "
            "in the admin UI."
        )


def creates_view(sql: str) -> bool:
    """Whether ``sql`` is a statement that creates a view — ``CREATE [OR REPLACE] [TEMP |
    MATERIALIZED | RECURSIVE] VIEW`` — read from its tokens, so comments, case and spacing do
    not matter and a column or string that merely contains the word does not count."""
    if not _MAY_CREATE_VIEW_RE.search(sql):
        return False
    from sqlglot.errors import TokenError
    from sqlglot.tokens import TokenType

    from sqlglot.dialects.postgres import Postgres

    try:
        tokens = Postgres().tokenize(sql)
    except TokenError:
        # Not tokenizable (an unterminated string, say). Its opening words still say what it
        # set out to be, and a malformed view statement is refused like a well-formed one — it
        # is never passed on to a source to find the error.
        return bool(_OPENS_AS_VIEW_RE.match(sql))
    if not tokens or tokens[0].token_type != TokenType.CREATE:
        return False
    for token in tokens[1:]:
        if token.token_type == TokenType.VIEW:
            return True
        if token.text.upper() not in _VIEW_MODIFIERS:
            return False
    return False


def _ddl_kind(sql: str) -> str:
    if re.match(r"^\s*CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\b", sql, re.IGNORECASE):
        return "TABLE"
    m = re.match(r"^\s*(CREATE|ALTER|DROP)\s+(\S+)", sql, re.IGNORECASE)
    if m:
        return f"{m.group(1).upper()} {m.group(2).upper()}"
    return "DDL"


def _command_tag(sql: str) -> str:
    """PG command-complete tag for a DDL statement."""
    kind = _ddl_kind(sql)
    return kind if kind != "TABLE" else f"CREATE {kind}"


state = None  # module-level reference; replaced by tests via patch()


class DdlHandler:  # REQ-042, REQ-060
    def __init__(self, handler):
        self._handler = handler

    def handle(self, ctx, sql: str) -> str:  # REQ-042, REQ-060
        """Execute DDL and return the PG command-complete tag."""
        if creates_view(sql):
            # Before anything else: for no role and on neither path does a view body reach the
            # engine or a source as written.
            raise ViewNotCreatedOverPgwire
        import provisa.pgwire.ddl_handler as _m

        state = _m.state  # type: ignore[assignment]
        if state is None:
            from provisa.api.app import state  # type: ignore[assignment]

        role_id = ctx.session.role_id
        role = state.roles.get(role_id)
        if role is None:
            # Authenticated role must exist; never substitute an empty (capability-less) role.
            raise PermissionError(f"Unknown role {role_id!r}")
        caps = role.get("capabilities") or []
        if "ddl" not in caps:
            raise PermissionError(f"Role {role_id!r} lacks 'ddl' capability")

        write_target = self._resolve_write_target(role_id, role, state)
        write_catalog, write_schema = write_target

        # Determine whether to use direct source pool or the engine
        source_id = _catalog_to_source_id(write_catalog, state)
        if source_id and state.source_types.get(source_id):
            self._exec_direct(ctx, sql, source_id, write_schema, role_id, state)
        else:
            if not _CREATE_TABLE_RE.match(sql):
                raise ValueError(
                    f"Only CREATE TABLE is supported for the engine catalog {write_catalog!r}. "
                    "Use a registered source as ddl_catalog for full DDL support."
                )
            if state.federation_engine is None:
                raise RuntimeError("Query engine not available for DDL")
            self._exec_engine(ctx, sql, write_catalog, write_schema, role_id, state)

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

    def _exec_engine(self, _ctx, sql, write_catalog, write_schema, role_id, state):
        m = _TABLE_RE.match(sql)
        if not m:
            raise ValueError(f"Cannot parse DDL: {sql[:120]!r}")

        table_name = m.group("name")
        rest = m.group("rest")
        or_replace = "OR REPLACE " if m.group("or_replace") else ""
        qualified_sql = (
            f"CREATE {or_replace}TABLE {write_catalog}.{write_schema}.{table_name} {rest}"
        )
        log.info("DDL(engine) role=%r: %s", role_id, qualified_sql[:200])
        run_on_connection_loop(state.federation_engine.execute_engine(qualified_sql), timeout=60)
        _register_ddl_object(role_id, table_name, write_catalog, write_schema, "TABLE")

    def _exec_direct(self, _ctx, sql, source_id, write_schema, role_id, state):
        # For CREATE TABLE: qualify unqualified name with write_schema
        if _CREATE_TABLE_RE.match(sql):
            m = _TABLE_RE.match(sql)
            if m and not m.group("schema"):
                table_name = m.group("name")
                rest = m.group("rest")
                or_replace = "OR REPLACE " if m.group("or_replace") else ""
                sql = f"CREATE {or_replace}TABLE {write_schema}.{table_name} {rest}"
                log.info("DDL(direct) role=%r source=%r: %s", role_id, source_id, sql[:200])
                run_on_connection_loop(
                    _exec_direct_ddl_async(state.source_pools, source_id, sql), timeout=60
                )
                _register_ddl_object(role_id, table_name, source_id, write_schema, "TABLE")
                return

        # ALTER TABLE, DROP, CREATE INDEX, etc. — raw passthrough with schema context
        log.info("DDL(direct/passthrough) role=%r source=%r: %s", role_id, source_id, sql[:200])
        source_type = state.source_types.get(source_id, "")
        run_on_connection_loop(
            _exec_direct_ddl_with_schema_async(
                state.source_pools, source_id, source_type, write_schema, sql
            ),
            timeout=60,
        )


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
