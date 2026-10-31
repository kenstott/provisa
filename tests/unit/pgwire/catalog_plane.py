# Copyright (c) 2026 Kenneth Stott
# Canary: 4e0a7b16-93cd-4f28-a5d7-c1e6f2083b94
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A catalog statement on a real control plane, for the tests of what a client reads (REQ-2190): the
model a test states is written to the store, and ``answer`` reads the real views through the real
routing. Governance is left out, as these tests are about the catalog's answers: a role's rows are
tested through the pipeline (tests/unit/pgwire/test_catalog_on_meta.py)."""

# Requirements: REQ-2190

from __future__ import annotations

import json
from typing import Any

from sqlalchemy.engine import Engine

from provisa.core import schema_org
from provisa.core.pg_error import PostgresError, postgres_error
from provisa.executor.result import QueryResult
from provisa.pgwire import catalog
from provisa.pgwire.catalog_route import addressed_statement
from provisa.transpiler.metadata_facts import CatalogFacts
from provisa.transpiler.metadata_functions import MetadataFunctionRefused
from provisa.security.rights import Capability, has_capability
from provisa.transpiler.metadata_settings import set_statement, statement_settings
from provisa.transpiler.metadata_views import SCOPE_COLUMN

_SOURCE = "src"
_MODEL_TABLES = (
    "table_write_grants",
    "relationships",
    "table_columns",
    "registered_tables",
    "metrics",
    "tracked_functions",
    "roles",
    "domains",
    "sources",
)


def _dict(value: Any) -> dict:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> list:
    return value if isinstance(value, list) else []


def _insert(conn, table, **values) -> None:
    conn.execute(table.insert().values(**values))


def _write_tables(conn, ctx: Any, state: Any, domains: set[str]) -> None:
    t = schema_org
    col_types = state.schema_build_cache.get("column_types", {})
    pk_columns = _dict(getattr(ctx, "pk_columns", None))
    for tm in {m.table_id: m for m in ctx.tables.values()}.values():
        domain = _text(tm.domain_id) or _text(tm.schema_name) or "public"
        if domain not in domains:
            _insert(conn, t.domains, id=domain, description=f"The {domain} domain")
            domains.add(domain)
        alias = tm.table_name if _text(tm.original_table_name) else None
        _insert(
            conn,
            t.registered_tables,
            id=tm.table_id,
            source_id=_SOURCE,
            domain_id=domain,
            schema_name=_text(tm.schema_name) or "public",
            table_name=_text(tm.original_table_name) or tm.table_name,
            alias=_text(alias),
        )
        keys = pk_columns.get(tm.table_id, ())
        for column in col_types.get(tm.table_id, ()):
            _insert(
                conn,
                t.table_columns,
                table_id=tm.table_id,
                column_name=column.column_name,
                data_type=column.data_type,
                native_type=_text(column.native_type),
                subtype=_text(column.subtype),
                not_null=not column.is_nullable,
                is_primary_key=column.column_name in keys,
            )


def _write_grants(conn, state: Any, roles: set[str], tables: set[int]) -> None:
    t = schema_org
    for table in state.schema_build_cache.get("tables") or ():
        if table["id"] not in tables:
            continue
        for grant in table.get("write_grants") or ():
            if grant["role"] not in roles:
                _insert(conn, t.roles, id=grant["role"], description=grant["role"])
                roles.add(grant["role"])
            _insert(
                conn,
                t.table_write_grants,
                table_id=table["id"],
                role_id=grant["role"],
                operation=grant["operation"],
                presets=grant.get("presets") or [],
            )


def _write_joins(conn, ctx: Any) -> None:
    t = schema_org
    type_tables = {getattr(m, "type_name", ""): m.table_id for m in ctx.tables.values()}
    for n, ((source_type, _field), join) in enumerate(_dict(getattr(ctx, "joins", None)).items()):
        source = type_tables.get(source_type)
        if source is None or not join.target_column:
            continue
        _insert(
            conn,
            t.relationships,
            id=f"r{n}",
            source_table_id=source,
            source_column=join.source_column,
            target_table_id=join.target.table_id,
            target_column=join.target_column,
            cardinality=join.cardinality,
        )


def write_model(engine: Engine, role_id: str, state: Any) -> None:
    """The model ``state`` holds, as the store holds one."""
    t = schema_org
    ctx = state.contexts.get(role_id)
    with engine.begin() as conn:
        conn.exec_driver_sql(f"TRUNCATE {', '.join(_MODEL_TABLES)} CASCADE")
        _insert(conn, t.sources, id=_SOURCE, type="postgresql")
        roles = {role_id, *_dict(getattr(state, "roles", None))}
        for role in roles:
            _insert(conn, t.roles, id=role, description=role)
        tables: set[int] = set()
        if ctx is not None and isinstance(ctx.tables, dict):
            _write_tables(conn, ctx, state, set())
            _write_joins(conn, ctx)
            tables = {m.table_id for m in ctx.tables.values()}
        _write_grants(conn, state, roles, tables)
        config = getattr(state, "config", None)
        for metric in _list(getattr(config, "metrics", None)):
            _insert(
                conn,
                t.metrics,
                name=metric.name,
                expression=metric.expression,
                description=getattr(metric, "description", None),
            )


def _facts(state: Any, role_id: str) -> CatalogFacts:
    from provisa.pgwire.catalog_facts import role_facts

    return role_facts(role_id, state)


class Plane:
    """``answer``'s read of meta, on ``engine``: a state's model is written the first time a
    statement of that state reads it."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self._written: tuple[int, str] | None = None

    def read(self, sql: str, role_id: str, state: Any, session: Any, _run: Any) -> QueryResult:
        written = (id(state), role_id)
        if self._written != written:
            write_model(self.engine, role_id, state)
            self._written = written
        request = catalog._catalog_request(sql, role_id, state, session)
        try:
            addressed = addressed_statement(_bare(sql), request)
        except MetadataFunctionRefused as exc:
            raise PostgresError("0A000", str(exc)) from exc
        settings = statement_settings(
            role_id,
            _can_write(role_id, state),
            request.database,
            False,
            session=request.session,
        )
        setting, values = set_statement(settings)
        positional = dict(zip([f"p{i}" for i in range(len(values))], values))
        with self.engine.begin() as conn:
            conn.exec_driver_sql(_named(setting), positional)
            try:
                result = conn.exec_driver_sql(addressed.replace("%", "%%"))
            except Exception as exc:
                answered = postgres_error(exc)
                if answered is not None:
                    raise answered from exc
                raise
            described = [int(d[1]) for d in result.cursor.description]
            names = list(result.keys())
            rows = [_plain(tuple(r)) for r in result.fetchall()]
        # Governance leaves a view's scope out of a star; this read is not governed.
        kept = [i for i, name in enumerate(names) if name != SCOPE_COLUMN]
        return catalog._as_declared(
            sql,
            QueryResult(
                rows=[tuple(row[i] for i in kept) for row in rows],
                column_names=[names[i] for i in kept],
                column_type_oids=[described[i] for i in kept],
            ),
        )


def _can_write(role_id: str, state: Any) -> bool:
    roles = _dict(getattr(state, "roles", None))
    return role_id in roles and has_capability(roles[role_id], Capability.WRITE)


def _bare(sql: str) -> str:
    import re

    return re.sub(r"\$\d+", "NULL", sql)


def _named(setting: str) -> str:
    import re

    return re.sub(r"\$(\d+)", lambda m: f"%(p{int(m[1]) - 1})s", setting)


def _plain(row: tuple) -> tuple:
    return tuple(json.loads(v) if isinstance(v, str) and v[:2] == '{"' else v for v in row)


def _text(value: Any) -> str | None:
    """A test's stated text; a stand-in's unset attribute is none."""
    return value if isinstance(value, str) else None
