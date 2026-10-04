# Copyright (c) 2026 Kenneth Stott
# Canary: a8abb703-feab-49f9-9801-97804a2108f7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An org schema created before this week's columns upgrades in place on PostgreSQL.

V1 ships no migrations: ``schema.sql`` re-runs at every org runtime build, so every column added
after a schema was first created must reach it either through a hand-written
``ADD COLUMN IF NOT EXISTS`` block or through the metadata reconciliation ``init_schema`` now runs
after the script. cloud-dev found the gap the hard way — its API crash-looped on
``column "body_encoding" does not exist`` — so this test strips a fresh org schema back to that
older shape and proves both paths restore it, on a real PostgreSQL.
"""

from __future__ import annotations

import os
import re
import uuid
from pathlib import Path

import pytest
import pytest_asyncio
import sqlalchemy as sa

from provisa.core.db import add_missing_columns, init_schema

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_SCHEMA_SQL = (Path(__file__).parent.parent.parent / "provisa" / "core" / "schema.sql").read_text()
_ORG = "upgrade_probe"
_SCHEMA = f"org_{_ORG}"
_OLD_CHECK = "CHECK (type IN ('openapi', 'graphql_api', 'grpc_api'))"


async def _columns(conn, table: str) -> set[str]:
    rows = await conn.fetch(
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_schema = '{_SCHEMA}' AND table_name = '{table}'"
    )
    return {r["column_name"] for r in rows}


@pytest_asyncio.fixture
async def old_shape(tenant_db):
    """A fresh org schema, then regressed to the pre-REQ-1668 shape."""
    await init_schema(tenant_db, _SCHEMA_SQL, org_id=_ORG)
    async with tenant_db.acquire() as conn:
        await conn.execute(f"SET search_path TO {_SCHEMA}")
        await conn.execute("ALTER TABLE api_endpoints DROP COLUMN body_encoding")
        await conn.execute("ALTER TABLE api_endpoints DROP COLUMN query_template")
        await conn.execute("ALTER TABLE api_endpoints DROP COLUMN response_normalizer")
        await conn.execute("ALTER TABLE sources DROP COLUMN password_ref")
        await conn.execute("ALTER TABLE api_sources DROP CONSTRAINT api_sources_type_check")
        await conn.execute(
            f"ALTER TABLE api_sources ADD CONSTRAINT api_sources_type_check {_OLD_CHECK}"
        )
        assert "body_encoding" not in await _columns(conn, "api_endpoints")
    yield tenant_db
    async with tenant_db.acquire() as conn:
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA}_mv_cache CASCADE")


async def test_init_schema_upgrades_an_older_org_schema_in_place(old_shape):
    await init_schema(old_shape, _SCHEMA_SQL, org_id=_ORG)
    async with old_shape.acquire() as conn:
        await conn.execute(f"SET search_path TO {_SCHEMA}")
        cols = await _columns(conn, "api_endpoints")
        assert {"body_encoding", "query_template", "response_normalizer"} <= cols
        assert "password_ref" in await _columns(conn, "sources")
        # the widened type check admits the query-API kinds an older schema refused
        await conn.execute(
            "INSERT INTO api_sources (id, type, base_url, auth) "
            "VALUES ('probe_neo4j', 'neo4j', 'http://neo4j:7474', '{}')"
        )
        await conn.execute("DELETE FROM api_sources WHERE id = 'probe_neo4j'")


async def test_metadata_reconciliation_restores_a_column_schema_sql_never_alters(old_shape):
    """The reconciliation is what covers a column nobody wrote an ALTER block for: run it alone,
    scoped to the org schema, and the metadata's column comes back typed like schema.sql's."""
    with old_shape.engine.begin() as sa_conn:
        from provisa.core import schema_org

        add_missing_columns(sa_conn, schema_org.metadata.sorted_tables, _SCHEMA)
    async with old_shape.acquire() as conn:
        rows = await conn.fetch(
            "SELECT column_name, data_type, column_default FROM information_schema.columns "
            f"WHERE table_schema = '{_SCHEMA}' AND table_name = 'sources' "
            "AND column_name = 'password_ref'"
        )
        assert len(rows) == 1
        assert rows[0]["data_type"] == "text"
        assert rows[0]["column_default"] == "''::text"
        assert {"body_encoding", "query_template"} <= await _columns(conn, "api_endpoints")


# --- a metadata table schema.sql does not create ---------------------------------------------
#
# ``schema_org`` declares tables ``schema.sql`` has no CREATE for (``provisa_sources``,
# ``query_audit_log``, ``query_sla_log``, ``source_catalog_cache``). The reconciliation creates
# them: in the org's schema, typed as the PostgreSQL plane declares them. Each test below runs on
# a database of its own, so "nothing in public" is a statement about this code and nothing else.

_PG = (
    f"postgresql+psycopg://{os.environ.get('PG_USER', 'provisa')}:"
    f"{os.environ.get('PG_PASSWORD', 'provisa')}@{os.environ.get('PG_HOST', 'localhost')}:"
    f"{os.environ.get('PG_PORT', '5432')}"
)
_NOT_IN_SCHEMA_SQL = ("provisa_sources", "query_audit_log", "query_sla_log", "source_catalog_cache")


@pytest_asyncio.fixture
async def own_database():
    """A ``Database`` on a PostgreSQL database created for the test, and that database's URL."""
    from provisa.core.database import Database, create_engine_from_url

    name = f"schema_probe_{uuid.uuid4().hex[:10]}"
    admin = sa.create_engine(
        f"{_PG}/{os.environ.get('PG_DATABASE', 'provisa')}", isolation_level="AUTOCOMMIT"
    )
    with admin.connect() as conn:
        conn.execute(sa.text(f'CREATE DATABASE "{name}"'))
    db = Database(create_engine_from_url(f"{_PG}/{name}"), name="org", search_path=_SCHEMA)
    try:
        yield db
    finally:
        await db.close()
        with admin.connect() as conn:
            conn.execute(sa.text(f'DROP DATABASE "{name}" WITH (FORCE)'))
        admin.dispose()


def _tables_by_schema(db, names) -> set[tuple[str, str]]:
    with db.engine.connect() as conn:
        rows = conn.execute(
            sa.text(
                "SELECT table_schema, table_name FROM information_schema.tables "
                "WHERE table_name = ANY(:names) AND table_type = 'BASE TABLE'"
            ),
            {"names": list(names)},
        ).fetchall()
    return {(r[0], r[1]) for r in rows}


def _column_shapes(db, schema: str, table: str) -> list[tuple]:
    """(name, type, nullable, default) per column, the default with its schema qualifier removed."""
    with db.engine.connect() as conn:
        rows = conn.execute(
            sa.text(
                "SELECT column_name, udt_name, is_nullable, column_default "
                "FROM information_schema.columns "
                "WHERE table_schema = :schema AND table_name = :table ORDER BY column_name"
            ),
            {"schema": schema, "table": table},
        ).fetchall()
    return [(r[0], r[1], r[2], re.sub(r"\b\w+\.(?=\w+_seq)", "", r[3] or "")) for r in rows]


async def test_a_table_schema_sql_does_not_create_lands_in_the_org_schema(own_database):
    from provisa.api._meta_views import _ops_table_usage_ddl
    from provisa.audit.query_log import AUDIT_SCHEMA_SQL, init_audit_schema
    from provisa.core import schema_org

    db = own_database
    await init_schema(db, _SCHEMA_SQL, org_id=_ORG)

    names = [t.name for t in schema_org.metadata.sorted_tables]
    found = _tables_by_schema(db, names)
    assert {schema for schema, _ in found} == {_SCHEMA}, sorted(found)
    assert {name for _, name in found} == set(names)

    # Every JSON column of the metadata is JSONB in the org schema, created or pre-existing.
    with db.engine.connect() as conn:
        json_columns = conn.execute(
            sa.text(
                "SELECT table_name, column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = :schema AND data_type IN ('json', 'jsonb')"
            ),
            {"schema": _SCHEMA},
        ).fetchall()
    live = {(r[0], r[1]): r[2] for r in json_columns}
    for name in _NOT_IN_SCHEMA_SQL:
        for column in schema_org.metadata.tables[name].columns:
            if isinstance(column.type, sa.JSON):
                assert live[(name, column.name)] == "jsonb", (name, column.name)
    assert "json" not in set(live.values()), sorted(k for k, v in live.items() if v == "json")

    # The audit log this created is the table its own DDL declares, column for column: build that
    # DDL's table in a schema of its own and compare.
    with db.engine.begin() as conn:
        conn.exec_driver_sql("CREATE SCHEMA audit_reference")
        conn.exec_driver_sql("SET LOCAL search_path TO audit_reference")
        conn.exec_driver_sql(AUDIT_SCHEMA_SQL.split("DO $$")[0])
    declared = _column_shapes(db, "audit_reference", "query_audit_log")
    # The record's columns: the statement and its outcome, and the provenance a reader of the
    # record is answered with (model stamp and commit, the rules enforced, why it was routed where
    # it was, the sources read and the age of the data).
    assert {shape[0] for shape in declared} == {
        "id",
        "tenant_id",
        "user_id",
        "role_id",
        "query_hash",
        "query_text_enc",
        "table_ids",
        "source",
        "status_code",
        "duration_ms",
        "route",
        "row_count",
        "trace_id",
        "model_stamp",
        "model_commit",
        "enforced",
        "route_reason",
        "sources",
        "data_age",
        "logged_at",
    }
    assert _column_shapes(db, _SCHEMA, "query_audit_log") == declared
    with db.engine.begin() as conn:
        conn.exec_driver_sql("DROP SCHEMA audit_reference CASCADE")

    # Its initializer then adds what is its own, and what is built on the table works.
    await init_audit_schema(db, org_id=_ORG)
    with db.engine.begin() as conn:
        rules = conn.execute(
            sa.text(
                "SELECT rulename FROM pg_rules WHERE schemaname = :schema "
                "AND tablename = 'query_audit_log' ORDER BY 1"
            ),
            {"schema": _SCHEMA},
        ).fetchall()
        assert [r[0] for r in rules] == ["no_delete_audit", "no_update_audit"]
        conn.exec_driver_sql(f'SET LOCAL search_path TO "{_SCHEMA}"')
        conn.exec_driver_sql(_ops_table_usage_ddl("postgresql"))

    # A second start changes nothing.
    await init_schema(db, _SCHEMA_SQL, org_id=_ORG)
    await init_audit_schema(db, org_id=_ORG)
    assert _tables_by_schema(db, names) == found


async def test_a_table_missing_from_an_org_schema_is_recreated_there(own_database):
    db = own_database
    await init_schema(db, _SCHEMA_SQL, org_id=_ORG)
    with db.engine.begin() as conn:
        conn.exec_driver_sql(f'DROP TABLE "{_SCHEMA}".provisa_sources')
    assert _tables_by_schema(db, ["provisa_sources"]) == set()
    await init_schema(db, _SCHEMA_SQL, org_id=_ORG)
    assert _tables_by_schema(db, ["provisa_sources"]) == {(_SCHEMA, "provisa_sources")}


async def test_the_catalog_cache_table_is_created_in_the_schema_its_readers_use(own_database):
    from provisa.discovery.catalog_cache import CachedTable, ensure_table, read_cache, write_cache

    db = own_database  # scoped to the org schema, as a tenant plane is
    await init_schema(db, _SCHEMA_SQL, org_id=_ORG)
    with db.engine.begin() as conn:
        conn.exec_driver_sql(f'DROP TABLE "{_SCHEMA}".source_catalog_cache')
    await ensure_table(db)
    assert _tables_by_schema(db, ["source_catalog_cache"]) == {(_SCHEMA, "source_catalog_cache")}
    await write_cache(db, "src", "public", [CachedTable("public", "orders", ["id"], None)])
    cached = await read_cache(db, "src", "public")
    assert cached is not None and [t.table_name for t in cached] == ["orders"]


async def test_two_starts_reconciling_one_org_schema_at_once_both_succeed(own_database):
    """A newly provisioned org is initialized by its provisioning and by the first request that
    reaches it, at once. The second reconcile starts while the first has created the missing
    tables and not yet committed: it must wait and then find them, not create them again."""
    import threading

    from provisa.core import schema_org

    db = own_database
    await init_schema(db, _SCHEMA_SQL, org_id=_ORG)
    with db.engine.begin() as conn:
        conn.exec_driver_sql(f'DROP TABLE "{_SCHEMA}".provisa_sources')

    tables = schema_org.metadata.sorted_tables
    second: list[BaseException | str] = []

    def _second_start() -> None:
        try:
            with db.engine.begin() as conn:
                add_missing_columns(conn, tables, _SCHEMA)
            second.append("done")
        except BaseException as exc:  # asserted on below
            second.append(exc)

    with db.engine.begin() as first:
        add_missing_columns(first, tables, _SCHEMA)  # provisa_sources created, not committed
        other = threading.Thread(target=_second_start)
        other.start()
        other.join(1.5)
        assert other.is_alive()  # waiting for the first, not creating beside it
    other.join(30)
    assert second == ["done"], second
    assert _tables_by_schema(db, ["provisa_sources"]) == {(_SCHEMA, "provisa_sources")}
