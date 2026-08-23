# Copyright (c) 2026 Kenneth Stott
# Canary: 5b1c9e30-77af-4d62-9a18-e0c34b7d5f21
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Which control-plane stores can actually hold an environment (REQ-1488, REQ-1534).

REQ-1488 makes an environment a SCHEMA, so the question "does this control plane support
environments" is the question "can this backend hold two same-named tables in two namespaces and
let a connection be scoped to one of them". That is what these tests ask each backend directly,
against the real server, rather than trusting the capability table.

Two facts are reported per backend and they are not the same fact:

  * ``namespaces`` — the BACKEND can do it: create a second namespace, put the same table in both,
    keep the rows apart.
  * ``scoped`` — PROVISA can reach it: ``Capabilities.enter_org_sql`` returns a statement that
    scopes an unqualified query to one namespace, which is how ``Database.acquire`` puts a
    connection into an org's environment.

A backend that has namespaces but no scoping statement is a gap in Provisa's dialect table, not a
limit of the store — and the assertions say which, so a later dialect entry turns a documented
"not reachable" into a passing capability without rewriting the probe.
"""

from __future__ import annotations

import os

import pytest

from provisa.core.database import Capabilities, Database, create_engine_from_url
from provisa.core.environments import org_schema

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_ORG = "cpprobe"
_TABLE = "env_probe"
#: prod keeps the pre-environment name; dev is the environment the org is asked to add to it.
_PROD = org_schema(_ORG, "prod")
_DEV = org_schema(_ORG, "dev")


class _Ns:
    """The DDL by which one dialect makes, fills and removes a namespace.

    Each backend spells "another namespace" its own way — a PostgreSQL/SQL Server SCHEMA, a MySQL
    DATABASE, an Oracle USER — and all three are the same thing for REQ-1488's purpose: a container
    in which the org's tables can exist twice under one name.
    """

    def __init__(self, create, drop, qualify=lambda ns, t: f"{ns}.{t}", column="VARCHAR(16)"):
        self.create = create
        self.drop = drop
        self.qualify = qualify
        self.column = column


_NS = {
    "postgresql": _Ns(
        create=lambda ns: [f"CREATE SCHEMA IF NOT EXISTS {ns}"],
        drop=lambda ns: [f"DROP SCHEMA IF EXISTS {ns} CASCADE"],
    ),
    "mysql": _Ns(
        create=lambda ns: [f"CREATE DATABASE IF NOT EXISTS {ns}"],
        drop=lambda ns: [f"DROP DATABASE IF EXISTS {ns}"],
    ),
    "mssql": _Ns(
        # SQL Server has no CREATE SCHEMA IF NOT EXISTS and no cascading drop: the table goes
        # first, and each statement is guarded by a catalog lookup instead.
        create=lambda ns: [
            f"IF SCHEMA_ID('{ns}') IS NULL EXEC('CREATE SCHEMA {ns}')",
        ],
        drop=lambda ns: [
            f"IF OBJECT_ID('{ns}.{_TABLE}', 'U') IS NOT NULL DROP TABLE {ns}.{_TABLE}",
            f"IF SCHEMA_ID('{ns}') IS NOT NULL DROP SCHEMA {ns}",
        ],
        column="NVARCHAR(16)",
    ),
    "oracle": _Ns(
        # An Oracle schema IS a user, so creating one is creating an account and giving it room to
        # store a table. Dropping the user cascades its objects, which is Oracle's DROP SCHEMA.
        create=lambda ns: [
            f'CREATE USER {ns} IDENTIFIED BY "Provisa_2026"',
            f"ALTER USER {ns} QUOTA UNLIMITED ON USERS",
        ],
        drop=lambda ns: [f"DROP USER {ns} CASCADE"],
        column="VARCHAR2(16)",
    ),
}


async def _quiet(conn, statements: list[str]) -> None:
    """Run teardown DDL, ignoring what was never created.

    Only teardown uses this: a probe that cannot build its namespaces must fail, but a probe that
    fails halfway must still remove the half it built, and "this one was not there" is the ordinary
    case rather than an error.
    """
    for sql in statements:
        try:
            await conn.execute(sql)
        except Exception as exc:  # noqa: BLE001 - allow-blind-except: teardown of absent objects
            print(f"teardown: {sql} -> {exc}")


async def _probe(url: str, *, via_loader: bool = True) -> dict[str, object]:
    """Ask one live control plane whether it can hold `prod` and `dev` side by side.

    Returns the two facts the module docstring names plus the dialect, so a caller can assert on
    the one it means and print the whole row when it fails.

    *via_loader* false builds the engine with SQLAlchemy directly. ``_ADMIN_ASYNC_DRIVER`` lists
    only postgresql, sqlite, duckdb, mysql and mariadb, so ``create_engine_from_url`` refuses a
    SQL Server or Oracle URI outright — and refusing to build the engine would answer a question
    about Provisa's driver table, not about the store. The bypass measures the STORE; the separate
    assertions below record the refusal.
    """
    if via_loader:
        engine = create_engine_from_url(url)
    else:
        from sqlalchemy.ext.asyncio import create_async_engine

        engine = create_async_engine(url, pool_pre_ping=True)
    plain = Database(engine, "probe")
    dialect = plain.dialect
    ns = _NS[dialect]
    caps = Capabilities.for_dialect(dialect)
    try:
        async with plain.acquire() as conn:
            await _quiet(conn, [s for n in (_PROD, _DEV) for s in ns.drop(n)])
            for n, mark in ((_PROD, "in-prod"), (_DEV, "in-dev")):
                for sql in ns.create(n):
                    await conn.execute(sql)
                await conn.execute(f"CREATE TABLE {ns.qualify(n, _TABLE)} (v {ns.column})")
                await conn.execute(f"INSERT INTO {ns.qualify(n, _TABLE)} (v) VALUES ('{mark}')")

        # Qualified reads: does the backend keep two same-named tables apart at all?
        async with plain.acquire() as conn:
            prod_v = await conn.fetchval(f"SELECT v FROM {ns.qualify(_PROD, _TABLE)}")
            dev_v = await conn.fetchval(f"SELECT v FROM {ns.qualify(_DEV, _TABLE)}")
        namespaces = (prod_v, dev_v) == ("in-prod", "in-dev")

        # Scoped reads: does Provisa's own enter_org_sql put a connection INSIDE one of them?
        scoped = False
        if caps.enter_org_sql(_DEV):
            scoped_db = Database(engine, "probe", search_path=_DEV)
            async with scoped_db.acquire() as conn:
                scoped = await conn.fetchval(f"SELECT v FROM {_TABLE}") == "in-dev"
        return {"dialect": dialect, "namespaces": namespaces, "scoped": scoped}
    finally:
        async with plain.acquire() as conn:
            await _quiet(conn, [s for n in (_PROD, _DEV) for s in ns.drop(n)])
        await engine.dispose()


async def test_postgresql_control_plane_holds_an_environment():
    """The reference plane: schemas, and a search_path that enters one (REQ-1488)."""
    port = os.environ["PG_PORT"]
    result = await _probe(f"postgresql+asyncpg://provisa:provisa@localhost:{port}/provisa")
    assert result == {"dialect": "postgresql", "namespaces": True, "scoped": True}


@pytest.mark.requires_mariadb
async def test_mysql_control_plane_holds_an_environment():
    """MySQL/MariaDB: a DATABASE is the namespace, and USE enters it.

    The backend is capable, so REQ-1534's refusal of a non-PostgreSQL plane is Provisa's bootstrap
    (``init_schema`` sends every non-PG dialect down a portable path that ignores *env*), not this
    store's limit.
    """
    port = os.environ["MARIADB_PORT"]
    result = await _probe(f"mysql+aiomysql://root:provisa@localhost:{port}/provisa")
    assert result == {"dialect": "mysql", "namespaces": True, "scoped": True}


@pytest.mark.requires_sqlserver
async def test_sqlserver_has_namespaces_that_provisa_can_neither_open_nor_enter():
    """SQL Server HAS schemas; Provisa reaches none of it, in two independent places.

    The control-plane loader has no mssql driver, so the store cannot be opened at all, and
    ``Capabilities.for_dialect`` has no mssql branch either, so even an opened one would report no
    schemas and hand back no scoping statement. Both are entries Provisa has not written, which is
    why the probe bypasses the loader to show the server itself keeping the two environments apart.
    """
    pytest.importorskip("aioodbc", reason="aioodbc/unixODBC not available")
    port = os.environ["SQLSERVER_PORT"]
    url = (
        f"mssql+aioodbc://sa:Provisa_2026%21@localhost:{port}/master"
        "?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=yes&TrustServerCertificate=yes"
    )
    with pytest.raises(ValueError, match="unsupported control-plane store backend 'mssql'"):
        create_engine_from_url(url)
    assert Capabilities.for_dialect("mssql").schemas is False
    result = await _probe(url, via_loader=False)
    assert result["namespaces"] is True, result
    assert result["scoped"] is False, result


@pytest.mark.requires_oracle
async def test_oracle_holds_an_environment_but_is_not_an_openable_control_plane():
    """Oracle: the schema IS a user, and ALTER SESSION SET CURRENT_SCHEMA enters it.

    The dialect table already knows that — ``Capabilities.for_dialect('oracle')`` reports schemas
    and returns the statement. The loader's driver table does not, so the store is unreachable for
    a reason that has nothing to do with what it can hold.
    """
    port = os.environ["ORACLE_PORT"]
    url = f"oracle+oracledb_async://system:provisa@localhost:{port}/?service_name=FREEPDB1"
    with pytest.raises(ValueError, match="unsupported control-plane store backend 'oracle'"):
        create_engine_from_url(url)
    result = await _probe(url, via_loader=False)
    assert result == {"dialect": "oracle", "namespaces": True, "scoped": True}


async def test_sqlite_control_plane_has_one_namespace(tmp_path):
    """SQLite has no second namespace to give an environment, which is why REQ-1534 refuses one.

    No container and no marker: the store is a file, so this runs everywhere the suite does, and it
    is the end of the matrix the other cases are measured against.
    """
    url = f"sqlite+aiosqlite:///{tmp_path}/control.db"
    db = Database(create_engine_from_url(url), "probe")
    assert db.capabilities.schemas is False
    assert db.capabilities.enter_org_sql(_DEV) is None
    async with db.acquire() as conn:
        with pytest.raises(Exception) as err:
            await conn.execute(f"CREATE SCHEMA {_DEV}")
    assert "SCHEMA" in str(err.value).upper()
    await db.engine.dispose()
