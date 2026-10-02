# Copyright (c) 2026 Kenneth Stott
# Canary: ece0d501-c072-41fa-b8dc-90c86a2baa3d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A config load manages only what a config declared (REQ-1919).

Every source, domain, role and registered table records where it came from. A load adds and
updates what its file declares, takes over an admin-made object the file declares, and at its end
removes the config-origin objects the file no longer declares — each through the dependency
guard, none of them when any is refused. An object made through the admin is never removed by a
load, in either mode.

The scenarios run here on a SQLite control plane, and unchanged on PostgreSQL in
``tests/integration/test_load_manages_what_config_declared_pg.py``.
"""

# Requirements: REQ-1918, REQ-1919

from __future__ import annotations

import contextlib
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import insert, select

from provisa.core import config_loader, secrets_store
from provisa.core.config_loader import (
    ConfigDropRefused,
    load_config,
    parse_config,
    parse_config_dict,
)
from provisa.core.database import Database, create_engine_from_url
from provisa.core.db import _init_schema_portable
from provisa.core.models import Column, Domain, Role, Source, SourceType, Table
from provisa.core.repositories import domain as domain_repo
from provisa.core.repositories import role as role_repo
from provisa.core.repositories import source as source_repo
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import metadata

_REPO = Path(__file__).resolve().parents[2]


@pytest.fixture(autouse=True)
def no_vault(monkeypatch):
    """The scenarios' sources carry no secret reference, so the load needs no org vault bound."""

    @contextlib.asynccontextmanager
    async def _unbound():
        yield

    monkeypatch.setattr(secrets_store, "bound_to_request_org", _unbound)


@pytest.fixture
async def db() -> Database:
    plane = Database(create_engine_from_url("sqlite+pysqlite:///:memory:"), name="origin-test")
    await _init_schema_portable(plane)
    return plane


# --- the file -------------------------------------------------------------------------------------


def _table(name: str, **extra: Any) -> dict:
    return {
        "source_id": "cfg",
        "domain_id": "sales",
        "schema": "public",
        "table": name,
        "columns": [{"name": "id", "data_type": "integer", "visible_to": ["seller"]}],
        **extra,
    }


def _view(name: str, sql: str) -> dict:
    return {
        "source_id": "__derived__",
        "domain_id": "sales",
        "schema": "views",
        "table": name,
        "view_sql": sql,
        "columns": [{"name": "id", "data_type": "integer", "visible_to": ["seller"]}],
    }


_ORDERS_TO_CUSTOMERS = {
    "id": "orders-to-customers",
    "source_table_id": "orders",
    "target_table_id": "customers",
    "source_column": "id",
    "target_column": "id",
    "cardinality": "many-to-one",
}


def _file(
    *,
    tables: list[dict] | None = None,
    roles: tuple[str, ...] = ("seller",),
    domains: tuple[str, ...] = ("sales",),
    sources: tuple[str, ...] = ("cfg",),
    relationships: list[dict] | None = None,
):
    return parse_config_dict(
        {
            "sources": [
                {"id": s, "type": "postgresql", "host": "h", "port": 5432, "database": "d"}
                for s in sources
            ],
            "domains": [{"id": d} for d in domains],
            "roles": [{"id": r, "capabilities": [], "domain_access": ["sales"]} for r in roles],
            "tables": [_table("orders")] if tables is None else tables,
            "relationships": relationships or [],
        }
    )


async def _load(db: Database, config, *, replace: bool = False) -> None:
    async with db.acquire() as conn:
        await load_config(config, conn, replace=replace)


async def _rows(db: Database, table: str, *columns: str) -> list[tuple]:
    tbl = metadata.tables[table]
    async with db.acquire() as conn:
        found = await conn.execute_core(select(*(tbl.c[c] for c in columns)))
        return sorted(tuple(r) for r in found.fetchall())


async def _origins(db: Database, table: str, key: str = "id") -> dict[str, str]:
    return dict(await _rows(db, table, key, "origin"))  # type: ignore[arg-type]


async def _admin_makes_its_own(db: Database) -> None:
    """A source, a domain, a role and two tables made through the admin — one of the tables on
    the source the config declares."""
    async with db.acquire() as conn:
        await source_repo.upsert(
            conn,
            Source(id="mine", type=SourceType.postgresql, host="h", port=5432, database="d"),
            origin="admin",
        )
        await domain_repo.upsert(conn, Domain(id="lab"), origin="admin")
        await role_repo.upsert(
            conn,
            Role(id="tester", capabilities=[], domain_access=["lab"]),
            org_id="o",
            origin="admin",
        )
        for source_id, name in (("mine", "scratch"), ("cfg", "extra")):
            await table_repo.upsert(
                conn,
                Table(
                    source_id=source_id,
                    domain_id="lab",
                    schema_name="public",
                    table_name=name,
                    columns=[Column(name="id", data_type="integer", visible_to=["tester"])],
                ),
                origin="admin",
            )


# --- what a load creates, and what it leaves alone ------------------------------------------------


async def test_what_a_load_creates_is_the_configs(db):
    await _load(db, _file())
    assert (await _origins(db, "sources"))["cfg"] == "config"
    assert (await _origins(db, "domains"))["sales"] == "config"
    assert (await _origins(db, "roles"))["seller"] == "config"
    assert await _origins(db, "registered_tables", "table_name") == {"orders": "config"}


@pytest.mark.parametrize("replace", [False, True])
async def test_a_load_never_removes_what_the_admin_made(db, replace):
    await _load(db, _file(), replace=replace)
    await _admin_makes_its_own(db)

    await _load(db, _file(), replace=replace)
    await _load(db, _file(tables=[], roles=(), domains=("sales",)), replace=replace)

    assert (await _origins(db, "sources"))["mine"] == "admin"
    assert (await _origins(db, "domains"))["lab"] == "admin"
    assert (await _origins(db, "roles"))["tester"] == "admin"
    # The table the admin registered on the config's own source is still there too.
    assert await _origins(db, "registered_tables", "table_name") == {
        "extra": "admin",
        "scratch": "admin",
    }


async def test_a_file_that_declares_an_admin_made_object_takes_it_over(db, caplog):
    await _load(db, _file())
    await _admin_makes_its_own(db)

    with caplog.at_level("INFO", logger="provisa.core.repositories.origin"):
        await _load(
            db,
            _file(
                roles=("seller", "tester"),
                domains=("sales", "lab"),
                sources=("cfg", "mine"),
                tables=[_table("orders"), _table("extra", domain_id="lab")],
            ),
        )

    assert (await _origins(db, "sources"))["mine"] == "config"
    assert (await _origins(db, "domains"))["lab"] == "config"
    assert (await _origins(db, "roles"))["tester"] == "config"
    assert await _origins(db, "registered_tables", "table_name") == {
        "extra": "config",
        "orders": "config",
        "scratch": "admin",
    }
    assert sorted(r.getMessage().split(",")[0] for r in caplog.records) == [
        "config load takes over domain 'lab'",
        "config load takes over role 'tester'",
        "config load takes over source 'mine'",
        "config load takes over table 'cfg.public.extra'",
    ]


# --- what the file dropped ------------------------------------------------------------------------


@pytest.mark.parametrize("replace", [False, True])
async def test_a_config_role_dropped_from_the_file_is_removed_when_nothing_depends_on_it(
    db, replace
):
    await _load(db, _file(roles=("seller", "auditor")), replace=replace)
    await _load(db, _file(), replace=replace)
    assert "auditor" not in await _origins(db, "roles")
    assert "seller" in await _origins(db, "roles")


async def test_a_config_role_someone_holds_is_refused_naming_the_holder(db):
    await _load(db, _file(roles=("seller", "auditor"), domains=("sales", "spare")))
    async with db.acquire() as conn:
        await conn.execute_core(
            insert(metadata.tables["user_role_assignments"]).values(
                user_id="bob", role_id="auditor", domain_id="*"
            )
        )

    with pytest.raises(ConfigDropRefused) as err:
        await _load(db, _file())

    report = err.value.report()
    assert [(r["kind"], r["id"]) for r in report] == [("role", "auditor")]
    assert [(d["kind"], d["name"]) for d in report[0]["dependents"]] == [
        ("role_assignment", "bob holds auditor")
    ]
    assert "role 'auditor' is depended on by role assignment 'bob holds auditor'" in str(err.value)
    # Nothing is removed unless everything may go: the domain the file also dropped is still there.
    assert "auditor" in await _origins(db, "roles")
    assert "spare" in await _origins(db, "domains")


async def test_every_refusal_is_reported_in_the_one_error(db):
    await _load(
        db,
        _file(
            roles=("seller", "auditor"),
            tables=[
                _table("orders"),
                _table("customers"),
                _view("big_orders", "SELECT id FROM orders"),
            ],
        ),
    )
    async with db.acquire() as conn:
        await conn.execute_core(
            insert(metadata.tables["user_role_assignments"]).values(
                user_id="bob", role_id="auditor", domain_id="*"
            )
        )

    with pytest.raises(ConfigDropRefused) as err:
        await _load(
            db, _file(tables=[_table("customers"), _view("big_orders", "SELECT id FROM orders")])
        )

    assert [(r["kind"], r["name"]) for r in err.value.report()] == [
        ("table", "cfg.public.orders"),
        ("role", "auditor"),
    ]


@pytest.mark.parametrize("replace", [False, True])
async def test_a_load_dropping_a_table_with_its_relationship_succeeds(db, replace):
    both = [_table("orders"), _table("customers")]
    await _load(db, _file(tables=both, relationships=[_ORDERS_TO_CUSTOMERS]), replace=replace)
    assert await _rows(db, "relationships", "id") == [("orders-to-customers",)]

    await _load(db, _file(tables=[_table("customers")]), replace=replace)

    assert await _origins(db, "registered_tables", "table_name") == {"customers": "config"}
    assert await _rows(db, "relationships", "id") == []
    assert await _rows(db, "table_columns", "column_name") == [("id",)]


@pytest.mark.parametrize("replace", [False, True])
async def test_a_load_dropping_a_table_a_remaining_view_reads_fails_naming_the_view(db, replace):
    view = _view("big_orders", "SELECT id FROM orders")
    await _load(db, _file(tables=[_table("orders"), view]), replace=replace)

    with pytest.raises(ConfigDropRefused) as err:
        await _load(db, _file(tables=[view]), replace=replace)

    report = err.value.report()
    assert [(r["kind"], r["name"]) for r in report] == [("table", "cfg.public.orders")]
    assert [(d["kind"], d["name"]) for d in report[0]["dependents"]] == [("table", "big_orders")]
    assert await _origins(db, "registered_tables", "table_name") == {
        "big_orders": "config",
        "orders": "config",
    }


async def test_a_view_dropped_with_the_table_it_reads_goes_with_it(db):
    await _load(db, _file(tables=[_table("orders"), _view("big_orders", "SELECT id FROM orders")]))
    await _load(db, _file(tables=[]))
    assert await _origins(db, "registered_tables", "table_name") == {}


async def test_a_source_dropped_with_its_tables_goes_and_one_holding_an_admin_table_is_refused(
    db,
):
    two = _file(
        sources=("cfg", "old"), tables=[_table("orders"), _table("legacy", source_id="old")]
    )
    await _load(db, two)
    await _load(db, _file())
    assert "old" not in await _origins(db, "sources")
    assert await _origins(db, "registered_tables", "table_name") == {"orders": "config"}

    await _load(db, two)
    async with db.acquire() as conn:
        await table_repo.upsert(
            conn,
            Table(
                source_id="old",
                domain_id="sales",
                schema_name="public",
                table_name="kept",
                columns=[Column(name="id", data_type="integer", visible_to=["seller"])],
            ),
            origin="admin",
        )
    with pytest.raises(ConfigDropRefused) as err:
        await _load(db, _file())
    report = err.value.report()
    assert [(r["kind"], r["id"]) for r in report] == [("source", "old")]
    assert [(d["kind"], d["name"]) for d in report[0]["dependents"]] == [("table", "kept")]


async def test_a_config_domain_a_role_still_reaches_is_refused_naming_the_role(db):
    await _load(db, _file(domains=("sales", "spare")))
    async with db.acquire() as conn:
        await role_repo.upsert(
            conn,
            Role(id="tester", capabilities=[], domain_access=["spare"]),
            org_id="o",
            origin="admin",
        )
    with pytest.raises(ConfigDropRefused) as err:
        await _load(db, _file())
    report = err.value.report()
    assert [(r["kind"], r["id"]) for r in report] == [("domain", "spare")]
    assert [(d["kind"], d["name"]) for d in report[0]["dependents"]] == [("role", "tester")]


# --- a table whose schema the file corrected ------------------------------------------------------


@pytest.mark.parametrize("replace", [False, True])
async def test_a_table_the_file_moved_to_another_schema_is_the_same_table(db, replace):
    both = [_table("orders"), _table("customers")]
    await _load(db, _file(tables=both, relationships=[_ORDERS_TO_CUSTOMERS]), replace=replace)
    before = dict(await _rows(db, "registered_tables", "table_name", "id"))

    moved = [_table("orders", schema="sales"), _table("customers")]
    await _load(db, _file(tables=moved, relationships=[_ORDERS_TO_CUSTOMERS]), replace=replace)

    assert await _rows(db, "registered_tables", "table_name", "schema_name") == [
        ("customers", "public"),
        ("orders", "sales"),
    ]
    assert dict(await _rows(db, "registered_tables", "table_name", "id")) == before
    assert await _rows(db, "relationships", "id") == [("orders-to-customers",)]


# --- the demo configuration -----------------------------------------------------------------------


async def test_the_install_configuration_loads_again_with_admin_made_objects_present(db):
    """The boot's own load: replace mode, the configuration the installer ships, run a second
    time over a control plane that also holds objects made through the admin."""
    config = parse_config(_REPO / "config" / "provisa-install.yaml")
    await _load(db, config, replace=True)
    await _admin_makes_its_own_beside(db, config)

    await _load(db, parse_config(_REPO / "config" / "provisa-install.yaml"), replace=True)

    assert (await _origins(db, "sources"))["mine"] == "admin"
    assert (await _origins(db, "domains"))["lab"] == "admin"
    assert (await _origins(db, "roles"))["tester"] == "admin"
    tables = await _origins(db, "registered_tables", "table_name")
    assert (tables["scratch"], tables["extra"]) == ("admin", "admin")


async def _admin_makes_its_own_beside(db: Database, config) -> None:
    """The admin's objects, with its second table on a source the shipped file declares."""
    declared = config.sources[0].id
    async with db.acquire() as conn:
        await source_repo.upsert(
            conn,
            Source(id="mine", type=SourceType.postgresql, host="h", port=5432, database="d"),
            origin="admin",
        )
        await domain_repo.upsert(conn, Domain(id="lab"), origin="admin")
        await role_repo.upsert(
            conn,
            Role(id="tester", capabilities=[], domain_access=["lab"]),
            org_id="o",
            origin="admin",
        )
        for source_id, name in (("mine", "scratch"), (declared, "extra")):
            await table_repo.upsert(
                conn,
                Table(
                    source_id=source_id,
                    domain_id="lab",
                    schema_name="public",
                    table_name=name,
                    columns=[Column(name="id", data_type="integer", visible_to=["tester"])],
                ),
                origin="admin",
            )


def test_the_loader_no_longer_removes_these_kinds_by_any_other_path():
    """The replace cleanup and the per-source purge are gone: the one removal of a source, domain,
    role or table in a load is the guarded one at its end."""
    source = Path(config_loader.__file__).read_text()
    assert "_purge_removed_tables" not in source
    assert not hasattr(role_repo, "delete_all_except")
    assert not hasattr(domain_repo, "delete_all_except")
    for call in ("table_repo.remove_registrations(", "source_repo.remove_where("):
        assert call not in source, call


def test_the_export_writes_no_origin():
    """An exported config is a file like any other: what it declares becomes the config's when
    it is loaded, whatever made the objects it was exported from."""
    from provisa.api.admin import config_export

    for keys in (
        config_export._ROLE_KEYS,
        config_export._DOMAIN_KEYS,
        config_export._TABLE_KEYS,
    ):
        assert "origin" not in keys
