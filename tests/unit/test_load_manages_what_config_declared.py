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
    await seed_the_view_source(plane)
    return plane


async def seed_the_view_source(plane: Database) -> None:
    """The built-in source views are registered on, which the startup seed creates before any
    config is loaded."""
    async with plane.acquire() as conn:
        await conn.execute_core(
            insert(metadata.tables["sources"]).values(
                id="__derived__", type="postgresql", origin="seed"
            )
        )


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
    **more: list[dict],
):
    """A config file. ``more`` carries any other list a config declares (functions, metrics,
    rls_rules, tags, tag_assignments, glossary_terms, data_products, webhooks)."""
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
            **more,
        }
    )


def _command(name: str, domain: str = "sales") -> dict:
    return {
        "name": name,
        "source_id": "cfg",
        "function_name": name,
        "returns": "cfg.public.orders",
        "domain_id": domain,
    }


def _metric(name: str) -> dict:
    return {"name": name, "expression": "SUM(orders.id)"}


async def _load(db: Database, config, *, origin: str = "config") -> None:
    async with db.acquire() as conn:
        await load_config(config, conn, origin=origin)


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


async def test_a_load_never_removes_what_the_admin_made(db):
    await _load(db, _file())
    await _admin_makes_its_own(db)

    await _load(db, _file())
    await _load(db, _file(tables=[], roles=(), domains=("sales",)))

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


async def test_a_config_role_dropped_from_the_file_is_removed_when_nothing_depends_on_it(db):
    await _load(db, _file(roles=("seller", "auditor")))
    await _load(db, _file())
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


async def test_a_load_dropping_a_table_with_its_relationship_succeeds(db):
    both = [_table("orders"), _table("customers")]
    await _load(db, _file(tables=both, relationships=[_ORDERS_TO_CUSTOMERS]))
    assert await _rows(db, "relationships", "id") == [("orders-to-customers",)]

    await _load(db, _file(tables=[_table("customers")]))

    assert await _origins(db, "registered_tables", "table_name") == {"customers": "config"}
    assert await _rows(db, "relationships", "id") == []
    assert await _rows(db, "table_columns", "column_name") == [("id",)]


async def test_a_load_dropping_a_table_a_remaining_view_reads_fails_naming_the_view(db):
    view = _view("big_orders", "SELECT id FROM orders")
    await _load(db, _file(tables=[_table("orders"), view]))

    with pytest.raises(ConfigDropRefused) as err:
        await _load(db, _file(tables=[view]))

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


# --- every kind a config can declare (REQ-1919) ----------------------------------------------


async def test_a_file_that_no_longer_declares_a_domain_or_its_command_removes_both(db):
    """The action-governance sequence: a file declared a domain and a command in it; the next
    file declares neither. Both are the config's, both are dropped, so neither blocks the other."""
    await _load(
        db,
        _file(
            domains=("sales", "analytics"),
            tables=[_table("orders")],
            functions=[_command("enrich_orders", "analytics")],
        ),
    )
    assert "enrich_orders" in await _origins(db, "tracked_functions", "name")

    await _load(db, _file())

    assert "enrich_orders" not in await _origins(db, "tracked_functions", "name")
    assert "analytics" not in await _origins(db, "domains")


async def test_dropping_a_domain_with_its_command_relationship_and_metric_loads_cleanly(db):
    both = [_table("orders"), _table("customers", domain_id="analytics")]
    await _load(
        db,
        _file(
            domains=("sales", "analytics"),
            tables=both,
            relationships=[_ORDERS_TO_CUSTOMERS],
            functions=[_command("enrich_orders", "analytics")],
            metrics=[_metric("order_count")],
        ),
    )

    await _load(db, _file(tables=[_table("orders")]))

    assert "analytics" not in await _origins(db, "domains")
    assert await _rows(db, "relationships", "id") == []
    assert await _origins(db, "metrics", "name") == {}
    assert await _origins(db, "tracked_functions", "name") == {}


async def test_dropping_a_domain_while_still_declaring_its_command_is_refused_naming_it(db):
    await _load(
        db,
        _file(domains=("sales", "analytics"), functions=[_command("enrich_orders", "analytics")]),
    )

    with pytest.raises(ConfigDropRefused) as err:
        await _load(db, _file(functions=[_command("enrich_orders", "analytics")]))

    report = err.value.report()
    assert [(r["kind"], r["id"]) for r in report] == [("domain", "analytics")]
    assert [(d["kind"], d["name"]) for d in report[0]["dependents"]] == [
        ("command", "enrich_orders")
    ]


async def test_what_the_admin_made_of_every_kind_survives_a_load(db):
    """A relationship, metric, command, webhook, data product, tag, tag assignment, row filter
    and glossary term made through the admin are none of the file's business."""
    from provisa.core.models import (
        DataProduct,
        Function,
        Metric,
        Relationship,
        RLSRule,
        Tag,
        TagAssignment,
        Webhook,
    )
    from provisa.core.repositories import data_product as data_product_repo
    from provisa.core.repositories import function as function_repo
    from provisa.core.repositories import glossary as glossary_repo
    from provisa.core.repositories import metric as metric_repo
    from provisa.core.repositories import relationship as relationship_repo
    from provisa.core.repositories import rls as rls_repo
    from provisa.core.repositories import tag as tag_repo

    await _load(db, _file(tables=[_table("orders"), _table("customers")]))
    async with db.acquire() as conn:
        await relationship_repo.upsert(
            conn,
            Relationship(
                id="mine",
                source_table_id="orders",
                target_table_id="customers",
                source_column="id",
                target_column="id",
                cardinality="many-to-one",
            ),
            origin="admin",
        )
        await metric_repo.upsert(
            conn, Metric(name="my_count", expression="SUM(orders.id)"), origin="admin"
        )
        await function_repo.upsert_function(
            conn,
            Function(
                name="mine", source_id="cfg", function_name="mine", returns="", domain_id="sales"
            ),
            origin="admin",
        )
        await function_repo.upsert_webhook(
            conn, Webhook(name="hook", url="http://x", domain_id="sales"), origin="admin"
        )
        await data_product_repo.upsert(
            conn, DataProduct(id="core", domain_id="sales", name="Core"), origin="admin"
        )
        await tag_repo.upsert(conn, Tag(id="finance"), origin="admin")
        orders_id = dict(await _rows(db, "registered_tables", "table_name", "id"))["orders"]
        await tag_repo.assign(
            conn,
            TagAssignment(tag_id="pii", object_type="column", table_id=orders_id, column_name="id"),
            origin="admin",
        )
        await rls_repo.upsert(
            conn, RLSRule(table_id="orders", role_id="seller", filter="id > 0"), origin="admin"
        )
        await glossary_repo.create_abstract_term(conn, "Revenue", domains=set())

    await _load(db, _file(tables=[_table("orders"), _table("customers")]))

    assert await _rows(db, "relationships", "id", "origin") == [("mine", "admin")]
    assert await _origins(db, "metrics", "name") == {"my_count": "admin"}
    assert await _origins(db, "tracked_functions", "name") == {"mine": "admin"}
    assert await _origins(db, "tracked_webhooks", "name") == {"hook": "admin"}
    assert await _origins(db, "data_products") == {"core": "admin"}
    assert await _origins(db, "tags") == {"finance": "admin"}
    assert [o for (o,) in await _rows(db, "tag_assignments", "origin")] == ["admin"]
    assert [o for (o,) in await _rows(db, "rls_rules", "origin")] == ["admin"]
    assert (await _origins(db, "glossary_terms", "name"))["revenue"] == "admin"


async def test_a_tag_assignment_the_file_drops_is_removed_while_its_tag_and_table_stay(db):
    tagged = [{"tag_id": "pii", "object_type": "table", "table_ref": "cfg.public.orders"}]
    await _load(db, _file(tag_assignments=tagged))
    assert [o for (o,) in await _rows(db, "tag_assignments", "origin")] == ["config"]

    await _load(db, _file())

    assert await _rows(db, "tag_assignments", "id") == []
    assert await _origins(db, "registered_tables", "table_name") == {"orders": "config"}


async def test_a_row_filter_the_file_drops_is_removed_loudly(db, caplog):
    """Removing it widens what the role reads, so the load says so: a WARNING naming the role,
    the table and the rule's id, never the predicate, and an entry in the org's trail."""
    rule = {"table_id": "orders", "role_id": "seller", "filter": "region = 'eu-secret'"}
    await _load(db, _file(rls_rules=[rule]))
    ((rule_id,),) = await _rows(db, "rls_rules", "id")

    with caplog.at_level("WARNING", logger="provisa.core.config_loader"):
        await _load(db, _file())

    assert await _rows(db, "rls_rules", "id") == []
    warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert warnings == [
        f"config load removes row filter {rule_id} (seller on table orders): the config no "
        "longer declares it, so role 'seller' now reads what it filtered"
    ]
    assert "eu-secret" not in warnings[0]
    trail = await _rows(db, "admin_audit_log", "action", "actor_id", "subject_id")
    assert trail == [("row_filter.removed_by_config_load", "config-load", "seller")]


async def test_a_glossary_term_the_file_drops_goes_while_abstract_and_stays_once_rooted(db):
    await _load(
        db,
        _file(
            # A capital letter in the file: the catalog's names are lowercase (REQ-1844).
            glossary_terms=[
                {"name": "Bookings", "domains": ["sales"], "edges": [{"to": "ID"}]},
                {"name": "id", "domains": ["sales"], "definition": "the row's key"},
            ]
        ),
    )
    # "id" is also the column of orders: it is rooted, derived from the column as well.
    terms = await _origins(db, "glossary_terms", "name")
    assert terms["bookings"] == "config"

    await _load(db, _file())

    terms = await _origins(db, "glossary_terms", "name")
    assert "bookings" not in terms
    assert terms.get("id") in (None, "seed")


async def test_the_loader_removes_nothing_during_the_load():
    """Relationships, metrics, commands and webhooks are no longer deleted as a set while the
    load runs: everything a file dropped is judged at its end."""
    source = Path(config_loader.__file__).read_text()
    for call in (
        "rel_repo.remove_where(",
        "metric_repo.remove_where(",
        "function_repo.remove_all(",
    ):
        assert call not in source, call
    assert "_replace_mode_cleanup" not in source


# --- an import through the admin is not a load of the deployment's file -----------------------


async def test_an_import_through_the_admin_removes_nothing_and_changes_no_origin(db):
    """An import adds and updates. What the deployment's file declared and the import does not
    mention stays; an admin-made object the import names is not taken over; what the import
    creates is the admin's."""
    await _load(
        db,
        _file(
            roles=("seller", "auditor"),
            domains=("sales", "spare"),
            tables=[_table("orders"), _table("customers")],
        ),
    )
    await _admin_makes_its_own(db)
    before = {
        table: await _origins(db, table, key)
        for table, key in (
            ("sources", "id"),
            ("domains", "id"),
            ("roles", "id"),
            ("registered_tables", "table_name"),
        )
    }

    # The imported model names one config object (the source and a table), one admin-made
    # object (the role "tester") and one new table; it mentions nothing else.
    imported = _file(
        roles=("tester",), domains=("sales",), tables=[_table("orders"), _table("imported")]
    )
    await _load(db, imported, origin="admin")

    after = {
        table: await _origins(db, table, key)
        for table, key in (
            ("sources", "id"),
            ("domains", "id"),
            ("roles", "id"),
            ("registered_tables", "table_name"),
        )
    }
    # Nothing is gone, and nothing that existed changed origin.
    for table, origins in before.items():
        for ident, origin in origins.items():
            assert after[table].get(ident) == origin, (table, ident)
    # What the import created is the admin's.
    assert after["registered_tables"]["imported"] == "admin"
    assert set(after["registered_tables"]) == set(before["registered_tables"]) | {"imported"}


async def test_the_load_refuses_an_origin_it_does_not_know(db):
    async with db.acquire() as conn:
        with pytest.raises(ValueError, match="origin must be one of"):
            await load_config(_file(), conn, origin="import")


# --- a secondary worker only upserts (REQ-1229) ---------------------------------------------------


async def test_a_secondarys_load_removes_nothing_and_raises_nothing(db, monkeypatch):
    """Only the primary's load removes what the file dropped. A secondary — whose file may be
    older than the primary's — upserts, and neither removes a dropped object nor refuses one
    that is still depended on."""
    view = _view("big_orders", "SELECT id FROM orders")
    await _load(
        db,
        _file(
            roles=("seller", "auditor"), domains=("sales", "spare"), tables=[_table("orders"), view]
        ),
    )

    monkeypatch.setenv("PROVISA_ROLE", "secondary")
    # Drops a role and a domain nothing depends on, and a table a remaining view reads.
    await _load(db, _file(tables=[view]))

    assert {"seller", "auditor"} <= set(await _origins(db, "roles"))
    assert "spare" in await _origins(db, "domains")
    assert await _origins(db, "registered_tables", "table_name") == {
        "big_orders": "config",
        "orders": "config",
    }

    # The primary's load of the same file is the one that judges it.
    monkeypatch.setenv("PROVISA_ROLE", "primary")
    with pytest.raises(ConfigDropRefused):
        await _load(db, _file(tables=[view]))


def test_a_worker_is_the_primary_unless_started_as_a_secondary():
    assert config_loader.is_primary_worker({}) is True
    assert config_loader.is_primary_worker({"PROVISA_ROLE": "primary"}) is True
    assert config_loader.is_primary_worker({"PROVISA_ROLE": " Secondary "}) is False


# --- a table whose schema the file corrected ------------------------------------------------------


async def test_a_table_the_file_moved_to_another_schema_is_the_same_table(db):
    both = [_table("orders"), _table("customers")]
    await _load(db, _file(tables=both, relationships=[_ORDERS_TO_CUSTOMERS]))
    before = dict(await _rows(db, "registered_tables", "table_name", "id"))

    moved = [_table("orders", schema="sales"), _table("customers")]
    await _load(db, _file(tables=moved, relationships=[_ORDERS_TO_CUSTOMERS]))

    assert await _rows(db, "registered_tables", "table_name", "schema_name") == [
        ("customers", "public"),
        ("orders", "sales"),
    ]
    assert dict(await _rows(db, "registered_tables", "table_name", "id")) == before
    assert await _rows(db, "relationships", "id") == [("orders-to-customers",)]


# --- the demo configuration -----------------------------------------------------------------------


async def test_the_install_configuration_loads_again_with_admin_made_objects_present(db):
    """The boot's own load: the configuration the installer ships, run a second
    time over a control plane that also holds objects made through the admin."""
    config = parse_config(_REPO / "config" / "provisa-install.yaml")
    await _load(db, config)
    await _admin_makes_its_own_beside(db, config)

    await _load(db, parse_config(_REPO / "config" / "provisa-install.yaml"))

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


def test_every_row_the_schema_seeds_states_its_origin():
    """The PostgreSQL schema seeds roles and domains by INSERT, some several rows at a time; the
    column has no default, so each row of each statement says "seed"."""
    import re

    sql = (_REPO / "provisa" / "core" / "schema.sql").read_text()
    statements = list(
        re.finditer(
            r"INSERT INTO (roles|domains|sources|registered_tables) \(([^)]*)\)\s*VALUES(.*?);",
            sql,
            re.S,
        )
    )
    assert statements, "no seed statements found"
    for statement in statements:
        columns = [c.strip() for c in statement.group(2).split(",")]
        assert columns[-1] == "origin", statement.group(0)[:80]
        body = statement.group(3).split("ON CONFLICT")[0]
        rows = re.findall(r"\(\s*'[^']*',", body)
        seeded = re.findall(r"'seed'\s*\)", body)
        assert rows and len(rows) == len(seeded), statement.group(0)[:80]
