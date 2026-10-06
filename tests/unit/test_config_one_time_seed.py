# Copyright (c) 2026 Kenneth Stott
# Canary: ece0d501-c072-41fa-b8dc-90c86a2baa3d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A configuration is a one-time seed (REQ-1919, amended 2026-10-06).

A configuration file seeds the model store once — at a deployment's first start, into an empty
store — and from then on the store alone owns the model. A restart, redeploy or reload never
applies the file again, nothing an admin changed is overwritten by it, and the process's model
is read from the store, never from the file. An admin can apply a configuration again as an
explicit one-time seed: it adds and updates what the file declares through the model store,
guards and versions included, and removes nothing. The model as it stands can be written as a
configuration file, whole or in part; applied to an empty store it builds the same model.

This file supersedes the 2026-10-03 behaviour (every object records its origin; a load removes
the config-origin objects its file no longer declares): each scenario that asserted a removal or
an origin now asserts what the one-time seed does instead. The scenarios run on a SQLite control
plane.
"""

# Requirements: REQ-1919, REQ-1918, REQ-164

from __future__ import annotations

import contextlib
import re
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml
from sqlalchemy import insert, select

from provisa.core import config_loader, secrets_store
from provisa.core.config_loader import (
    apply_config,
    is_seeded,
    parse_config,
    parse_config_dict,
    seed_config,
    store_config,
)
from provisa.core.database import Database, create_engine_from_url
from provisa.core.db import _init_schema_portable
from provisa.core.models import Column, Domain, Role, Source, SourceType, Table
from provisa.core.repositories import domain as domain_repo
from provisa.core.repositories import role as role_repo
from provisa.core.repositories import source as source_repo
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import metadata
from provisa.core.store_config import (
    MODEL_SECTIONS,
    only_sections,
    store_model,
    with_store_model,
)

_REPO = Path(__file__).resolve().parents[2]


@pytest.fixture(autouse=True)
def no_vault(monkeypatch):
    """The scenarios' sources carry no secret reference, so the seed needs no org vault bound."""

    @contextlib.asynccontextmanager
    async def _unbound():
        yield

    monkeypatch.setattr(secrets_store, "bound_to_request_org", _unbound)


async def _plane(name: str) -> Database:
    plane = Database(create_engine_from_url("sqlite+pysqlite:///:memory:"), name=name)
    await _init_schema_portable(plane)
    await seed_the_view_source(plane)
    return plane


@pytest.fixture
async def db() -> Database:
    return await _plane("seed-test")


async def seed_the_view_source(plane: Database) -> None:
    """The built-in source views are registered on, which the startup seed creates before any
    config is applied."""
    async with plane.acquire() as conn:
        await conn.execute_core(
            insert(metadata.tables["sources"]).values(id="__derived__", type="postgresql")
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


def _raw(
    *,
    tables: list[dict] | None = None,
    roles: tuple[str, ...] = ("seller",),
    domains: tuple[str, ...] = ("sales",),
    sources: tuple[str, ...] = ("cfg",),
    host: str = "h",
    relationships: list[dict] | None = None,
    **more: Any,
) -> dict:
    """A config file, as written. ``more`` carries any other list a config declares (functions,
    metrics, rls_rules, tags, tag_assignments, glossary_terms, data_products, webhooks) and any
    setting (server, naming)."""
    return {
        "sources": [
            {"id": s, "type": "postgresql", "host": host, "port": 5432, "database": "d"}
            for s in sources
        ],
        "domains": [{"id": d} for d in domains],
        "roles": [{"id": r, "capabilities": [], "domain_access": ["sales"]} for r in roles],
        "tables": [_table("orders")] if tables is None else tables,
        "relationships": relationships or [],
        **more,
    }


def _file(**kwargs: Any):
    return parse_config_dict(_raw(**kwargs))


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


async def _boot(db: Database, raw: dict) -> tuple[bool, Any]:
    """What a launch does with the deployment's file (``app._load_and_build``): seed an empty
    store from it, then read the configuration the process runs from the store."""
    async with db.acquire() as conn:
        seeded = await seed_config(parse_config_dict(raw), conn)
        return seeded, await store_config(raw, conn)


async def _apply(db: Database, config) -> None:
    async with db.acquire() as conn:
        await apply_config(config, conn)


async def _model(db: Database) -> dict:
    async with db.acquire() as conn:
        return await store_model(conn)


async def _rows(db: Database, table: str, *columns: str) -> list[tuple]:
    tbl = metadata.tables[table]
    async with db.acquire() as conn:
        found = await conn.execute_core(select(*(tbl.c[c] for c in columns)))
        return sorted(tuple(r) for r in found.fetchall())


async def _ids(db: Database, table: str, key: str = "id") -> set:
    return {r[0] for r in await _rows(db, table, key)}


async def _admin_makes_its_own(db: Database) -> None:
    """A source, a domain, a role and two tables made through the admin — one of the tables on
    the source the config declares."""
    async with db.acquire() as conn:
        await source_repo.upsert(
            conn, Source(id="mine", type=SourceType.postgresql, host="h", port=5432, database="d")
        )
        await domain_repo.upsert(conn, Domain(id="lab"))
        await role_repo.upsert(
            conn, Role(id="tester", capabilities=[], domain_access=["lab"]), org_id="o"
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
            )


# --- the first start seeds an empty store ---------------------------------------------------------


async def test_the_first_start_seeds_an_empty_store(db):
    async with db.acquire() as conn:
        assert await is_seeded(conn) is False
    seeded, config = await _boot(db, _raw())
    assert seeded is True
    async with db.acquire() as conn:
        assert await is_seeded(conn) is True
    assert "cfg" in await _ids(db, "sources")
    assert "sales" in await _ids(db, "domains")
    assert "seller" in await _ids(db, "roles")
    assert await _ids(db, "registered_tables", "table_name") == {"orders"}
    # The process runs the store's model.
    assert [s.id for s in config.sources] == ["cfg"]
    assert [t.table_name for t in config.tables] == ["orders"]


async def test_a_second_boot_with_a_changed_config_file_changes_nothing(db):
    await _boot(db, _raw(roles=("seller", "auditor"), tables=[_table("orders")]))
    before = await _model(db)

    # The file changed: another host, a role and a table dropped, one added, a new source.
    changed = _raw(
        roles=("buyer",),
        sources=("cfg", "fresh"),
        host="elsewhere",
        tables=[_table("customers")],
    )
    seeded, config = await _boot(db, changed)

    assert seeded is False
    assert await _model(db) == before
    # The process's model is the store's, not the changed file's.
    assert {s.id for s in config.sources} == {"cfg"}
    assert config.sources[0].host == "h"
    assert {r.id for r in config.roles} >= {"seller", "auditor"}
    assert "buyer" not in {r.id for r in config.roles}
    assert [t.table_name for t in config.tables] == ["orders"]


async def test_a_file_only_source_the_store_does_not_hold_does_not_exist(db):
    from provisa.federation.registry_view import registered_sources

    await _boot(db, _raw())
    _seeded, config = await _boot(db, _raw(sources=("cfg", "only_in_file")))
    assert {s.id for s in config.sources} == {"cfg"}
    async with db.acquire() as conn:
        held = await registered_sources(SimpleNamespace(model_db=db, config=config), conn)
    assert {s.id for s in held} == {"cfg"}


async def test_an_admin_edit_survives_a_restart(db):
    await _boot(db, _raw(roles=("seller", "auditor")))
    async with db.acquire() as conn:
        # The admin edits a seeded role and a seeded table, deletes a seeded role, and makes its own.
        await role_repo.upsert(
            conn, Role(id="seller", capabilities=["usage"], domain_access=["*"]), org_id="o"
        )
        await role_repo.delete(conn, "auditor")
        await table_repo.upsert(
            conn,
            Table(
                source_id="cfg",
                domain_id="sales",
                schema_name="public",
                table_name="orders",
                description="edited by the admin",
                columns=[Column(name="id", data_type="integer", visible_to=["seller"])],
            ),
        )
    await _admin_makes_its_own(db)
    before = await _model(db)

    await _boot(db, _raw(roles=("seller", "auditor")))

    assert await _model(db) == before
    seller = next(r for r in (await _model(db))["roles"] if r["id"] == "seller")
    assert (seller["capabilities"], seller["domain_access"]) == (["usage"], ["*"])
    assert "auditor" not in await _ids(db, "roles")
    assert {"mine"} <= await _ids(db, "sources")
    assert {"lab"} <= await _ids(db, "domains")
    assert await _ids(db, "registered_tables", "table_name") == {"orders", "scratch", "extra"}


async def test_an_admin_edit_to_a_seeded_source_governs_after_a_reboot(db):
    """The maintainer's ruling: edit a seeded source's setting, reboot, and the edit governs —
    in the store, in the configuration the process runs, and in what the runtime registry reads."""
    from provisa.federation.registry_view import registered_sources

    await _boot(db, _raw(host="h"))
    async with db.acquire() as conn:
        held = await source_repo.get(conn, "cfg")
        await source_repo.upsert(
            conn,
            Source.model_validate(
                {
                    **source_repo.source_from_row(held).model_dump(),
                    "host": "edited-host",
                    "pool_max": 9,
                    "use_pgbouncer": True,
                }
            ),
        )

    _seeded, config = await _boot(db, _raw(host="h"))

    (cfg,) = config.sources
    assert (cfg.host, cfg.pool_max, cfg.use_pgbouncer) == ("edited-host", 9, True)
    async with db.acquire() as conn:
        (runtime,) = await registered_sources(SimpleNamespace(model_db=db, config=config), conn)
    assert (runtime.host, runtime.pool_max, runtime.use_pgbouncer) == ("edited-host", 9, True)


async def test_every_setting_a_file_gives_a_source_is_held_by_the_store(db):
    """REQ-1919: the store row is complete — what the file says about a source reaches the
    runtime through the store."""
    raw = _raw()
    raw["sources"][0].update(
        {
            "pool_min": 2,
            "pool_max": 7,
            "use_pgbouncer": True,
            "pgbouncer_port": 6543,
            "cache_schema": "cached",
            "cache_catalog": "cache_cat",
            "approval_hook": True,
            "allowed_domains": ["sales"],
            "gql_naming_convention": "snake_case",
        }
    )
    _seeded, config = await _boot(db, raw)
    (cfg,) = config.sources
    assert (cfg.pool_min, cfg.pool_max, cfg.use_pgbouncer, cfg.pgbouncer_port) == (2, 7, True, 6543)
    assert (cfg.cache_schema, cfg.cache_catalog, cfg.approval_hook) == ("cached", "cache_cat", True)
    assert (cfg.allowed_domains, cfg.gql_naming_convention) == (["sales"], "snake_case")


async def test_every_setting_a_file_gives_a_table_and_a_role_is_held_by_the_store(db):
    raw = _raw(
        tables=[
            _table(
                "orders",
                hot=True,
                relay_pagination=True,
                approval_hook=True,
                promotions=[{"source_path": "a.b", "target_column": "b", "data_type": "text"}],
                columns=[
                    {
                        "name": "id",
                        "data_type": "integer",
                        "visible_to": ["seller"],
                        "encrypted": True,
                    }
                ],
            )
        ]
    )
    raw["roles"][0]["max_rows"] = 50
    raw["roles"][0]["relationship_guard"] = False
    _seeded, config = await _boot(db, raw)
    (orders,) = config.tables
    assert (orders.hot, orders.relay_pagination, orders.approval_hook) == (True, True, True)
    assert orders.promotions == raw["tables"][0]["promotions"]
    assert orders.columns[0].encrypted is True
    seller = next(r for r in config.roles if r.id == "seller")
    assert (seller.max_rows, seller.relationship_guard) == (50, False)


async def test_two_nodes_seed_the_store_once(db, monkeypatch):
    """A second node of a cluster (``PROVISA_ROLE=secondary``) or a later launch finds the
    store seeded; whose node it is does not decide, the store does."""
    assert (await _boot(db, _raw()))[0] is True
    monkeypatch.setenv("PROVISA_ROLE", "secondary")
    assert (await _boot(db, _raw(roles=("other",))))[0] is False
    assert "other" not in await _ids(db, "roles")


async def test_the_install_configuration_seeds_once_and_admin_made_objects_survive_a_reboot(db):
    """The boot's own seed: the configuration the installer ships, then a second launch over a
    control plane that also holds objects made through the admin."""
    raw = config_loader.read_config_with_includes(_REPO / "config" / "provisa-install.yaml")
    await _boot(db, raw)
    declared = parse_config(_REPO / "config" / "provisa-install.yaml").sources[0].id
    async with db.acquire() as conn:
        await source_repo.upsert(
            conn, Source(id="mine", type=SourceType.postgresql, host="h", port=5432, database="d")
        )
        await domain_repo.upsert(conn, Domain(id="lab"))
        await role_repo.upsert(
            conn, Role(id="tester", capabilities=[], domain_access=["lab"]), org_id="o"
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
            )
    before = await _model(db)

    seeded, _config = await _boot(db, raw)

    assert seeded is False
    assert await _model(db) == before


# --- an explicit apply adds and updates, and removes nothing --------------------------------------


async def test_an_apply_adds_and_updates_what_the_file_declares(db):
    await _boot(db, _raw())
    await _apply(
        db,
        _file(
            roles=("seller", "auditor"),
            tables=[_table("orders", description="now described"), _table("customers")],
        ),
    )
    assert {"seller", "auditor"} <= await _ids(db, "roles")
    assert await _rows(db, "registered_tables", "table_name", "description") == [
        ("customers", None),
        ("orders", "now described"),
    ]


async def test_an_apply_removes_nothing_of_any_kind(db):
    """What an earlier seed declared and a later file does not mention stays: a role, a domain,
    a source with its table, a view, a relationship, a metric, a command, a webhook, a data
    product, a tag and its assignment, a row filter, a glossary term and the admin's objects."""
    full = _file(
        roles=("seller", "auditor"),
        domains=("sales", "analytics"),
        sources=("cfg", "old"),
        tables=[
            _table("orders"),
            _table("customers", domain_id="analytics"),
            _table("legacy", source_id="old"),
            _view("big_orders", "SELECT id FROM orders"),
        ],
        relationships=[_ORDERS_TO_CUSTOMERS],
        functions=[_command("enrich_orders", "analytics")],
        metrics=[_metric("order_count")],
        webhooks=[{"name": "notify", "url": "http://x", "domain_id": "sales"}],
        data_products=[{"id": "core", "domain_id": "sales", "name": "Core"}],
        tags=[{"id": "finance"}],
        tag_assignments=[
            {"tag_id": "pii", "object_type": "table", "table_ref": "cfg.public.orders"}
        ],
        rls_rules=[{"table_id": "orders", "role_id": "seller", "filter": "region = 'eu'"}],
        glossary_terms=[
            {"name": "Bookings", "domains": ["sales"], "edges": [{"to": "ID"}]},
            {"name": "id", "domains": ["sales"], "definition": "the row's key"},
        ],
    )
    await _apply(db, full)
    await _admin_makes_its_own(db)
    before = await _model(db)

    await _apply(db, _file(tables=[_table("orders")]))

    assert await _model(db) == before


async def test_an_apply_keeps_the_columns_the_file_no_longer_lists(db):
    """An apply removes nothing, a column included: one a later file no longer lists stays as it
    is stored, with the relationship keyed on it."""
    with_ref = {
        "columns": [
            {"name": "id", "data_type": "integer", "visible_to": ["seller"]},
            {"name": "customer_id", "data_type": "integer", "visible_to": ["seller"]},
        ]
    }
    keyed = {**_ORDERS_TO_CUSTOMERS, "source_column": "customer_id"}
    await _apply(
        db, _file(tables=[_table("orders", **with_ref), _table("customers")], relationships=[keyed])
    )
    await _apply(db, _file(tables=[_table("orders"), _table("customers")], relationships=[keyed]))
    assert ("customer_id",) in await _rows(db, "table_columns", "column_name")
    assert await _rows(db, "relationships", "source_column") == [("customer_id",)]


async def test_an_apply_goes_through_the_guards(db):
    """A view the file declares that would read itself is refused by the model store's guard
    (REQ-1918), and the whole apply is undone."""
    await _apply(db, _file())
    before = await _model(db)
    looping = [
        _table("orders"),
        _view("loop_a", "SELECT id FROM loop_b"),
        _view("loop_b", "SELECT id FROM loop_a"),
    ]
    with pytest.raises(table_repo.ViewLoopRefused):
        await _apply(db, _file(roles=("seller", "new_role"), tables=looping))
    assert await _model(db) == before


async def test_an_apply_refuses_a_reserved_role(db):
    raw = _raw()
    raw["roles"].append({"id": "org_admin", "capabilities": [], "domain_access": ["*"]})
    with pytest.raises(role_repo.ReservedRoleRedefined):
        await _apply(db, parse_config_dict(raw))


async def test_an_apply_advances_versions(db):
    both = [_table("orders"), _table("customers")]
    await _apply(db, _file(tables=both, relationships=[_ORDERS_TO_CUSTOMERS]))
    ((first,),) = await _rows(db, "relationships", "version")
    await _apply(db, _file(tables=both, relationships=[{**_ORDERS_TO_CUSTOMERS, "alias": "BUYS"}]))
    ((second,),) = await _rows(db, "relationships", "version")
    assert second == first + 1


async def test_an_apply_marks_the_store_seeded(db):
    await _apply(db, _file())
    seeded, _config = await _boot(db, _raw(roles=("other",)))
    assert seeded is False
    assert "other" not in await _ids(db, "roles")


async def test_a_seeded_role_a_file_redefined_keeps_that_definition(db):
    """No seed reverts: a later file that no longer declares a seeded role it once redefined
    leaves the role as it stands."""
    raw = _raw(domains=("sales", "analytics"))
    raw["roles"].append(
        {"id": "analyst", "capabilities": ["usage", "write"], "domain_access": ["analytics"]}
    )
    await _apply(db, parse_config_dict(raw))
    await _apply(db, _file())
    async with db.acquire() as conn:
        analyst = await role_repo.get(conn, "analyst")
    assert analyst is not None
    assert (sorted(analyst["capabilities"]), analyst["domain_access"]) == (
        ["usage", "write"],
        ["analytics"],
    )


async def test_an_apply_that_would_publish_a_name_twice_is_refused_naming_both(db):
    """An apply removes nothing, so a table the file moved to another schema would stand beside
    the one registered under the same name in the same domain: refused, naming both, and the whole
    apply is undone."""
    both = [_table("orders"), _table("customers")]
    await _apply(db, _file(tables=both, relationships=[_ORDERS_TO_CUSTOMERS]))
    before = await _model(db)
    moved = [_table("orders", schema="sales"), _table("customers")]
    with pytest.raises(config_loader.PublishedNameClash) as err:
        await _apply(db, _file(roles=("seller", "new_role"), tables=moved))
    assert "sales.orders" in str(err.value)
    assert "cfg.public.orders" in str(err.value) and "cfg.sales.orders" in str(err.value)
    assert await _model(db) == before


async def test_a_moved_table_with_a_name_of_its_own_is_added_and_the_old_one_stays(db):
    both = [_table("orders"), _table("customers")]
    await _apply(db, _file(tables=both, relationships=[_ORDERS_TO_CUSTOMERS]))
    moved = {**_table("orders", schema="sales"), "alias": "sales_orders"}
    await _apply(db, _file(tables=[_table("customers"), moved]))
    assert await _rows(db, "registered_tables", "table_name", "schema_name") == [
        ("customers", "public"),
        ("orders", "public"),
        ("orders", "sales"),
    ]
    assert await _rows(db, "relationships", "id") == [("orders-to-customers",)]


async def test_an_apply_s_naming_rules_are_added_and_none_removed(db):
    await _apply(db, _file(naming={"rules": [{"pattern": "^tbl_", "replace": ""}]}))
    await _apply(db, _file(naming={"rules": [{"pattern": "^vw_", "replace": "v_"}]}))
    await _apply(db, _file(naming={"rules": [{"pattern": "^vw_", "replace": "view_"}]}))
    assert await _rows(db, "naming_rules", "pattern", "replacement") == [
        ("^tbl_", ""),
        ("^vw_", "view_"),
    ]


# --- the model written as a configuration ---------------------------------------------------------


def _everything() -> dict:
    raw = _raw(
        roles=("seller", "auditor"),
        domains=("sales", "analytics"),
        sources=("cfg", "old"),
        tables=[
            _table("orders", description="all orders", hot=True),
            _table("customers", domain_id="analytics", alias="clients"),
            _table("legacy", source_id="old"),
            _view("big_orders", "SELECT id FROM orders"),
        ],
        relationships=[{**_ORDERS_TO_CUSTOMERS, "target_table_id": "clients"}],
        functions=[_command("enrich_orders", "analytics")],
        metrics=[_metric("order_count")],
        webhooks=[{"name": "notify", "url": "http://x", "domain_id": "sales"}],
        data_products=[{"id": "core", "domain_id": "sales", "name": "Core"}],
        tags=[{"id": "finance", "description": "money"}],
        tag_assignments=[
            {"tag_id": "pii", "object_type": "table", "table_ref": "cfg.public.orders"}
        ],
        rls_rules=[{"table_id": "orders", "role_id": "seller", "filter": "region = 'eu'"}],
        glossary_terms=[
            {
                "name": "Bookings",
                "domains": ["sales"],
                "definition": "sold",
                "edges": [{"to": "ID"}],
            },
            {"name": "id", "domains": ["sales"], "definition": "the row's key"},
        ],
        naming={"rules": [{"pattern": "^tbl_", "replace": ""}]},
        scheduled_triggers=[{"id": "nightly", "cron": "0 2 * * *", "url": "http://hook"}],
        server={"port": 8123},
    )
    raw["sources"][0]["pool_max"] = 8
    raw["roles"][1]["max_rows"] = 10
    return raw


async def test_the_export_round_trips(db):
    """Export the model → apply it to an empty store → the same model."""
    raw = _everything()
    await _boot(db, raw)
    await _admin_makes_its_own(db)
    exported = with_store_model(raw, await _model(db))
    # Through the file format: what an admin downloads and uploads again.
    text = yaml.safe_dump(exported, sort_keys=False)

    fresh = await _plane("round-trip")
    async with fresh.acquire() as conn:
        await apply_config(parse_config_dict(yaml.safe_load(text)), conn)

    assert await _model(fresh) == await _model(db)


async def test_the_export_writes_every_model_section_and_keeps_the_file_s_settings(db):
    raw = _everything()
    await _boot(db, raw)
    exported = with_store_model(raw, await _model(db))
    assert set(MODEL_SECTIONS) <= set(exported)
    assert exported["server"] == {"port": 8123}
    assert exported["naming"]["rules"] == [{"pattern": "^tbl_", "replace": ""}]
    # What the deployment seeds into every store is not part of it.
    assert "__derived__" not in {s["id"] for s in exported["sources"]}
    assert not {"org_admin", "platform_admin"} & {r["id"] for r in exported["roles"]}
    assert not {"meta", "ops", ""} & {d["id"] for d in exported["domains"]}
    # An exported configuration carries no origin.
    assert "origin" not in yaml.safe_dump(exported)


async def test_a_chosen_part_of_the_model_is_exported_and_applies_alone(db):
    raw = _everything()
    await _boot(db, raw)
    exported = with_store_model(raw, await _model(db))
    part = only_sections(exported, ["sources", "domains"])
    assert set(part) == {"sources", "domains", "tables", "roles"}
    assert part["tables"] == [] and part["roles"] == []

    fresh = await _plane("part")
    async with fresh.acquire() as conn:
        await apply_config(parse_config_dict(part), conn)
    assert (await _model(fresh))["sources"] == (await _model(db))["sources"]
    assert (await _model(fresh))["tables"] == []

    with pytest.raises(ValueError, match="not a model section"):
        only_sections(exported, ["server"])


# --- what is gone ---------------------------------------------------------------------------------


def test_nothing_removes_what_a_file_no_longer_declares():
    """The removal at the end of a load, the seed reverts, the take-over and the origin are gone
    (superseded 2026-10-06)."""
    source = Path(config_loader.__file__).read_text()
    for name in (
        "ConfigDropRefused",
        "_remove_what_the_config_dropped",
        "_revert_seeded",
        "_rekey_moved_tables",
        "is_primary_worker",
        "origin",
    ):
        assert name not in source, name
    assert not (_REPO / "provisa" / "core" / "repositories" / "origin.py").exists()
    assert not (_REPO / "provisa" / "core" / "repositories" / "seed_definitions.py").exists()
    assert "seed_redefinitions" not in metadata.tables
    assert not hasattr(table_repo, "deferring_column_drops")
    assert not hasattr(table_repo, "rekey")


def test_the_schema_seeds_its_rows_without_an_origin():
    """The PostgreSQL schema seeds roles and domains by INSERT; no statement names an origin,
    and no model table has the column."""
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
        assert "origin" not in columns, statement.group(0)[:80]
    assert not re.search(r"^\s*origin\s+TEXT", sql, re.M)
    assert "CREATE TABLE IF NOT EXISTS model_seed" in sql


# --- a demo organisation is its config ------------------------------------------------------------


async def test_a_demo_rebuild_starts_exactly_as_its_config_says(db):
    """DEMO ORGANISATIONS ARE THEIR CONFIG: every build rebuilds a demo's model from its
    configuration — an admin's additions and edits last until then, and the seed stays."""
    from provisa.core.config_loader import rebuild_from_config

    demo = _everything()
    fresh = await _plane("demo-fresh")
    async with fresh.acquire() as conn:
        await rebuild_from_config(parse_config_dict(_everything()), conn)
    expected = await _model(fresh)

    async with db.acquire() as conn:
        await rebuild_from_config(parse_config_dict(demo), conn)
    await _admin_makes_its_own(db)
    async with db.acquire() as conn:
        await role_repo.upsert(
            conn, Role(id="seller", capabilities=["usage"], domain_access=["*"]), org_id="o"
        )
        await conn.execute_core(
            insert(metadata.tables["user_role_assignments"]).values(
                user_id="ada", role_id="analyst", domain_id="*"
            )
        )
    assert await _model(db) != expected

    async with db.acquire() as conn:
        await rebuild_from_config(parse_config_dict(_everything()), conn)

    assert await _model(db) == expected
    # What the deployment seeds, and who holds a seeded role, stays.
    assert {"analyst", "org_admin"} <= await _ids(db, "roles")
    assert ("ada", "analyst") in await _rows(db, "user_role_assignments", "user_id", "role_id")
    assert "__derived__" in await _ids(db, "sources")


# --- Kafka sources are model ----------------------------------------------------------------------


def _kafka() -> dict:
    return {
        "id": "events",
        "bootstrap_servers": "kafka:9092",
        "topics": [
            {
                "id": "clicks",
                "topic": "web.clicks",
                "domain_id": "sales",
                "default_window": "15m",
                "columns": [{"name": "id", "data_type": "integer", "visible_to": ["seller"]}],
            }
        ],
    }


async def test_kafka_sources_are_seeded_once_and_read_from_the_store(db):
    raw = _raw(kafka_sources=[_kafka()])
    seeded, config = await _boot(db, raw)
    assert seeded is True
    assert [k["id"] for k in config.kafka_sources] == ["events"]
    assert ("events", "clicks") in await _rows(db, "registered_tables", "source_id", "table_name")
    assert await _rows(db, "kafka_topics", "source_id", "topic") == [("events", "web.clicks")]

    changed = _kafka()
    changed["topics"][0]["default_window"] = "1h"
    changed["topics"].append(
        {
            "id": "views",
            "topic": "web.views",
            "domain_id": "sales",
            "columns": [{"name": "id", "data_type": "integer", "visible_to": ["seller"]}],
        }
    )
    _seeded, config = await _boot(db, _raw(kafka_sources=[changed]))
    assert config.kafka_sources == [_kafka()]
    assert ("events", "views") not in await _rows(
        db, "registered_tables", "source_id", "table_name"
    )


async def test_kafka_sources_round_trip_through_the_export(db):
    raw = _raw(kafka_sources=[_kafka()])
    await _boot(db, raw)
    exported = with_store_model(raw, await _model(db))
    fresh = await _plane("kafka-round-trip")
    async with fresh.acquire() as conn:
        await apply_config(parse_config_dict(yaml.safe_load(yaml.safe_dump(exported))), conn)
    assert await _model(fresh) == await _model(db)
