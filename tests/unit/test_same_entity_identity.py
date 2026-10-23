# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2141 (identity across variants): a relationship marked same entity joins two tables one
class term grounds, one-to-one on a key of each -- the primary key or a declared unique
constraint -- and is refused by name otherwise. While one stands on a table, the table's class
term cannot be withdrawn or changed."""

# Requirements: REQ-2141

from __future__ import annotations

import types
from contextlib import asynccontextmanager
from typing import Any, cast

import pytest
from sqlalchemy import insert, select

from provisa.api.admin import glossary_router
from provisa.api.admin.schema_mutation import _same_entity_refusal
from provisa.api.errors import ApiError
from provisa.core.models import Cardinality, Column, Relationship, Table, UniqueConstraint
from provisa.core.repositories import glossary as glossary_repo
from provisa.core.repositories import identity
from provisa.core.repositories import relationship as relationship_repo
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import domains, registered_tables, sources

SCHEMA = "org_default"


def _tbl(name: str, *, unique: list[str] | None = None) -> Table:
    return Table(
        source_id="__derived__",
        domain_id="sales",
        schema_name="s",
        table_name=name,
        columns=[
            Column(name="id", data_type="integer", visible_to=[], is_primary_key=True),
            Column(name="ref", data_type="text", visible_to=[]),
            Column(name="note", data_type="text", visible_to=[]),
        ],
        unique_constraints=[UniqueConstraint(name=f"{name}_ref", columns=unique)] if unique else [],
        view_sql="SELECT 1",
    )


@asynccontextmanager
async def _surface(control_plane, monkeypatch):
    """customers and crm_customers grounded in ``patron`` (crm's ``ref`` a unique key),
    invoices grounded in ``bill``, and ledger grounded in nothing."""
    with control_plane.engine().begin() as c:
        c.exec_driver_sql(f'SET LOCAL search_path TO "{SCHEMA}"')
        c.execute(insert(domains).values(id="sales", description=""))
        c.execute(insert(sources).values(id="__derived__", type="derived"))
    db = control_plane.org()

    async def _pool():
        return db

    async def _notify(org_id: str, reason: str) -> None:
        return None

    monkeypatch.setattr(glossary_router, "_pool", _pool)
    monkeypatch.setattr(glossary_router, "_notify", _notify)
    monkeypatch.setattr(glossary_router, "_require_glossary_rw", lambda request: None)
    monkeypatch.setattr(glossary_router, "_authority", lambda request: None)
    monkeypatch.setattr(glossary_router, "_curate_authority", lambda request: None)
    monkeypatch.setattr(glossary_router, "_is_org_curator", lambda request: False)
    ids: dict[str, int] = {}
    async with db.acquire() as conn:
        for name, unique in (
            ("customers", None),
            ("crm_customers", ["ref"]),
            ("invoices", None),
            ("ledger", None),
        ):
            await table_repo.upsert(conn, _tbl(name, unique=unique))
            row = (
                await conn.execute_core(
                    select(registered_tables.c.id).where(registered_tables.c.table_name == name)
                )
            ).one()
            ids[name] = row.id
        for name in ("patron", "bill"):
            ids[name] = await glossary_repo.create_abstract_term(conn, name, domains={"sales"})
        for table, term in (
            ("customers", "patron"),
            ("crm_customers", "patron"),
            ("invoices", "bill"),
        ):
            await glossary_repo.ground_table(conn, ids[term], ids[table])
    yield db, ids


def _request() -> Any:
    return types.SimpleNamespace(
        state=types.SimpleNamespace(
            identity=types.SimpleNamespace(user_id="alice", roles=[], acting_roles=[]),
            active_org_id="acme",
        )
    )


async def _refusal(conn, ids, target: str, *, source_column="id", target_column="id", **over):
    args: dict[str, Any] = {
        "relationship": "same",
        "source_table_id": ids["customers"],
        "target_table_id": ids[target],
        "source_column": source_column,
        "target_column": target_column,
        "cardinality": "one-to-one",
        "junction": False,
    }
    return await identity.same_entity_refusal(conn, **{**args, **over})


@pytest.mark.asyncio
async def test_two_tables_one_term_grounds_joined_on_keys_are_the_same_entity(
    control_plane, monkeypatch
):
    async with _surface(control_plane, monkeypatch) as (db, ids), db.acquire() as conn:
        # The primary key of each, and a declared unique constraint on the other side.
        assert await _refusal(conn, ids, "crm_customers") is None
        assert await _refusal(conn, ids, "crm_customers", target_column="ref") is None


@pytest.mark.asyncio
async def test_each_rule_is_refused_by_name(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (db, ids), db.acquire() as conn:
        not_keys = await _refusal(conn, ids, "crm_customers", target_column="note")
        assert not_keys is not None
        assert not_keys.code == "schema.relationship_same_entity_not_keys"
        assert not_keys.params == {
            "relationship": "same",
            "table": "__derived__.s.crm_customers",
            "columns": "note",
        }
        for target in ("invoices", "ledger"):
            other = await _refusal(conn, ids, target)
            assert other is not None and other.code == (
                "schema.relationship_same_entity_not_grounded"
            )
            assert other.params["target"] == f"__derived__.s.{target}"
        many = await _refusal(conn, ids, "crm_customers", cardinality="many-to-one")
        assert many is not None and many.code == "schema.relationship_same_entity_cardinality"
        through = await _refusal(conn, ids, "crm_customers", junction=True)
        assert through is not None and through.code == "schema.relationship_same_entity_direct"


@pytest.mark.asyncio
async def test_the_relationship_save_refuses_it_by_name(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (db, ids), db.acquire() as conn:
        given = types.SimpleNamespace(
            id="same",
            source_column="id",
            target_column="note",
            cardinality="one-to-one",
            via_table="",
            target_function_name=None,
        )
        refused = await _same_entity_refusal(
            conn, cast(Any, given), {"id": ids["customers"]}, {"id": ids["crm_customers"]}
        )
        assert refused is not None and refused.success is False
        assert refused.code == "schema.relationship_same_entity_not_keys"
        given.target_column = "id"
        assert (
            await _same_entity_refusal(
                conn, cast(Any, given), {"id": ids["customers"]}, {"id": ids["crm_customers"]}
            )
            is None
        )


async def _mark(conn) -> None:
    await relationship_repo.upsert(
        conn,
        Relationship(
            id="customer_is_crm",
            source_table_id="customers",
            target_table_id="crm_customers",
            source_column="id",
            target_column="id",
            cardinality=Cardinality.one_to_one,
            same_entity=True,
        ),
    )


@pytest.mark.asyncio
async def test_the_term_a_same_entity_relationship_stands_on_cannot_change(
    control_plane, monkeypatch
):
    async with _surface(control_plane, monkeypatch) as (db, ids):
        async with db.acquire() as conn:
            await _mark(conn)
            assert await identity.stored_refusal(conn, "customer_is_crm") is None
        with pytest.raises(ApiError) as refused:
            await glossary_router.remove_term_table(_request(), ids["patron"], ids["customers"])
        assert refused.value.status_code == 409
        assert refused.value.code == "glossary.table_in_identity"
        assert refused.value.params == {
            "table": "__derived__.s.customers",
            "relationship": "customer_is_crm",
        }
        async with db.acquire() as conn:
            # A configuration naming another term is refused too; the same term changes nothing.
            with pytest.raises(identity.TableInIdentity):
                await glossary_repo.declare_class_term(conn, ids["crm_customers"], ids["bill"])
            await glossary_repo.declare_class_term(conn, ids["crm_customers"], ids["patron"])
            # A table no identity stands on is ungrounded as before.
            assert await glossary_repo.unground_table(conn, ids["bill"], ids["invoices"])


@pytest.mark.asyncio
async def test_a_configuration_is_held_to_the_rule_once_its_class_terms_are_declared(
    control_plane, monkeypatch
):
    from provisa.core.config_loader import _check_identities

    async with _surface(control_plane, monkeypatch) as (db, ids), db.acquire() as conn:
        bad = Relationship(
            id="customer_is_bill",
            source_table_id="customers",
            target_table_id="invoices",
            source_column="id",
            target_column="id",
            cardinality=Cardinality.one_to_one,
            same_entity=True,
        )
        await relationship_repo.upsert(conn, bad)
        await _mark(conn)
        good = (await relationship_repo.get(conn, "customer_is_crm"))["same_entity"]
        assert good is True
        with pytest.raises(ValueError, match="customer_is_bill.*not grounded in the same term"):
            await _check_identities(conn, cast(Any, types.SimpleNamespace(relationships=[bad])))
        await _check_identities(
            conn,
            cast(
                Any,
                types.SimpleNamespace(
                    relationships=[
                        Relationship(
                            id="customer_is_crm",
                            source_table_id="customers",
                            target_table_id="crm_customers",
                            source_column="id",
                            target_column="id",
                            cardinality=Cardinality.one_to_one,
                            same_entity=True,
                        )
                    ]
                ),
            ),
        )


@pytest.mark.asyncio
async def test_a_class_grounded_in_tables_not_all_joined_is_a_gap_until_they_are(
    control_plane, monkeypatch
):
    async with _surface(control_plane, monkeypatch) as (db, ids), db.acquire() as conn:
        # patron grounds customers and crm_customers, joined by nothing; bill grounds one table.
        assert await identity.identity_gap_ids(conn) == {ids["patron"]}
        listed = {t["name"]: t["identity_gap"] for t in await glossary_repo.list_terms(conn)}
        assert (listed["patron"], listed["bill"]) == (True, False)
        await _mark(conn)
        assert await identity.identity_gap_ids(conn) == set()
        assert (await glossary_repo.get_term(conn, ids["patron"]))["identity_gap"] is False
