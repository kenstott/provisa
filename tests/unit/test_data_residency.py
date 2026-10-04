# Copyright (c) 2026 Kenneth Stott
# Canary: f8bd8682-04e6-41cd-9621-5f782dd8ea8c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""data_residency (REQ-1921, "a region governs where data may be, a domain governs who owns it"):
setting, changing or removing where an object's data lives needs the right, for the values its
grant lists (the org's regions and "no region"). P -> Q needs both; a new object needs Q; a draft
object needs only Q (a claim). A refusal names the value not covered. The right exists only when
the platform declares regions."""

# Requirements: REQ-1921

from __future__ import annotations

from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from provisa.core.models import ProvisaConfig
from provisa.security.residency import (
    CREATED,
    NO_REGION,
    ResidencyRefused,
    require_change,
)

EU_US = {"eu", "us"}


@pytest.mark.parametrize(
    ("covered", "before", "after", "refused_value"),
    [
        # none -> X needs no_region and X
        ({NO_REGION, "eu"}, None, "eu", None),
        ({"eu"}, None, "eu", NO_REGION),
        ({NO_REGION}, None, "eu", "eu"),
        # X -> none needs X and no_region
        ({"eu", NO_REGION}, "eu", None, None),
        ({"eu"}, "eu", None, NO_REGION),
        ({NO_REGION}, "eu", None, "eu"),
        # X -> Y needs both
        (EU_US, "eu", "us", None),
        ({"us"}, "eu", "us", "eu"),
        ({"eu"}, "eu", "us", "us"),
        # a new object needs only its destination
        ({"eu"}, CREATED, "eu", None),
        ({"us"}, CREATED, "eu", "eu"),
        ({NO_REGION}, CREATED, None, None),
        ({"eu"}, CREATED, None, NO_REGION),
    ],
)
def test_a_change_needs_every_value_it_touches_covered(covered, before, after, refused_value):
    if refused_value is None:
        require_change(covered, "table orders", before, after)
        return
    with pytest.raises(ResidencyRefused) as refused:
        require_change(covered, "table orders", before, after)
    assert refused.value.code == "security.data_residency_refused"
    assert refused.value.params == {"object": "table orders", "value": refused_value}


def test_a_draft_object_is_claimed_with_its_destination_alone():
    require_change({"us"}, "table orders", "eu", "us", draft=True)
    with pytest.raises(ResidencyRefused) as refused:
        require_change({"eu"}, "table orders", "eu", "us", draft=True)
    assert refused.value.params["value"] == "us"


def test_without_the_right_any_change_is_refused_naming_it_and_no_change_needs_nothing():
    with pytest.raises(
        ResidencyRefused, match="no grant of the caller's cover no region"
    ) as refused:
        require_change(None, "source crm", None, "eu")
    assert refused.value.code == "security.data_residency_missing"
    assert refused.value.params == {"object": "source crm", "value": NO_REGION}
    assert "a data_residency grant listing no region allows it" in str(refused.value)
    require_change(None, "source crm", "eu", "eu")


def test_a_grant_is_the_union_of_the_values_of_the_roles_that_hold_the_right():
    from provisa.security.rights import residency_values_for_claims

    roles = {
        "eu_steward": {"capabilities": ["data_residency"], "residency_values": ["eu"]},
        "us_steward": {"capabilities": ["data_residency"], "residency_values": ["us", NO_REGION]},
        "analyst": {"capabilities": ["usage"], "residency_values": []},
    }
    assert residency_values_for_claims(["eu_steward", "us_steward"], roles) == {
        "eu",
        "us",
        NO_REGION,
    }
    assert residency_values_for_claims(["analyst"], roles) is None


# -- the grant at load ------------------------------------------------------------------------

_PLATFORM = {
    "regions": [
        {"id": "eu", "address": "https://eu.example.com"},
        {"id": "us", "address": "https://us.example.com"},
    ]
}


def _config(role: dict, *, regions: bool) -> dict:
    cfg: dict = {
        "sources": [],
        "domains": [{"id": "sales"}],
        "tables": [],
        "roles": [role],
    }
    if regions:
        stores, selected = [], []
        for rid in ("eu", "us"):
            stores += [
                {"id": f"{rid}-pg", "url": f"postgresql://{rid}/db"},
                {"id": f"{rid}-trino", "url": f"trino://{rid}:8080", "kind": "trino-byo"},
            ]
            selected.append(
                {
                    "id": rid,
                    "engine": f"{rid}-trino",
                    "replicas": f"{rid}-pg",
                    "views": f"{rid}-pg",
                    "cache": f"{rid}-pg",
                    "state": f"{rid}-pg",
                    "record": f"{rid}-pg",
                }
            )
        cfg.update(platform=_PLATFORM, stores=stores, regions=selected)
    return cfg


def _steward(values: list[str], caps: tuple[str, ...] = ("data_residency",)) -> dict:
    return {
        "id": "steward",
        "capabilities": list(caps),
        "domain_access": ["*"],
        "residency_values": values,
    }


def test_a_grant_naming_the_orgs_regions_and_no_region_loads():
    cfg = ProvisaConfig.model_validate(_config(_steward(["eu", NO_REGION]), regions=True))
    assert cfg.roles[0].residency_values == ["eu", NO_REGION]


@pytest.mark.parametrize(
    ("role", "regions", "said"),
    [
        (_steward([]), False, "exists only when the platform declares regions"),
        (_steward(["ap"]), True, "names ap, which the org does not select"),
        (_steward(["eu"], caps=()), True, "lists residency values but does not hold"),
    ],
)
def test_a_grant_that_does_not_hold_together_is_refused_at_load(role, regions, said):
    with pytest.raises(ValidationError, match=said):
        ProvisaConfig.model_validate(_config(role, regions=regions))


# -- the check on an admin change -------------------------------------------------------------


@pytest.fixture
def node():
    from provisa.core import process_region

    was = process_region._region

    def _bind(region: str | None) -> None:
        process_region.bind_launch(_PLATFORM if region else {}, requested=region)

    yield _bind
    process_region._region = was


def test_an_admin_change_is_judged_by_the_callers_grant(node, monkeypatch):
    from provisa.api.admin import capabilities
    from provisa.api.app import state

    node("eu")
    monkeypatch.setattr(
        state,
        "roles",
        {"eu_steward": {"capabilities": ["data_residency"], "residency_values": ["eu"]}},
        raising=False,
    )
    monkeypatch.setattr(capabilities, "_identity_from_info", lambda info: info.identity)
    steward = SimpleNamespace(identity=SimpleNamespace(user_id="u1", roles=["eu_steward"]))
    capabilities.require_residency_change(steward, "table orders", CREATED, "eu")
    with pytest.raises(ResidencyRefused, match="region 'us'"):
        capabilities.require_residency_change(steward, "table orders", "eu", "us")
    nobody = SimpleNamespace(identity=SimpleNamespace(user_id="u2", roles=[]))
    with pytest.raises(ResidencyRefused, match="no grant of the caller's cover region 'eu'"):
        capabilities.require_residency_change(nobody, "table orders", CREATED, "eu")


def test_with_no_platform_regions_nothing_is_judged(node, monkeypatch):
    from provisa.api.admin import capabilities

    node(None)
    monkeypatch.setattr(capabilities, "_identity_from_info", lambda info: info.identity)
    nobody = SimpleNamespace(identity=SimpleNamespace(user_id="u2", roles=[]))
    capabilities.require_residency_change(nobody, "table orders", CREATED, None)


async def test_a_role_saved_with_a_grant_is_held_to_the_same_rules(node, tmp_path):
    from provisa.core.database import Database, create_engine_from_url
    from provisa.core.models import Role
    from provisa.core.regions import OrgRegion, StoreConfig
    from provisa.core.repositories import region as region_repo
    from provisa.core.repositories import role as role_repo
    from provisa.core.schema_org import metadata

    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'model.db'}")
    with engine.begin() as raw:
        metadata.create_all(raw)
    db = Database(engine, "test")
    node("eu")
    async with db.acquire() as conn:
        for store in (
            StoreConfig(id="eu-pg", url="postgresql://eu/db"),
            StoreConfig(id="eu-trino", url="trino://eu:8080", kind="trino-byo"),
        ):
            await region_repo.upsert_store(conn, store, origin="admin")
        await region_repo.upsert_region(
            conn,
            OrgRegion(
                id="eu",
                engine="eu-trino",
                replicas="eu-pg",
                views="eu-pg",
                cache="eu-pg",
                state="eu-pg",
                record="eu-pg",
            ),
            origin="admin",
        )
        steward = Role(**_steward(["eu", NO_REGION]))
        await role_repo.upsert(conn, steward, org_id=None, origin="admin")
        assert (await role_repo.get(conn, "steward"))["residency_values"] == ["eu", NO_REGION]
        with pytest.raises(ValueError, match="names us, which the org does not select"):
            await role_repo.upsert(conn, Role(**_steward(["us"])), org_id=None, origin="admin")
    node(None)
    async with db.acquire() as conn:
        with pytest.raises(ValueError, match="exists only when the platform declares regions"):
            await role_repo.upsert(conn, Role(**_steward([])), org_id=None, origin="admin")


async def test_set_table_region_refuses_a_change_the_grant_does_not_cover(
    node, monkeypatch, tmp_path
):
    """The admin mutation answers the refusal by name and changes nothing."""
    from unittest.mock import AsyncMock, patch

    from sqlalchemy import select, update

    from provisa.api.admin import capabilities
    from provisa.api.admin.schema_mutation import Mutation
    from provisa.api.app import state
    from provisa.core.schema_org import registered_tables
    from tests.unit.test_landing_ttl_admin import _db, _Pool

    node("eu")
    monkeypatch.setattr(
        state,
        "roles",
        {"eu_steward": {"capabilities": ["data_residency"], "residency_values": ["eu"]}},
        raising=False,
    )
    monkeypatch.setattr(capabilities, "_identity_from_info", lambda info: info.identity)
    info = SimpleNamespace(identity=SimpleNamespace(user_id="u1", roles=["eu_steward"]))
    async with _db(tmp_path) as db:
        async with db.acquire() as conn:
            await conn.execute_core(update(registered_tables).values(region="eu"))
            (table_id,) = (await conn.execute_core(select(registered_tables.c.id))).one()
        with (
            patch(
                "provisa.api.admin.schema_mutation._get_pool",
                new=AsyncMock(return_value=_Pool(db)),
            ),
            patch("provisa.api.admin.schema_mutation.require_capability", return_value=None),
        ):
            result = await Mutation().set_table_region(info, table_id=table_id, region="us")
        assert result.success is False
        assert result.code == "security.data_residency_refused"
        assert result.params == {"object": "table orders", "value": "us"}
        async with db.acquire() as conn:
            assert (
                await conn.execute_core(select(registered_tables.c.region))
            ).scalar_one() == "eu"


async def test_set_table_region_runs_the_real_capability_gate(node, monkeypatch, tmp_path):
    """REQ-1921: setting a region needs data_residency IN ADDITION TO owning the object through its
    domain (the ``table_registration`` gate). A holder of data_residency ALONE is refused at the
    capability gate, before the residency check ever runs; adding table_registration carries the
    call through to the residency refusal that names the region. The gate is deliberately NOT
    patched out here — the point is that it runs first, so patching it would prove nothing."""
    from unittest.mock import AsyncMock, patch

    from sqlalchemy import select, update

    from provisa.api.admin import capabilities
    from provisa.api.admin.schema_mutation import Mutation
    from provisa.api.app import state
    from provisa.core.schema_org import registered_tables
    from tests.unit.test_landing_ttl_admin import _db, _Pool

    node("eu")
    monkeypatch.setattr(
        state,
        "roles",
        {
            # data_residency for eu, but does NOT own the object (no table_registration).
            "res_only": {
                "capabilities": ["data_residency"],
                "residency_values": ["eu"],
                "domain_access": ["*"],
            },
            # owns the object AND holds data_residency for eu.
            "res_owner": {
                "capabilities": ["data_residency", "table_registration"],
                "residency_values": ["eu"],
                "domain_access": ["*"],
            },
        },
        raising=False,
    )
    monkeypatch.setattr(capabilities, "_identity_from_info", lambda info: info.identity)
    async with _db(tmp_path) as db:
        async with db.acquire() as conn:
            await conn.execute_core(update(registered_tables).values(region="eu"))
            (table_id,) = (await conn.execute_core(select(registered_tables.c.id))).one()
        with patch(
            "provisa.api.admin.schema_mutation._get_pool",
            new=AsyncMock(return_value=_Pool(db)),
        ):
            # data_residency alone: refused at the capability gate, before the residency check.
            info_only = SimpleNamespace(identity=SimpleNamespace(user_id="u1", roles=["res_only"]))
            with pytest.raises(PermissionError, match="table_registration"):
                await Mutation().set_table_region(info_only, table_id=table_id, region="us")
            # owns the object too: carried through to the residency refusal naming the region.
            info_owner = SimpleNamespace(
                identity=SimpleNamespace(user_id="u2", roles=["res_owner"])
            )
            result = await Mutation().set_table_region(info_owner, table_id=table_id, region="us")
            assert result.success is False
            assert result.code == "security.data_residency_refused"
            assert result.params == {"object": "table orders", "value": "us"}
        async with db.acquire() as conn:
            assert (
                await conn.execute_core(select(registered_tables.c.region))
            ).scalar_one() == "eu"
