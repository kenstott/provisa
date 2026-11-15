# Copyright (c) 2026 Kenneth Stott
# Canary: 2d8e9884-3f7d-42a3-81c2-8c23108621e8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2180 (amended 2026-11-11, OFFERS; CONTRACTS ARE DERIVED FROM THE OFFERS): a configuration
declares a data product's offers on the product, as it declares its members. Loading it adds each
offer, or replaces the product's offer of its name; an apply removes nothing, so an offer the file
does not declare is kept, the default every product starts with among them. A sample names its
views as the product names its tables. The model written back out as a configuration declares the
offers the store holds, and no contract: those are derived."""

# Requirements: REQ-2180, REQ-1919

from __future__ import annotations

import contextlib
from typing import Any

import pytest
from sqlalchemy import insert

# The API layer says at import whether the deployment holds many organizations, which a secret's
# read asks; run alone, nothing else here imports it.
import provisa.api.app  # noqa: F401
from provisa.core import secrets_store
from provisa.core.config_loader import apply_config, parse_config_dict
from provisa.core.database import Database
from provisa.core.repositories import data_product_offer as offers
from provisa.core.schema_org import metadata
from provisa.core.store_config import store_model


@pytest.fixture(autouse=True)
def no_vault(monkeypatch):
    """The source carries no secret reference, so the load needs no org vault bound."""

    @contextlib.asynccontextmanager
    async def _unbound():
        yield

    monkeypatch.setattr(secrets_store, "bound_to_request_org", _unbound)


@pytest.fixture
async def db(control_plane) -> Database:
    plane = control_plane.org()
    async with plane.acquire() as conn:
        # The built-in source views are registered on, which the startup seed creates.
        await conn.execute_core(
            insert(metadata.tables["sources"]).values(id="__derived__", type="postgresql")
        )
    return plane


def _config(*offers_: dict[str, Any]) -> Any:
    product: dict[str, Any] = {
        "id": "core",
        "domain_id": "sales",
        "name": "Core",
        "tables": ["orders"],
    }
    if offers_:
        product["offers"] = list(offers_)
    return parse_config_dict(
        {
            "sources": [
                {"id": "cfg", "type": "postgresql", "host": "h", "port": 5432, "database": "d"}
            ],
            "domains": [{"id": "sales"}],
            "roles": [
                {
                    "id": "seller",
                    "description": "Sells.",
                    "capabilities": [],
                    "domain_access": ["sales"],
                }
            ],
            "tables": [
                {
                    "source_id": "cfg",
                    "domain_id": "sales",
                    "schema": "public",
                    "table": "orders",
                    "columns": [{"name": "id", "data_type": "integer", "visible_to": ["seller"]}],
                }
            ],
            "relationships": [],
            "data_products": [product],
        }
    )


async def _apply(db: Database, config: Any) -> None:
    async with db.acquire() as conn:
        await apply_config(config, conn)


async def _offers(db: Database) -> dict[str, dict]:
    async with db.acquire() as conn:
        return {o["name"]: o for o in await offers.list_offers(conn, "core")}


_SELLERS = {
    "name": "sellers",
    "role_id": "seller",
    "dq_results": False,
    "duration_days": 30,
}


async def test_a_declared_offer_is_loaded_beside_the_default(db):
    await _apply(db, _config(_SELLERS))
    held = await _offers(db)
    assert sorted(held) == ["default", "sellers"]
    sellers = held["sellers"]
    assert (sellers["role_id"], sellers["dq_results"], sellers["duration_days"]) == (
        "seller",
        False,
        30,
    )
    assert (sellers["data_level"], sellers["open"], sellers["profiles"]) == ("full", False, False)


async def test_a_product_declaring_no_offer_keeps_its_default(db):
    await _apply(db, _config())
    assert list(await _offers(db)) == ["default"]


async def test_an_offer_declared_again_replaces_the_one_of_its_name_and_others_are_kept(db):
    await _apply(db, _config(_SELLERS))
    first = (await _offers(db))["sellers"]
    await _apply(db, _config({**_SELLERS, "duration_days": 90}))
    held = await _offers(db)
    assert sorted(held) == ["default", "sellers"]
    assert held["sellers"]["id"] == first["id"]
    assert (held["sellers"]["duration_days"], held["sellers"]["version"]) == (90, 2)
    # An apply removes nothing: the file no longer declaring it keeps it.
    await _apply(db, _config())
    assert sorted(await _offers(db)) == ["default", "sellers"]


async def test_the_default_offer_is_declared_by_its_name(db):
    await _apply(db, _config({"name": "default", "role_id": "seller", "profiles": True}))
    (default,) = (await _offers(db)).values()
    assert (default["name"], default["role_id"], default["profiles"]) == ("default", "seller", True)


async def test_a_sample_naming_no_registered_view_fails_the_load_by_name(db):
    sample = {
        "name": "taste",
        "role_id": "seller",
        "data_level": "sample",
        "sample_views": ["orders_sample"],
    }
    with pytest.raises(ValueError, match="offer 'taste' names view 'orders_sample'"):
        await _apply(db, _config(sample))


async def test_an_offer_that_does_not_fit_is_refused_when_the_file_is_read():
    with pytest.raises(ValueError, match="a synthetic offer has no profiles"):
        _config({"name": "fake", "role_id": "seller", "data_level": "synthetic", "profiles": True})


async def test_the_model_written_out_declares_its_offers_and_no_contract(db):
    await _apply(db, _config(_SELLERS))
    async with db.acquire() as conn:
        model = await store_model(conn)
    (product,) = [p for p in model["data_products"] if p["id"] == "core"]
    assert product["offers"] == [
        {"name": "default", "role_id": "viewer"},
        {"name": "sellers", "role_id": "seller", "dq_results": False, "duration_days": 30},
    ]
    assert all("contract" not in t for t in model["tables"])
    # Written out and read back, it declares the same offers.
    from provisa.core.models import DataProduct

    assert [o.name for o in DataProduct.model_validate(product).offers] == ["default", "sellers"]
