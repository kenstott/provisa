# Copyright (c) 2026 Kenneth Stott
# Canary: f555cbb5-02bb-4434-a96f-aba8daddd5b7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A view's input named by its source's catalog resolves in every org and environment.

A view is written once, in the model: the demo config's dim_pet reads
``"pet_store_sqlite"."pet_store"."pets"``. Outside the default org's prod the engine's catalog is
prefixed (``org_sandbox__pet_store_sqlite``), and the event loop's wiring, which resolves each
view's inputs against the model, refused the view there and wired nothing for the whole org."""

# Requirements: REQ-939, REQ-1266

from __future__ import annotations

from types import SimpleNamespace

from provisa.mv.models import MVDefinition
from provisa.mv.view_inputs import ModelIndex, resolve_view_inputs


def _state(catalog: str) -> SimpleNamespace:
    return SimpleNamespace(
        tables=[
            {
                "id": 7,
                "source_id": "pet-store-sqlite",
                "schema_name": "pet_store",
                "table_name": "pets",
            }
        ],
        source_catalogs={"pet-store-sqlite": catalog},
        view_context=SimpleNamespace(tables={}),
        mv_registry=SimpleNamespace(get_enabled=lambda: []),
    )


def _dim_pet() -> MVDefinition:
    return MVDefinition(
        id="view-dim_pet",
        source_tables=[],
        target_catalog="store",
        target_schema="views",
        sql='SELECT "id", "name" FROM "pet_store_sqlite"."pet_store"."pets"',
    )


def test_a_view_naming_its_sources_catalog_resolves_in_a_non_default_org():
    state = _state("org_sandbox__pet_store_sqlite")
    assert [r.table_id for r in resolve_view_inputs(_dim_pet(), state)] == [7]


def test_a_view_naming_its_sources_catalog_resolves_in_a_test_environment():
    state = _state("org_acme_env_test__pet_store_sqlite")
    assert [r.table_id for r in resolve_view_inputs(_dim_pet(), state)] == [7]


def test_the_engines_own_catalog_name_still_resolves():
    state = _state("org_sandbox__pet_store_sqlite")
    resolved = ModelIndex(state).resolve(
        "v", ("org_sandbox__pet_store_sqlite", "pet_store", "pets")
    )
    assert resolved.table_id == 7
