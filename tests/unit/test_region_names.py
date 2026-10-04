# Copyright (c) 2026 Kenneth Stott
# Canary: d3c2cbf3-335f-4431-8b24-72535ddb6864
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every object a region keeps carries the region in its name, so regions may share a database
instance and a Redis (REQ-1922, "regions may share a database instance"). The model, every
region's, keeps its name; with no platform regions every name is as before."""

# Requirements: REQ-1922

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.core.environments import active_org_schema, org_schema
from provisa.federation.replica_address import replica_address

_PLATFORM = {
    "regions": [
        {"id": "eu", "address": "https://eu.example.com"},
        {"id": "us", "address": "https://us.example.com"},
    ]
}


@pytest.fixture
def node():
    from provisa.core import process_region

    was = process_region._region

    def _bind(region: str | None) -> None:
        process_region.bind_launch(_PLATFORM if region else {}, requested=region)

    yield _bind
    process_region._region = was


def test_a_regions_schemas_carry_its_name_and_the_model_does_not():
    assert org_schema("acme") == "org_acme"
    assert org_schema("acme", region="eu") == "org_acme_rg_eu"
    assert org_schema("acme", "dev", "_replicas", region="eu") == "org_acme_env_dev_rg_eu_replicas"
    assert org_schema("acme", "dev", "_replicas") == "org_acme_env_dev_replicas"


def test_with_no_platform_regions_every_name_is_as_before(node):
    node(None)
    assert active_org_schema("acme", "_replicas") == "org_acme_replicas"
    assert active_org_schema("acme", "_mv_cache") == "org_acme_mv_cache"
    assert active_org_schema("acme") == "org_acme"


def test_a_nodes_derived_stores_are_its_regions_and_another_regions_are_named_for_it(node):
    node("eu")
    assert active_org_schema("acme") == "org_acme"  # the model: every region's
    assert active_org_schema("acme", "_replicas") == "org_acme_rg_eu_replicas"
    assert active_org_schema("acme", "_mv_cache") == "org_acme_rg_eu_mv_cache"
    assert active_org_schema("acme", "_gql_cache") == "org_acme_rg_eu_gql_cache"
    here = replica_address(org_id="acme", source_id="crm", schema_name="public", table_name="o")
    there = replica_address(
        org_id="acme", source_id="crm", schema_name="public", table_name="o", region="us"
    )
    assert (here.schema, there.schema) == ("org_acme_rg_eu_replicas", "org_acme_rg_us_replicas")
    assert here.table == there.table


def test_each_regions_cache_and_counts_are_keyed_apart_in_one_redis(node):
    from provisa.cache.tenancy import cache_place
    from provisa.core.request_context import current_env, current_org
    from provisa.federation.replica_hot import count_scope

    state = SimpleNamespace(org_id="acme")
    org, env = current_org.set("acme"), current_env.set(None)
    try:
        node("eu")
        eu = (cache_place(state), count_scope("acme", "prod"))
        node("us")
        us = (cache_place(state), count_scope("acme", "prod"))
        node(None)
        none = (cache_place(state), count_scope("acme", "prod"))
    finally:
        current_org.reset(org)
        current_env.reset(env)
    assert eu[0] != us[0] and eu[1] != us[1]
    assert eu[0].endswith("_rg_eu") and us[1].endswith("_rg_us")
    assert none[1] == "acme:prod"
