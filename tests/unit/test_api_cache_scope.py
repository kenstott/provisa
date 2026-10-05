# Copyright (c) 2026 Kenneth Stott
# Canary: 31fa592b-d5ed-4d03-9f82-1f91c2fb4a35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An API cache table belongs to one org, one environment and one model (REQ-318, REQ-595, REQ-1914).

The rows an API source returned are kept in a cache table named from the call: source id,
operation and arguments. Source ids are an org's own, and a branch may bind the same source id to
another host, so that name alone let one org or environment read rows fetched for another; and
it did not change when the endpoint's definition did (its path, response root, columns), so the
old definition's rows were served until they expired.

The name now also depends on the acting org and environment and on the model the runtime loaded.
An endpoint's definition is part of the model, so changing it changes the stamp and the name.
"""

# Requirements: REQ-318, REQ-595, REQ-1914, REQ-1529

from __future__ import annotations

import pytest

from provisa.api_source import engine_cache
from provisa.api_source.engine_cache import cache_table_name

CALL = ("crm", "listCustomers", {"page": 1})


@pytest.fixture
def scope(monkeypatch):
    acting = {"scope": "acme:m7"}
    monkeypatch.setattr(engine_cache, "_scope", lambda: acting["scope"])
    return acting


def test_the_same_call_in_the_same_org_environment_and_model_is_the_same_table(scope):
    assert cache_table_name(*CALL) == cache_table_name(*CALL)
    assert cache_table_name(*CALL).startswith("r_")


@pytest.mark.parametrize(
    "other",
    ["globex:m7", "acme_env_staging:m7", "acme:m8"],
    ids=["another-org", "a-branch", "a-changed-model"],
)
def test_the_same_call_elsewhere_is_another_table(scope, other):
    here = cache_table_name(*CALL)
    scope["scope"] = other
    assert cache_table_name(*CALL) != here


def test_the_call_still_decides_the_table(scope):
    names = {
        cache_table_name("crm", "listCustomers", {"page": 1}),
        cache_table_name("crm", "listCustomers", {"page": 2}),
        cache_table_name("crm", "listOrders", {"page": 1}),
        cache_table_name("erp", "listCustomers", {"page": 1}),
    }
    assert len(names) == 4
    assert cache_table_name("crm", "x", {"a": 1, "b": 2}) == cache_table_name(
        "crm", "x", {"b": 2, "a": 1}
    )


def test_the_scope_is_the_one_every_cache_uses():
    import provisa.api.app as appmod
    from provisa.cache.tenancy import cache_tenant
    from provisa.core.request_context import current_org

    token = current_org.set(appmod.state.org_id)  # the deployment's own org, bound (REQ-1266)
    try:
        assert engine_cache._scope() == cache_tenant(appmod.state)
    finally:
        current_org.reset(token)


# --- the schema the tables are written to --------------------------------------------------------


def test_two_orgs_api_cache_tables_land_in_two_schemas():
    """The org a request acts in is bound in the context -- the deployment's own org (``root``)
    included (REQ-1266). The cache schema follows the acting org and its environment."""
    from types import SimpleNamespace

    from provisa.api_source.engine_cache import org_cache_schema
    from provisa.core.request_context import current_env, current_org

    state = SimpleNamespace(org_id="root")
    own = current_org.set("root")
    try:
        assert org_cache_schema(state) == "org_root_api_cache"
    finally:
        current_org.reset(own)

    org = current_org.set("acme")
    try:
        assert org_cache_schema(state) == "org_acme_api_cache"
        env = current_env.set("staging")
        try:
            assert org_cache_schema(state) == "org_acme_env_staging_api_cache"
        finally:
            current_env.reset(env)
    finally:
        current_org.reset(org)

    org = current_org.set("globex")
    try:
        assert org_cache_schema(state) == "org_globex_api_cache"
    finally:
        current_org.reset(org)


def test_every_cache_writer_asks_for_the_acting_orgs_schema():
    """No module builds a cache schema's name from the deployment's org any more."""
    import inspect

    from provisa.api.data import materialization

    # The pipeline's API stage is the one writer of API caches; Cypher over HTTP writes none of
    # its own (it reads through the pipeline).
    for module in (materialization,):
        source = inspect.getsource(module)
        assert 'f"org_' not in source, f"{module.__name__} names a cache schema from an org id"
        assert "org_cache_schema(state" in source, module.__name__
