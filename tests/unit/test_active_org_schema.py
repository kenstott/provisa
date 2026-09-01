# Copyright (c) 2026 Kenneth Stott
# Canary: 4a91d2f0-38b7-4c6e-9a5b-7d0e2c8f1b64
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Derived stores land in the environment that filled them (REQ-1623).

An MV's target, an API source's result cache and the GraphQL cache are named where they are
written, from an org id the writer holds and an environment it is never handed. What is asserted
here is that reading the environment from the request's own context puts each of those names inside
``env_schemas`` -- which is what already makes retiring the environment remove it.
"""

from __future__ import annotations

import pytest

from provisa.api.org_runtime import reset_current_env, set_current_env
from provisa.core.environments import (
    PROD,
    SCHEMA_SUFFIXES,
    active_org_schema,
    env_schemas,
)


@pytest.fixture
def in_env():
    """Serve as ``feature_x`` for the duration of one test, unbound again afterwards."""
    token = set_current_env("feature_x")
    yield "feature_x"
    reset_current_env(token)


# --- an unbound context is prod, not a hole -------------------------------------


def test_no_environment_bound_is_prods_own_name():
    # REQ-1487: a context naming no environment IS prod. The pre-environment name is the answer.
    assert active_org_schema("acme", "_mv_cache") == "org_acme_mv_cache"


def test_the_base_schema_is_the_orgs_own(in_env):
    assert active_org_schema("acme") == "org_acme_env_feature_x"


# --- every derived store follows the environment being served -------------------


@pytest.mark.parametrize("suffix", ["_mv_cache", "_api_cache", "_gql_cache"])
def test_a_derived_store_carries_the_environment(in_env, suffix):
    assert active_org_schema("acme", suffix) == f"org_acme_env_feature_x{suffix}"


@pytest.mark.parametrize("suffix", SCHEMA_SUFFIXES)
def test_every_name_is_one_retiring_the_environment_drops(in_env, suffix):
    # The point of the whole change: deprovision_org drops exactly env_schemas(), so a derived
    # store outside that list is a write prod keeps after the environment is gone.
    assert active_org_schema("acme", suffix) in env_schemas("acme", in_env)


def test_prods_cache_is_not_an_environments_cache(in_env):
    assert active_org_schema("acme", "_mv_cache") != "org_acme_mv_cache"


def test_two_environments_do_not_share_a_cache():
    first = set_current_env("feature_x")
    try:
        x = active_org_schema("acme", "_api_cache")
    finally:
        reset_current_env(first)
    second = set_current_env("feature_y")
    try:
        y = active_org_schema("acme", "_api_cache")
    finally:
        reset_current_env(second)
    assert x != y


# --- the suffix set is closed ---------------------------------------------------


def test_an_unknown_store_suffix_is_refused(in_env):
    # Silently accepting one would create a schema nothing enumerates and nothing drops.
    with pytest.raises(ValueError):
        active_org_schema("acme", "_scratch")


def test_prod_bound_explicitly_matches_the_unbound_default():
    token = set_current_env(PROD)
    try:
        assert active_org_schema("acme", "_gql_cache") == "org_acme_gql_cache"
    finally:
        reset_current_env(token)
