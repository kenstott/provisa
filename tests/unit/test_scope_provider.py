# Copyright (c) 2026 Kenneth Stott
# Canary: 9bda5ea1-eb8f-41a2-9568-f39bc239375d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""``${scope:ENV}`` / ``${scope:ORG}`` — where a resolution is happening (REQ-1622)."""

from __future__ import annotations

import pytest

from provisa.api.org_runtime import (
    reset_current_env,
    reset_current_org,
    set_current_env,
    set_current_org,
)
from provisa.core.environments import PROD
from provisa.core.secrets import expand_scope, resolve_secrets


@pytest.fixture
def bound():
    """A context naming an org and an environment, unbound again afterwards."""
    org_token = set_current_org("acme")
    env_token = set_current_env("feature_x")
    yield
    reset_current_env(env_token)
    reset_current_org(org_token)


def test_env_resolves_to_the_bound_environment(bound):
    assert expand_scope("${scope:ENV}") == "feature_x"


def test_org_resolves_to_the_bound_organization(bound):
    assert expand_scope("${scope:ORG}") == "acme"


def test_both_resolve_within_one_string(bound):
    assert expand_scope("/data/${scope:ORG}/${scope:ENV}/orders.db") == (
        "/data/acme/feature_x/orders.db"
    )


def test_an_unbound_context_is_prod():
    # REQ-1487: a context naming no environment IS prod. This is an answer, not a hole.
    assert expand_scope("${scope:ENV}") == PROD


def test_an_unbound_org_raises_rather_than_resolving_empty():
    with pytest.raises(KeyError):
        expand_scope("${scope:ORG}")


def test_an_unknown_scope_name_raises():
    with pytest.raises(ValueError):
        expand_scope("${scope:REGION}")


# --- the narrowness is the point ------------------------------------------------


def test_expand_scope_leaves_other_providers_written_as_the_author_wrote_them(bound):
    # A credential reference in the same string resolves at its own use point; pulling it forward
    # here would read a name nothing was going to ask for.
    assert expand_scope("postgres://u:${env:NOT_SET_ANYWHERE}@h/${scope:ENV}") == (
        "postgres://u:${env:NOT_SET_ANYWHERE}@h/feature_x"
    )


def test_expand_scope_leaves_a_plain_string_alone(bound):
    assert expand_scope("/data/orders.db") == "/data/orders.db"


def test_the_full_resolver_also_knows_the_scope_provider(bound):
    # One grammar: an author writing a path templated with both halves is writing one string.
    assert resolve_secrets("${scope:ENV}") == "feature_x"


def test_env_provider_still_means_the_process_environment(monkeypatch, bound):
    # ${env:...} is the DEPLOYMENT's environment; the two words collide in English, the provider
    # names keep them apart.
    monkeypatch.setenv("PROVISA_TEST_SCOPE_COLLISION", "process-value")
    assert resolve_secrets("${env:PROVISA_TEST_SCOPE_COLLISION}") == "process-value"
    assert resolve_secrets("${scope:ENV}") == "feature_x"
