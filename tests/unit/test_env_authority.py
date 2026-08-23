# Copyright (c) 2026 Kenneth Stott
# Canary: a54a6160-5f9b-4a5c-8b53-09c10196e7e1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1528: what owning an environment confers, and what it deliberately does not."""

import pytest

from provisa.core import env_authority as auth
from provisa.core import env_classes as ec
from provisa.core.db import _SEED_ROLES
from provisa.security.rights import Capability

_ORG_ADMIN = frozenset(next(caps for role_id, caps in _SEED_ROLES if role_id == "org_admin"))
_DEVELOPER = frozenset(next(caps for role_id, caps in _SEED_ROLES if role_id == "developer"))


def test_the_grant_is_the_developer_role_minus_every_data_right():
    # REQ-1528 starts from a role somebody already decided on rather than a hand-picked list.
    # REQ-1539 then subtracts the data rights: creating an environment is a model-authoring act, and
    # what a person may do to DATA is what their roles say, in this environment as in every other.
    assert auth.ENVIRONMENT_OWNER_CAPABILITIES == _DEVELOPER - {"write", "full_results", "usage"}
    assert auth.ENVIRONMENT_OWNER_CAPABILITIES < _DEVELOPER


def test_the_grant_never_exceeds_what_an_org_admin_holds():
    assert auth.ENVIRONMENT_OWNER_CAPABILITIES <= _ORG_ADMIN


@pytest.mark.parametrize(
    "capability",
    [
        Capability.QUERY_DEVELOPMENT.value,
        Capability.CREATE_RELATIONSHIP.value,
        Capability.CREATE_VIEW.value,
    ],
)
def test_building_the_model_is_exactly_what_owning_an_environment_is_for(capability):
    assert capability in auth.ENVIRONMENT_OWNER_CAPABILITIES


@pytest.mark.parametrize(
    "capability",
    [
        Capability.SOURCE_REGISTRATION.value,
        Capability.TABLE_REGISTRATION.value,
        Capability.MASKING_CONFIG.value,
        Capability.COLUMN_GRANT.value,
    ],
)
def test_the_surfaces_an_org_admin_keeps_are_not_part_of_the_grant(capability):
    # REQ-1528: these are the org_admin's own — binding a source to a credential, and deciding what
    # any role may see of it. Branching confers a developer's rights, and a developer holds none.
    assert capability in _ORG_ADMIN
    assert capability not in auth.ENVIRONMENT_OWNER_CAPABILITIES


@pytest.mark.parametrize(
    "capability",
    ["user_management", "platform_settings", "cross_org", "observability", "admin", "superadmin"],
)
def test_the_organization_itself_is_never_part_of_the_grant(capability):
    # The grant is model-building and nothing adjacent: managing members, billing and platform
    # settings are the organization rather than the model.
    assert capability not in auth.ENVIRONMENT_OWNER_CAPABILITIES


def test_every_granted_name_is_a_capability_that_exists():
    # A misspelling in the seed would grant nothing while reading as though it did.
    real = {c.value for c in Capability}
    assert auth.ENVIRONMENT_OWNER_CAPABILITIES <= real


class TestDomainMembership:
    """REQ-1530: the role says what kind of act; domain membership says which objects."""

    def test_a_developer_is_unlimited_until_an_org_admin_narrows_them(self):
        # Every seeded role carries ["*"], so this is the state of the world before anyone scopes it.
        assert auth.domains_within([auth.ALL_DOMAINS]) is None
        assert auth.may_change_domain([auth.ALL_DOMAINS], "finance")

    def test_a_scoped_developer_may_change_only_their_own_domains(self):
        assert auth.domains_within(["sales", "marketing"]) == frozenset({"sales", "marketing"})
        assert auth.may_change_domain(["sales"], "sales")
        assert not auth.may_change_domain(["sales"], "finance")

    def test_a_developer_scoped_to_nothing_may_change_nothing(self):
        # An empty list is a real answer — no domains — not an absent one meaning all of them.
        assert auth.domains_within([]) == frozenset()
        assert not auth.may_change_domain([], "sales")

    def test_a_caller_that_never_read_domain_access_is_refused_rather_than_waved_through(self):
        # domain_access is NOT NULL on roles, so None is a bug in the reader, and defaulting it
        # either way would answer an authorization question the caller never actually asked.
        with pytest.raises(ValueError, match="never read it"):
            auth.domains_within(None)

    def test_the_limit_is_not_a_rule_that_only_applies_inside_a_branch(self):
        # REQ-1530: the same function answers for the org and for a branch, so there is no second
        # rule to state, test and remember for branches alone.
        assert auth.domains_within(["sales"]) == frozenset({"sales"})


def test_creating_an_environment_confers_no_right_over_data():
    # REQ-1539: the escalation REQ-1492 worried about was write authority arriving with an
    # environment. It cannot arrive at all now — the grant carries no data right to begin with, and
    # REQ-1491 still leaves the new environment bound to nothing.
    assert not (auth.ENVIRONMENT_OWNER_CAPABILITIES & {"write", "full_results", "usage"})
    assert ec.binding_columns("sources") >= {"host", "port", "database", "username"}


def test_a_credential_is_the_privilege_the_grant_does_not_hand_over():
    # REQ-1528: the escalation is worthless because reaching data needs secrets the copy never
    # carries — the owner must reconstruct the URIs and supply the credentials themselves.
    assert "org_secrets" in ec.NEVER_SENSITIVE
    assert "org_secrets" not in ec.CARRIED | ec.IDENTITY_ONLY
