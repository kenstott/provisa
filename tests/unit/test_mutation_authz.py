# Copyright (c) 2026 Kenneth Stott
# Canary: 2b4d6f8a-1c3e-4507-9b8d-0a2c4e6f8b1d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for the mutation-authz core (REQ-867, REQ-868, REQ-869).

Pure logic — no I/O, no DB. Covers the protocol classifiers, the kind taint, and
the WRITE-capability + per-mutation writable_by default-deny decision.
"""

from __future__ import annotations

import pytest

from provisa.security.mutation_authz import (
    MutationKind,
    authorize_mutation,
    classify_graphql,
    classify_grpc,
    classify_hasura,
    classify_kind,
    classify_openapi,
    reclassify_kind,
    require_mutation_write,
)
from provisa.security.rights import Capability, InsufficientRightsError


# --- protocol classifiers (REQ-869) --------------------------------------------


def test_openapi_get_is_read():
    assert classify_openapi("GET") is MutationKind.READ


def test_openapi_write_methods_are_write():
    for m in ("POST", "PUT", "PATCH", "DELETE"):
        assert classify_openapi(m) is MutationKind.WRITE


def test_openapi_unknown_method_defaults_to_write():
    assert classify_openapi("OPTIONS") is MutationKind.WRITE
    assert classify_openapi(None) is MutationKind.WRITE


def test_openapi_provisa_kind_override():
    assert classify_openapi("GET", provisa_kind="mutation") is MutationKind.WRITE
    assert classify_openapi("POST", provisa_kind="query") is MutationKind.READ


def test_graphql_operation_type():
    assert classify_graphql("mutation") is MutationKind.WRITE
    assert classify_graphql("query") is MutationKind.READ
    assert classify_graphql(None) is MutationKind.READ


def test_grpc_idempotency_level():
    assert classify_grpc("NO_SIDE_EFFECTS") is MutationKind.READ
    assert classify_grpc("IDEMPOTENT") is MutationKind.WRITE
    assert classify_grpc("IDEMPOTENCY_UNKNOWN") is MutationKind.WRITE
    assert classify_grpc(None) is MutationKind.WRITE


def test_hasura_action_type():
    assert classify_hasura("mutation") is MutationKind.WRITE
    assert classify_hasura("query") is MutationKind.READ


# --- kind taint (REQ-869) ------------------------------------------------------


def test_classify_kind_query_is_read():
    assert classify_kind("query") is MutationKind.READ


def test_classify_kind_mutation_is_write():
    assert classify_kind("mutation") is MutationKind.WRITE


def test_classify_kind_unknown_defaults_to_write():
    assert classify_kind(None) is MutationKind.WRITE
    assert classify_kind("something-else") is MutationKind.WRITE


# --- authorize_mutation: WRITE cap + writable_by default-deny (REQ-867/868) -----


def _role(role_id, *caps):
    return {"id": role_id, "capabilities": list(caps)}


def test_no_role_is_denied():
    ok, reason = authorize_mutation(None, ["analyst"])
    assert ok is False and "no role" in reason


def test_missing_write_capability_denied():
    ok, reason = authorize_mutation(_role("analyst"), ["analyst"])
    assert ok is False and "WRITE" in reason


def test_write_cap_but_not_in_writable_by_denied():
    ok, reason = authorize_mutation(_role("analyst", Capability.WRITE.value), ["ops"])
    assert ok is False and "writable_by" in reason


def test_empty_writable_by_is_default_deny():
    ok, _ = authorize_mutation(_role("analyst", Capability.WRITE.value), [])
    assert ok is False


def test_write_cap_and_listed_allowed():
    ok, _ = authorize_mutation(_role("analyst", Capability.WRITE.value), ["analyst", "ops"])
    assert ok is True


# --- nothing stands above the ACL (REQ-1327, REQ-1621) -------------------------
#
# The ACL is the author's own statement of who may write through the mutation, and it is the
# whole answer in every environment: no capability string means "every right", and the platform
# rights are over the deployment, not over an org's writes.

_UNLISTED = [
    ["admin"],
    ["superadmin"],
    ["admin", "superadmin"],
    ["platform_settings", "cross_org"],
    ["admin", "superadmin", "platform_settings", "cross_org", Capability.WRITE.value],
]


@pytest.mark.parametrize("held", _UNLISTED)
def test_no_capability_bypasses_an_empty_writable_by(held):
    ok, _ = authorize_mutation(_role("root", *held), [])
    assert ok is False


@pytest.mark.parametrize("held", _UNLISTED)
def test_no_capability_bypasses_a_list_naming_someone_else(held):
    ok, reason = authorize_mutation(_role("root", *held), ["someone-else"])
    assert ok is False
    assert ("writable_by" in reason) or ("WRITE" in reason)


@pytest.mark.parametrize("held", [["admin"], ["superadmin"], ["platform_settings", "cross_org"]])
def test_a_listed_role_still_needs_the_write_capability(held):
    # Being named in the list is half the answer; WRITE is the other, and nothing implies it.
    ok, reason = authorize_mutation(_role("root", *held), ["root"])
    assert ok is False and "WRITE" in reason


def test_a_listed_role_holding_write_is_allowed():
    # The ACL is the whole answer -- it does not deny outright.
    ok, _ = authorize_mutation(_role("root", Capability.WRITE.value), ["root"])
    assert ok is True


def test_require_mutation_write_refuses_an_unlisted_role():
    # REQ-1678: the security gate raises its own error; the API layer renders it as the 403.
    from provisa.security.mutation_authz import MutationNotPermitted

    action = {"kind": "mutation", "writable_by": ["someone-else"]}
    for held in _UNLISTED:
        with pytest.raises(MutationNotPermitted) as excinfo:
            require_mutation_write(action, _role("root", *held), "editThing")
        assert excinfo.value.field_name == "editThing"
    require_mutation_write(
        {"kind": "mutation", "writable_by": ["root"]},
        _role("root", Capability.WRITE.value),
        "editThing",
    )


def test_api_renders_the_refusal_as_403():  # REQ-1678
    from provisa.api.data.action_exec import require_mutation_write as api_gate
    from provisa.api.errors import ApiError

    action = {"kind": "mutation", "writable_by": ["someone-else"]}
    for held in _UNLISTED:
        with pytest.raises(ApiError) as excinfo:
            api_gate(action, _role("root", *held), "editThing")
        assert excinfo.value.status_code == 403


def test_require_mutation_write_leaves_reads_alone():
    require_mutation_write({"kind": "query"}, None, "thing")


# --- admin-only reclassification (REQ-870) -------------------------------------


def test_access_config_role_can_demote_mutation_to_read():
    kind = reclassify_kind(_role("gov", Capability.ACCESS_CONFIG.value), "mutation", "query")
    assert kind == "query"


@pytest.mark.parametrize("held", [["admin"], ["superadmin"], ["platform_settings", "cross_org"]])
def test_nothing_stands_in_for_access_config(held):
    # REQ-1327: reclassification is ACCESS_CONFIG's, and no other string or right implies it.
    with pytest.raises(InsufficientRightsError):
        reclassify_kind(_role("root", *held), "mutation", "query")


def test_non_privileged_role_cannot_reclassify():
    with pytest.raises(InsufficientRightsError):
        reclassify_kind(_role("analyst", Capability.WRITE.value), "mutation", "query")


def test_no_role_cannot_reclassify():
    with pytest.raises(InsufficientRightsError):
        reclassify_kind(None, "mutation", "query")


def test_promotion_read_to_write_is_rejected_even_for_the_right_holder():
    # Only demotion to read-safe is allowed; a read can never be promoted to a write.
    with pytest.raises(ValueError):
        reclassify_kind(_role("root", Capability.ACCESS_CONFIG.value), "query", "mutation")


def test_reclassify_noop_is_idempotent_without_privilege():
    # target == current is a no-op and needs no capability.
    assert reclassify_kind(_role("analyst"), "mutation", "mutation") == "mutation"
    assert reclassify_kind(_role("analyst"), "query", "query") == "query"
