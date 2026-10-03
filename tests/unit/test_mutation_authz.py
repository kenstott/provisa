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
    CommandNotFound,
    MutationKind,
    MutationNotPermitted,
    admit_command,
    classify_graphql,
    classify_grpc,
    classify_hasura,
    classify_kind,
    classify_openapi,
    reclassify_kind,
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


# --- admit_command: the one command admission (REQ-867/868/869, REQ-1758) ---------------------
#
# A command carries ONE role list, ``visible_to`` (empty assigns it to every role). A role calls it
# when it is assigned, reaches the command's domain, and -- for a mutation -- holds WRITE. A
# command the role may not use is "not found", the same as one never registered.


def _role(role_id, *caps, domains=("*",)):
    return {"id": role_id, "capabilities": list(caps), "domain_access": list(domains)}


def _command(kind="mutation", visible_to=(), domain="sales"):
    return {"kind": kind, "visible_to": list(visible_to), "domain_id": domain}


def test_no_role_is_refused():
    # REQ-1758: there is no call without a role, for a read as for a write.
    for kind in ("mutation", "query"):
        with pytest.raises(MutationNotPermitted, match="no acting role"):
            admit_command(_command(kind), None, "thing")


def test_a_mutation_needs_the_write_capability():
    with pytest.raises(MutationNotPermitted, match="WRITE") as excinfo:
        admit_command(_command(visible_to=["analyst"]), _role("analyst"), "editThing")
    assert excinfo.value.field_name == "editThing"


def test_a_role_not_assigned_finds_no_command():
    with pytest.raises(CommandNotFound):
        admit_command(
            _command(visible_to=["ops"]), _role("analyst", Capability.WRITE.value), "editThing"
        )


def test_an_empty_list_assigns_the_command_to_every_role():
    admit_command(_command(), _role("analyst", Capability.WRITE.value), "editThing")
    admit_command(_command("query"), _role("analyst"), "readThing")


def test_an_assigned_role_holding_write_is_admitted():
    admit_command(
        _command(visible_to=["analyst", "ops"]), _role("analyst", Capability.WRITE.value), "x"
    )


def test_a_role_outside_the_command_domain_finds_no_command():
    with pytest.raises(CommandNotFound):
        admit_command(
            _command(domain="hr"), _role("analyst", Capability.WRITE.value, domains=["sales"]), "x"
        )
    admit_command(
        _command(domain="sales"), _role("analyst", Capability.WRITE.value, domains=["sales"]), "x"
    )


def test_a_read_needs_no_write_capability():
    admit_command(_command("query", visible_to=["analyst"]), _role("analyst"), "readThing")


# --- nothing stands above the assignment (REQ-1327, REQ-1621) ----------------------------------
#
# No capability string means "every right", and the platform rights are over the deployment, not
# over an org's writes.

_UNLISTED = [
    ["admin"],
    ["superadmin"],
    ["admin", "superadmin"],
    ["platform_settings", "cross_org"],
    ["admin", "superadmin", "platform_settings", "cross_org", Capability.WRITE.value],
]


@pytest.mark.parametrize("held", _UNLISTED)
def test_no_capability_reaches_a_command_assigned_to_someone_else(held):
    with pytest.raises(CommandNotFound):
        admit_command(_command(visible_to=["someone-else"]), _role("root", *held), "editThing")


@pytest.mark.parametrize("held", [["admin"], ["superadmin"], ["platform_settings", "cross_org"]])
def test_an_assigned_role_still_needs_the_write_capability(held):
    # Being assigned is half the answer; WRITE is the other, and nothing implies it.
    with pytest.raises(MutationNotPermitted, match="WRITE"):
        admit_command(_command(visible_to=["root"]), _role("root", *held), "editThing")


def test_api_renders_the_refusals():  # REQ-1678
    from types import SimpleNamespace

    from provisa.api.data.action_exec import admit_command as api_gate
    from provisa.api.errors import ApiError

    state = SimpleNamespace(
        roles={
            "root": _role("root", "admin", "superadmin"),
            "writer": _role("writer", Capability.WRITE.value),
        }
    )
    with pytest.raises(ApiError) as hidden:
        api_gate(_command(visible_to=["someone-else"]), state, "root", "editThing")
    assert hidden.value.status_code == 404
    assert "Unknown command: 'editThing'" in str(hidden.value)
    with pytest.raises(ApiError) as refused:
        api_gate(_command(visible_to=["root"]), state, "root", "editThing")
    assert refused.value.status_code == 403
    assert api_gate(_command(), state, "writer", "editThing")["id"] == "writer"


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
