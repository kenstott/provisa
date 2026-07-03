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

from provisa.security.mutation_authz import (
    MutationKind,
    authorize_mutation,
    classify_graphql,
    classify_grpc,
    classify_hasura,
    classify_kind,
    classify_openapi,
)
from provisa.security.rights import Capability


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


def test_admin_bypasses_writable_by():
    ok, _ = authorize_mutation(_role("root", Capability.ADMIN.value), [])
    assert ok is True


def test_superadmin_bypasses_writable_by():
    ok, _ = authorize_mutation(_role("root", Capability.SUPERADMIN.value), [])
    assert ok is True


def test_admin_without_write_still_allowed():
    # ADMIN implies all capabilities including WRITE (check_capability convention).
    ok, _ = authorize_mutation(_role("root", Capability.ADMIN.value), ["someone-else"])
    assert ok is True
