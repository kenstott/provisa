# Copyright (c) 2026 Kenneth Stott
# Canary: 1f1046f8-87d0-46de-8bca-d8eae32f5c02
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A source's pgwire endpoint is kept per org (REQ-1266): ensured and stopped as the bound org's,
and refused, naming why, with no org bound. A module fixture runs before the per-test binding,
so one that starts an endpoint binds the deployment org around it (tests.conftest
``as_deployment_org``), as the cloud-ops, Salesforce, ServiceNow and govdata e2e fixtures do."""

# Requirements: REQ-1266

from __future__ import annotations

import pytest

from provisa.federation import pgwire_replica as pr

pytestmark = pytest.mark.unbound


class _Replica:
    closed = False

    def retire(self) -> None:  # the registry's stop (REQ-1824)
        self.closed = True


def test_an_endpoint_is_stopped_as_the_bound_orgs_and_no_other(monkeypatch, bind_org):
    acme, globex = _Replica(), _Replica()
    monkeypatch.setattr(pr, "_ENDPOINTS", {("acme", "src"): acme, ("globex", "src"): globex})
    bind_org("acme")
    pr.stop_endpoint("src")
    assert acme.closed and not globex.closed
    assert pr._ENDPOINTS == {("globex", "src"): globex}


def test_with_no_org_bound_an_endpoint_is_refused_by_name(monkeypatch):
    monkeypatch.setattr(pr, "_ENDPOINTS", {})
    with pytest.raises(RuntimeError, match="No active org bound"):
        pr.stop_endpoint("src")
