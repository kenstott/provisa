# Copyright (c) 2026 Kenneth Stott
# Canary: ace11082-8c55-44be-bc78-2470cb4d9858
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests run as work for the deployment's own org (REQ-1266).

The product binds an org at every entrypoint -- the boot, each request, each protocol session,
each background job -- and refuses a per-org read with none bound. A unit test calls below those
entrypoints, so it stands in for one: each test runs with the deployment org bound, as the boot
binds it.

That binding is the test harness's, not the product's, so it must never hide an entrypoint that
forgets to bind. A test marked ``@pytest.mark.unbound`` runs with nothing bound: the entrypoint
binders are proven under it (each binds by itself), and so are the refusals.
"""

from __future__ import annotations

import sys

import pytest

# The org a fresh AppState serves (AppState._org_id) -- used when the app module is not loaded.
_DEPLOYMENT_ORG = "default"


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "unbound: run with no org bound -- for the entrypoints that bind one themselves and for "
        "the refusal of work bound to none (REQ-1266)",
    )


@pytest.fixture(autouse=True)
def _deployment_org_bound(request: pytest.FixtureRequest):
    if request.node.get_closest_marker("unbound") is not None:
        yield
        return
    from provisa.core.request_context import reset_current_org, set_current_org

    app = sys.modules.get("provisa.api.app")
    org_id = app.state.org_id if app is not None else _DEPLOYMENT_ORG
    token = set_current_org(org_id)
    yield
    if "monkeypatch" in request.fixturenames:
        # A routed AppState attribute monkeypatch replaced is put back while the org is bound.
        request.getfixturevalue("monkeypatch").undo()
    reset_current_org(token)
