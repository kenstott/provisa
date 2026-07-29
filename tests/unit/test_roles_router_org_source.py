# Copyright (c) 2026 Kenneth Stott
# Canary: c2e00b4b-f308-455b-8fbe-60cff091c8d4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1317: a router's org comes from the middleware, never from the caller.

The class of defect: an endpoint reads its own org out of a client-supplied header and defaults when
it is absent. That is two bugs at once — the caller picks which org's rows it operates on, and a
caller who picks nothing is silently handed the default org's. ``roles_router`` did both. These tests
pin the org SOURCE, so re-adding either a header read or a default fails here.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI, HTTPException, Request
from fastapi.testclient import TestClient

from provisa.api.admin.roles_router import _active_org


def _app(active_org):
    """A request whose `state.active_org_id` is whatever the middleware would have resolved."""
    app = FastAPI()

    @app.get("/probe")
    async def _probe(request: Request):
        request.state.active_org_id = active_org
        try:
            return {"org": _active_org(request)}
        except HTTPException as exc:
            return {"error": exc.status_code}

    return app


def test_org_is_the_middleware_resolved_value():
    assert TestClient(_app("acme")).get("/probe").json() == {"org": "acme"}


def test_unresolved_org_is_a_401_not_a_default():
    # The no-fallback rule: `"root"` here silently pointed every org-less request at the default
    # org's roles. An unresolved org is a wiring failure and must surface as one.
    assert TestClient(_app(None)).get("/probe").json() == {"error": 401}


@pytest.mark.parametrize("header", ["X-Org-Id", "x-org-id", "X-Org-Provisa"])
def test_a_client_supplied_header_cannot_choose_the_org(header):
    # Only AuthMiddleware may set active_org_id — it is the thing that checked membership. A router
    # that reads the raw header skips that check entirely.
    client = TestClient(_app("acme"))
    assert client.get("/probe", headers={header: "victim"}).json() == {"org": "acme"}


def test_roles_router_source_reads_no_request_headers():
    """Structural guard: the header read is what made org choice a client decision, so the module
    must not touch `request.headers` at all."""
    from pathlib import Path

    import provisa.api.admin.roles_router as mod

    src = Path(mod.__file__).read_text()
    assert ".headers" not in src, "roles_router must take its org from the middleware, not a header"
