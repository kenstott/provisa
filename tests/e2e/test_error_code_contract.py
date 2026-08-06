# Copyright (c) 2026 Kenneth Stott
# Canary: 541bb317-b32c-4dae-bad2-b9cd3710fdde
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1350 e2e: server errors carry stable machine codes + JSON params alongside the
English message, over a real server round trip.

The registered ApiError handler must serialize ``{detail, code, params}``; pre-i18n
HTTPException sites keep answering plain ``{detail}`` and remain parseable. Also the
REQ-1290 off-mode leg: with bootstrap claiming disabled (this app runs unsecured, no
auth section), POST /auth/claim-bootstrap answers 404 — with its stable code.
"""

import os

import pytest
from httpx import ASGITransport, AsyncClient

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio]


@pytest.fixture(scope="module")
async def client():
    os.environ.setdefault("PG_PASSWORD", "provisa")

    from provisa.api.app import create_app

    app = create_app()

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            yield c


class TestApiErrorWireFormat:
    async def test_code_without_params(self, client):
        """ApiError with no interpolation params: {detail, code, params: {}}. This is also
        REQ-1290's off-mode contract — bootstrap claiming disabled answers 404, never
        a silent claim."""
        resp = await client.post("/auth/claim-bootstrap")
        assert resp.status_code == 404, resp.text
        body = resp.json()
        assert set(body) == {"detail", "code", "params"}
        assert body["code"] == "auth.bootstrap_claiming_disabled"
        assert body["params"] == {}
        assert body["detail"] == "Bootstrap claiming is not enabled"

    async def test_code_with_params(self, client):
        """ApiError carrying params: the JSON params ride alongside code + English detail
        so the UI can interpolate its own catalog template. Validation-first — the
        uncompilable email rule is refused before anything is written."""
        me = await client.get("/auth/me")
        org_id = me.json()["active_org_id"]
        resp = await client.patch(
            f"/admin/orgs/{org_id}/settings", json={"email_rule": "("}
        )
        assert resp.status_code == 400, resp.text
        body = resp.json()
        assert set(body) == {"detail", "code", "params"}
        assert body["code"] == "orgs.invalid_email_rule"
        assert body["detail"].startswith("Invalid email rule")
        assert body["params"].get("error"), body

    async def test_unmigrated_detail_only_shape_remains_valid(self, client):
        """A non-ApiError error path still answers a plain {detail} with no code — the
        documented fallback the UI renders verbatim."""
        resp = await client.get("/auth/no-such-endpoint")
        assert resp.status_code == 404
        body = resp.json()
        assert "detail" in body
        assert "code" not in body
