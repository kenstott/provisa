# Copyright (c) 2026 Kenneth Stott
# Canary: 67936b24-0fef-423b-8e7f-4aa7514c68f5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1317: the org header name the browser SENDS must equal the one the server READS.

The class of defect: a header is a contract held in two languages, and nothing links the two ends.
Three REST call sites sent ``X-Org-Id``, a name no server code reads, so org selection on the
control-plane host silently did nothing — no error anywhere, because an unread header is
indistinguishable from an absent one. Unit tests on each side passed; only the pair is wrong.

So these tests assert the pair. They read the literal out of the TypeScript source rather than
restating it, because a copy of the name in the test would drift with the source and prove nothing.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from provisa.auth.middleware import _requested_org_from_host

_UI = Path(__file__).resolve().parents[2] / "provisa-ui" / "src"
_AUTH_FETCH = _UI / "lib" / "authFetch.ts"


def _ui_org_header() -> str:
    """The single name the UI exports for the org header (provisa-ui/src/lib/authFetch.ts)."""
    src = _AUTH_FETCH.read_text()
    m = re.search(r'export const ORG_HEADER\s*=\s*"([^"]+)"', src)
    assert m, f"ORG_HEADER export not found in {_AUTH_FETCH}"
    return m.group(1)


def _server_reads(header_name: str, value: str) -> str | None:
    """What the middleware resolves as the org when the browser sends `header_name: value` to the
    control-plane host — the only host where the header is the org source (REQ-1276)."""
    app = FastAPI()

    @app.get("/x")
    async def _x(request: Request):
        return {"org": _requested_org_from_host(request)}

    resp = TestClient(app, base_url="http://cloud.provisa.dev").get(
        "/x", headers={header_name: value}
    )
    return resp.json()["org"]


def test_ui_org_header_is_the_name_the_middleware_reads():
    assert _server_reads(_ui_org_header(), "acme") == "acme", (
        "the UI sends an org header name the server does not read — org selection is a silent no-op"
    )


def test_the_old_name_is_not_silently_accepted():
    # Guards the other direction: if someone re-adds an X-Org-Id fallback on the server, the two
    # names both "work" and the contract stops being a contract.
    assert _server_reads("X-Org-Id", "acme") is None


def test_header_name_case_does_not_matter():
    # HTTP header names are case-insensitive, so the contract holds on the lowercase wire form too.
    name = _ui_org_header()
    assert _server_reads(name.lower(), "acme") == "acme"
    assert _server_reads(name.upper(), "acme") == "acme"


@pytest.mark.parametrize(
    "path",
    [_UI / "apolloClient.ts", _UI / "api" / "admin.ts"],
)
def test_no_call_site_hardcodes_an_org_header_name(path: Path):
    """Every org-header call site must reference ORG_HEADER, not its own string literal — one
    exported constant is what keeps the sent name and the read name from drifting again."""
    src = path.read_text()
    stray = re.findall(r'"[Xx]-[Oo]rg-[A-Za-z-]+"', src)
    assert not stray, f"{path.name} hardcodes org header name(s) {stray}; import ORG_HEADER instead"
    assert "ORG_HEADER" in src
