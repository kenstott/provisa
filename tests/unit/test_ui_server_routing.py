# Copyright (c) 2026 Kenneth Stott
# Canary: 5d58caff-7189-4e92-a23b-748b5b71f09a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

# REQ-1006: Sec-Fetch-Dest-driven API-vs-SPA routing decision.
import pytest

from provisa.ui_server import _STATIC_PREFIXES, is_spa_navigation


@pytest.mark.parametrize(
    "method,headers,expected",
    [
        # Sec-Fetch-Dest: document is a top-level navigation -> SPA shell.
        ("GET", {"sec-fetch-dest": "document"}, True),
        ("GET", {"sec-fetch-dest": "document", "accept": "*/*"}, True),
        # fetch/XHR/EventSource emit empty dest -> proxied to API.
        ("GET", {"sec-fetch-dest": "empty", "accept": "text/html"}, False),
        # iframe subresource -> proxied.
        ("GET", {"sec-fetch-dest": "iframe"}, False),
        # Non-GET is always an API request even with document dest absent.
        ("POST", {"accept": "text/html"}, False),
        ("DELETE", {"sec-fetch-dest": "document"}, True),  # document wins per header
        # Legacy UA (no Sec-Fetch-Dest): fall back to GET + Accept: text/html.
        ("GET", {"accept": "text/html,application/xhtml+xml"}, True),
        ("GET", {"accept": "application/json"}, False),
        ("GET", {}, False),
    ],
)
def test_is_spa_navigation(method, headers, expected):
    assert is_spa_navigation(method, headers) is expected


def test_auth_relay_is_served_as_a_file_not_proxied():
    # REQ-1348: the relay is fetched as an iframe subresource, which is_spa_navigation correctly
    # reports as a non-navigation. Without a static prefix that answer sends it to the API proxy,
    # so the org subdomain gets a 502 instead of the page that hands it a bearer.
    assert is_spa_navigation("GET", {"sec-fetch-dest": "iframe"}) is False
    assert "/auth-relay.html" in _STATIC_PREFIXES


def test_voyager_assets_are_served_as_files_not_proxied():
    # The SDL view's Voyager bundles load as script/style subresources of a srcDoc iframe.
    # Without the static prefix they reach the API proxy and answer 401, blanking the view.
    assert is_spa_navigation("GET", {"sec-fetch-dest": "script"}) is False
    assert is_spa_navigation("GET", {"sec-fetch-dest": "style"}) is False
    assert "/voyager/" in _STATIC_PREFIXES


def test_non_get_navigation_never_serves_spa():
    # A POST with document dest is a non-navigation edge; header still governs,
    # but the point is a non-GET without the header is never SPA.
    assert is_spa_navigation("POST", {"accept": "text/html"}) is False
    assert is_spa_navigation("PUT", {}) is False


def test_spa_shell_is_never_cached(tmp_path, monkeypatch):
    # /admin/ai-models is both an SPA tab and its own GET endpoint. FileResponse alone sends
    # last-modified/etag with no Cache-Control, so the browser heuristically caches the shell
    # under that URL and answers the tab's fetch() from cache — the JSON parse then dies on
    # "<!doctype" and the tab renders only that SyntaxError.
    from fastapi.testclient import TestClient

    import provisa.ui_server as ui_server

    (tmp_path / "index.html").write_text("<!doctype html><title>provisa</title>")
    monkeypatch.setattr(ui_server, "STATIC_DIR", tmp_path)
    with TestClient(ui_server.app) as client:
        resp = client.get("/admin/ai-models", headers={"sec-fetch-dest": "document"})
    assert resp.status_code == 200
    assert resp.headers["cache-control"] == "no-store"
