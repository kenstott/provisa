# REQ-1006: Sec-Fetch-Dest-driven API-vs-SPA routing decision.
import pytest

from provisa.ui_server import is_spa_navigation


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


def test_non_get_navigation_never_serves_spa():
    # A POST with document dest is a non-navigation edge; header still governs,
    # but the point is a non-GET without the header is never SPA.
    assert is_spa_navigation("POST", {"accept": "text/html"}) is False
    assert is_spa_navigation("PUT", {}) is False
