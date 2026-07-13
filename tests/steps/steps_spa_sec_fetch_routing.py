# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
"""pytest-bdd steps for REQ-1006 — deterministic Sec-Fetch-Dest-driven SPA-vs-API routing."""

from __future__ import annotations

import pytest
from pytest_bdd import given, scenarios, then, when

from provisa.ui_server import is_spa_navigation


@pytest.fixture
def shared_data():
    return {}


@given("a browser navigates directly to the SPA deep link /graph/node/42")
def given_spa_deep_link(shared_data):
    shared_data["method"] = "GET"
    shared_data["headers"] = {}


@when("the request includes Sec-Fetch-Dest: document")
def when_sec_fetch_document(shared_data):
    shared_data["headers"] = {"sec-fetch-dest": "document"}
    shared_data["is_nav"] = is_spa_navigation(shared_data["method"], shared_data["headers"])


@then("index.html is served with HTTP 200")
def then_index_served(shared_data):
    assert shared_data["is_nav"] is True  # top-level navigation → SPA shell


@given("the UI calls fetch('/api/setup/status')")
def given_ui_fetch(shared_data):
    # fetch() emits Sec-Fetch-Dest: empty, never 'document'.
    shared_data["method"] = "GET"
    shared_data["headers"] = {"sec-fetch-dest": "empty", "accept": "*/*"}


@when("the API endpoint is unlisted and returns 404")
def when_api_returns_404(shared_data):
    shared_data["is_nav"] = is_spa_navigation(shared_data["method"], shared_data["headers"])


@then("the 404 status is proxied to the browser (never silently replaced with index.html)")
def then_404_proxied(shared_data):
    # A non-navigation request is proxied to the API, so its real status (404) is surfaced.
    assert shared_data["is_nav"] is False


@given("a browser makes a XHR request to /api/v1/query")
def given_xhr_request(shared_data):
    shared_data["method"] = "GET"
    shared_data["headers"] = {"sec-fetch-dest": "empty"}


@when("the request lacks Sec-Fetch-Dest: document header")
def when_lacks_document_header(shared_data):
    shared_data["is_nav"] = is_spa_navigation(shared_data["method"], shared_data["headers"])


@then("the request is proxied to the API and its real HTTP status is returned")
def then_request_proxied(shared_data):
    assert shared_data["is_nav"] is False


scenarios("../features/REQ-1006.feature")
