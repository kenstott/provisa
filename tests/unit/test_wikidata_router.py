# Copyright (c) 2026 Kenneth Stott
# Canary: c3a7e185-4d29-4b6f-9f02-6e1b8d4a7c39
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The admin routes behind a Wikidata source's table form (REQ-1923), Wikidata stubbed."""

# Requirements: REQ-1923

from __future__ import annotations

import httpx
import pytest
import respx
from fastapi import FastAPI
from fastapi.testclient import TestClient

from provisa.api.admin.wikidata_router import router
from provisa.sparql import wikidata

_INT = "http://www.w3.org/2001/XMLSchema#integer"


@pytest.fixture
def client() -> TestClient:
    app = FastAPI()
    app.include_router(router)
    return TestClient(app)


def _count(n: int) -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "head": {"vars": ["count"]},
            "results": {
                "bindings": [{"count": {"type": "literal", "value": str(n), "datatype": _INT}}]
            },
        },
    )


def test_the_starter_tables_are_listed(client):
    answer = client.get("/admin/sources/sparql/wikidata/starters")
    assert answer.status_code == 200
    names = [s["name"] for s in answer.json()]
    assert names == ["countries", "currencies", "languages", "admin_regions", "organizations"]
    countries = answer.json()[0]
    assert {"name": "population", "data_type": "integer"} in countries["columns"]


@respx.mock
def test_search_answers_the_matches(client):
    respx.get(wikidata.SEARCH_API).mock(
        return_value=httpx.Response(200, json={"search": [{"id": "Q515", "label": "city"}]})
    )
    answer = client.get(
        "/admin/sources/sparql/wikidata/search", params={"q": "city", "language": "en"}
    )
    assert answer.status_code == 200
    assert answer.json() == [{"id": "Q515", "label": "city", "description": None}]


@respx.mock
def test_class_table_returns_the_query_and_columns(client):
    respx.post(wikidata.ENDPOINT).mock(return_value=_count(120))
    answer = client.post(
        "/admin/sources/sparql/wikidata/class-table",
        json={
            "class_id": "Q515",
            "properties": [
                {"id": "P1082", "label": "population", "data_type": "double", "item": False}
            ],
            "filter": None,
            "language": "en",
        },
    )
    assert answer.status_code == 200
    body = answer.json()
    assert body["columns"] == [
        {"name": "id", "data_type": "text"},
        {"name": "name", "data_type": "text"},
        {"name": "population", "data_type": "double"},
    ]
    assert "wdt:P31 wd:Q515" in body["query"]


@respx.mock
def test_class_table_over_the_limit_without_a_filter_is_refused(client):
    respx.post(wikidata.ENDPOINT).mock(return_value=_count(wikidata.MAX_ROWS + 1))
    answer = client.post(
        "/admin/sources/sparql/wikidata/class-table",
        json={"class_id": "Q5", "properties": [], "filter": None, "language": "en"},
    )
    assert answer.status_code == 422


def test_a_bad_class_id_is_refused(client):
    answer = client.get(
        "/admin/sources/sparql/wikidata/classes/Q5%20%7D", params={"language": "en"}
    )
    assert answer.status_code == 422


@respx.mock
def test_wikidata_unreachable_is_a_502(client):
    respx.get(wikidata.SEARCH_API).mock(side_effect=httpx.ConnectError("down"))
    answer = client.get(
        "/admin/sources/sparql/wikidata/search", params={"q": "city", "language": "en"}
    )
    assert answer.status_code == 502
