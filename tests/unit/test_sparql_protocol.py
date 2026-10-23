# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The SPARQL 1.1 Protocol's side of /data/sparql (REQ-2135): how a request carries its query,
which media type its answer is written in, and the answer as each format writes it -- a subject's
IRI built from its table and keys, percent-encoded, outside SQL."""

# Requirements: REQ-2135

import asyncio
import csv
import io
import json

import pytest
from rdflib import Graph, URIRef
from starlette.requests import Request

from provisa.api.rest.sparql_router import _query_text
from provisa.sparql_query.results import (
    NotAcceptable,
    negotiate,
    write_ask,
    write_construct,
    write_select,
)

BASE = "https://data.example.com"
XSD = "http://www.w3.org/2001/XMLSchema#"
NODE = {"node": {"domain": "hr", "table": "persons", "key": ["a/b c"]}}


def _request(method: str, query: str = "", body: bytes = b"", content_type: str = "") -> Request:
    headers = [(b"content-type", content_type.encode())] if content_type else []

    async def receive():
        return {"type": "http.request", "body": body, "more_body": False}

    scope = {
        "type": "http",
        "method": method,
        "path": "/data/sparql",
        "query_string": query.encode(),
        "headers": headers,
    }
    return Request(scope, receive)


def _text(req: Request):
    return asyncio.run(_query_text(req))


def test_a_get_carries_its_query_in_the_query_string():
    assert _text(_request("GET", "query=ASK%20%7B%7D")) == "ASK {}"


def test_a_post_carries_it_as_a_sparql_query_body_or_a_form_field():
    assert (
        _text(_request("POST", body=b"ASK {}", content_type="application/sparql-query")) == "ASK {}"
    )
    form = _request(
        "POST", body=b"query=ASK+%7B%7D", content_type="application/x-www-form-urlencoded"
    )
    assert _text(form) == "ASK {}"


def test_a_post_of_another_content_type_is_refused():
    assert _text(_request("POST", body=b"{}", content_type="application/json")).status_code == 415


def test_a_request_without_a_query_or_with_a_dataset_is_refused():
    assert _text(_request("GET")).status_code == 400
    assert _text(_request("GET", "query=ASK%7B%7D&default-graph-uri=x")).status_code == 400


@pytest.mark.parametrize(
    "accept, form, media",
    [
        (None, "SELECT", "application/sparql-results+json"),
        ("text/csv", "SELECT", "text/csv"),
        (
            "application/sparql-results+xml;q=0.5, text/tab-separated-values",
            "SELECT",
            "text/tab-separated-values",
        ),
        ("application/json", "SELECT", "application/sparql-results+json"),
        ("*/*", "CONSTRUCT", "text/turtle"),
        ("application/n-triples", "CONSTRUCT", "application/n-triples"),
        ("application/sparql-results+xml", "ASK", "application/sparql-results+xml"),
    ],
)
def test_the_answer_is_written_in_the_first_type_the_request_accepts(accept, form, media):
    assert negotiate(accept, form)[0] == media


def test_a_type_the_form_is_not_written_in_is_not_acceptable():
    with pytest.raises(NotAcceptable):
        negotiate("text/csv", "ASK")
    with pytest.raises(NotAcceptable):
        negotiate("text/turtle", "SELECT")


def test_select_as_json_builds_the_subject_iri_percent_encoded():
    rows = [{"s": NODE, "n": {"literal": 41, "datatype": f"{XSD}integer"}}, {"s": NODE, "n": None}]
    doc = json.loads(write_select(["s", "n"], rows, BASE, "json"))
    assert doc["head"]["vars"] == ["s", "n"]
    first, second = doc["results"]["bindings"]
    assert first["s"] == {"type": "uri", "value": f"{BASE}/id/hr/persons/a%2Fb%20c"}
    assert first["n"] == {"type": "literal", "value": "41", "datatype": f"{XSD}integer"}
    assert "n" not in second  # an unbound variable is left out of its solution


def test_select_as_csv():
    rows = [{"n": {"literal": "Ann", "lang": "en"}}]
    out = list(csv.reader(io.StringIO(write_select(["n"], rows, BASE, "csv").decode())))
    assert out == [["n"], ["Ann"]]


def test_ask():
    assert json.loads(write_ask(True, "json"))["boolean"] is True
    assert b"<boolean>false</boolean>" in write_ask(False, "xml")


def test_construct_keeps_each_triple_once():
    p = {"iri": "http://example.org/knows"}
    rows = [{"s": NODE, "p": p, "o": NODE}, {"s": NODE, "p": p, "o": NODE}]
    g = Graph().parse(
        data=write_construct(rows, BASE, {"ex": "http://example.org/"}, "nt"), format="nt"
    )
    assert len(g) == 1
    iri = URIRef(f"{BASE}/id/hr/persons/a%2Fb%20c")
    assert (iri, URIRef("http://example.org/knows"), iri) in g
