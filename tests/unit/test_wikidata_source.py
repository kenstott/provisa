# Copyright (c) 2026 Kenneth Stott
# Canary: 5b2e9c41-7a3d-4f86-9e10-c4d8a2f7b653
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Wikidata, a branded SPARQL source (REQ-1923): its preset, its starter reference tables, the
table of the instances of any class, and replication by default. Wikidata is never called:
every HTTP exchange is stubbed."""

# Requirements: REQ-1923

from __future__ import annotations

import json
import re
from urllib.parse import parse_qs

import httpx
import pytest
import respx

from provisa.api.admin.types import SourceInput
from provisa.api_source.models import ApiColumnType
from provisa.core.replicate import ALWAYS
from provisa.sparql import wikidata
from provisa.sparql.source import extract_variables

# ── 1. The preset ────────────────────────────────────────────────────────────


def _input(**fields) -> SourceInput:
    return SourceInput(
        id="wd",
        type="sparql",
        federation_hints_json=json.dumps({"brand": "wikidata"}),
        **fields,
    )


def test_preset_fills_the_query_service_endpoint_and_no_credential():
    given = _input()
    assert wikidata.apply_preset(given) is True
    assert given.host == "https://query.wikidata.org/sparql"
    assert given.username == "" and given.password == ""


def test_preset_replaces_any_endpoint_sent_with_the_brand():
    given = _input(host="https://dbpedia.org/sparql")
    wikidata.apply_preset(given)
    assert given.host == wikidata.ENDPOINT


def test_a_plain_sparql_source_is_left_as_it_is():
    given = SourceInput(id="s", type="sparql", host="https://dbpedia.org/sparql")
    assert wikidata.apply_preset(given) is False
    assert given.host == "https://dbpedia.org/sparql"
    assert given.replicate is None and given.cache_ttl is None


def test_a_wikidata_source_takes_no_credential():
    with pytest.raises(wikidata.InvalidWikidataSource) as refused:
        wikidata.apply_preset(_input(password="secret"))
    assert refused.value.code == "wikidata.no_credential"


# ── 4. Replicated by default ─────────────────────────────────────────────────


def test_wikidata_tables_are_replicated_on_a_schedule_by_default():
    given = _input()
    wikidata.apply_preset(given)
    assert given.replicate == ALWAYS
    assert given.cache_ttl == wikidata.REFRESH_SECONDS


def test_replication_settings_the_steward_chose_are_kept():
    given = _input(replicate=-1, cache_ttl=3600)
    wikidata.apply_preset(given)
    assert given.replicate == -1
    assert given.cache_ttl == 3600


# ── 2. Starter reference tables ──────────────────────────────────────────────

_STARTERS = {s.name: s for s in wikidata.STARTER_TABLES}


def test_the_starter_tables_offered():
    assert set(_STARTERS) == {
        "countries",
        "currencies",
        "languages",
        "admin_regions",
        "organizations",
    }


@pytest.mark.parametrize("name", sorted(_STARTERS))
def test_each_starter_query_selects_exactly_its_typed_columns(name):
    starter = _STARTERS[name]
    assert extract_variables(starter.query) == [c.name for c in starter.columns]
    assert starter.columns[0].name == "id" and starter.columns[1].name == "name"


@pytest.mark.parametrize("name", sorted(_STARTERS))
def test_each_starter_query_is_bounded(name):
    """Every starter reads a bounded set: anchored on an identifier or a closed class, one hop
    of subclass at most (no P279* walk), so it ends within the public endpoint's time limit."""
    query = _STARTERS[name].query
    assert "P279*" not in query and "P279+" not in query
    assert "SERVICE wikibase:label" not in query  # labels read directly, grouped
    assert re.search(r"\bGROUP BY\b", query)


def _types(name: str) -> dict[str, ApiColumnType]:
    return {c.name: c.type for c in _STARTERS[name].columns}


def test_countries_carry_their_iso_codes_capital_and_population():
    assert _types("countries") == {
        "id": ApiColumnType.string,
        "name": ApiColumnType.string,
        "iso_alpha2": ApiColumnType.string,
        "iso_alpha3": ApiColumnType.string,
        "capital_id": ApiColumnType.string,
        "capital": ApiColumnType.string,
        "population": ApiColumnType.integer,
    }
    query = _STARTERS["countries"].query
    for prop in ("wdt:P297", "wdt:P298", "wdt:P36", "wdt:P1082"):
        assert prop in query


def test_currencies_carry_their_iso_4217_code():
    assert _types("currencies") == {
        "id": ApiColumnType.string,
        "name": ApiColumnType.string,
        "iso_4217": ApiColumnType.string,
    }
    assert "wdt:P498" in _STARTERS["currencies"].query


def test_languages_carry_their_iso_639_codes():
    assert _types("languages") == {
        "id": ApiColumnType.string,
        "name": ApiColumnType.string,
        "iso_639_1": ApiColumnType.string,
        "iso_639_2": ApiColumnType.string,
        "iso_639_3": ApiColumnType.string,
    }
    query = _STARTERS["languages"].query
    for prop in ("wdt:P218", "wdt:P219", "wdt:P220"):
        assert prop in query


def test_admin_regions_are_the_first_level_subdivisions_with_their_country():
    assert _types("admin_regions") == {
        "id": ApiColumnType.string,
        "name": ApiColumnType.string,
        "iso_3166_2": ApiColumnType.string,
        "country_id": ApiColumnType.string,
        "country": ApiColumnType.string,
    }
    query = _STARTERS["admin_regions"].query
    assert "wd:Q10864048" in query and "wdt:P300" in query and "wdt:P17" in query


def test_organizations_carry_lei_ticker_and_isin():
    assert _types("organizations") == {
        "id": ApiColumnType.string,
        "name": ApiColumnType.string,
        "exchange_id": ApiColumnType.string,
        "exchange": ApiColumnType.string,
        "ticker": ApiColumnType.string,
        "lei": ApiColumnType.string,
        "isin": ApiColumnType.string,
    }
    query = _STARTERS["organizations"].query
    for prop in ("p:P414", "pq:P249", "wdt:P1278", "wdt:P946"):
        assert prop in query


def test_a_starter_is_described_for_the_picker():
    described = wikidata.describe_starter(_STARTERS["currencies"])
    assert described == {
        "name": "currencies",
        "query": _STARTERS["currencies"].query,
        "columns": [
            {"name": "id", "data_type": "text"},
            {"name": "name", "data_type": "text"},
            {"name": "iso_4217", "data_type": "text"},
        ],
    }


# ── 3. A table of any class ──────────────────────────────────────────────────

_SEARCH = "https://www.wikidata.org/w/api.php"


def _sparql_answer(vars_: list[str], rows: list[dict]) -> dict:
    return {"head": {"vars": vars_}, "results": {"bindings": rows}}


def _lit(value: str, datatype: str | None = None) -> dict:
    term = {"type": "literal", "value": value}
    if datatype:
        term["datatype"] = datatype
    return term


def _uri(value: str) -> dict:
    return {"type": "uri", "value": value}


_E = "http://www.wikidata.org/entity/"
_INT = "http://www.w3.org/2001/XMLSchema#integer"


@respx.mock
async def test_class_search_by_name_reads_wbsearchentities():
    route = respx.get(_SEARCH).mock(
        return_value=httpx.Response(
            200,
            json={
                "search": [
                    {"id": "Q515", "label": "city", "description": "large human settlement"},
                    {"id": "Q1549591", "label": "big city"},
                ]
            },
        )
    )
    found = await wikidata.search_entities("city", language="en")
    assert found == [
        {"id": "Q515", "label": "city", "description": "large human settlement"},
        {"id": "Q1549591", "label": "big city", "description": None},
    ]
    request = route.calls.last.request
    assert request.url.params["action"] == "wbsearchentities"
    assert request.url.params["search"] == "city"
    assert request.url.params["type"] == "item"
    assert request.url.params["language"] == "en"
    assert request.headers["User-Agent"].startswith("Provisa/")


def _queries(route) -> list[str]:
    return [parse_qs(c.request.content.decode())["query"][0] for c in route.calls]


@respx.mock
async def test_browsing_a_class_reads_its_broader_and_narrower_classes_and_count():
    def answer(request: httpx.Request) -> httpx.Response:
        query = parse_qs(request.content.decode())["query"][0]
        if "COUNT(" in query and "?count" in query:
            return httpx.Response(
                200, json=_sparql_answer(["count"], [{"count": _lit("10542", _INT)}])
            )
        if "?direction" in query:
            return httpx.Response(
                200,
                json=_sparql_answer(
                    ["cls", "label", "direction"],
                    [
                        {
                            "cls": _uri(_E + "Q486972"),
                            "label": _lit("human settlement"),
                            "direction": _lit("broader"),
                        },
                        {
                            "cls": _uri(_E + "Q1637706"),
                            "label": _lit("million city"),
                            "direction": _lit("narrower"),
                        },
                    ],
                ),
            )
        return httpx.Response(
            200,
            json=_sparql_answer(
                ["prop", "label", "kind", "uses"],
                [
                    {
                        "prop": _uri(_E + "P17"),
                        "label": _lit("country"),
                        "kind": _uri("http://wikiba.se/ontology#WikibaseItem"),
                        "uses": _lit("990", _INT),
                    },
                    {
                        "prop": _uri(_E + "P1082"),
                        "label": _lit("population"),
                        "kind": _uri("http://wikiba.se/ontology#Quantity"),
                        "uses": _lit("870", _INT),
                    },
                    {
                        "prop": _uri(_E + "P571"),
                        "label": _lit("inception"),
                        "kind": _uri("http://wikiba.se/ontology#Time"),
                        "uses": _lit("400", _INT),
                    },
                ],
            ),
        )

    route = respx.post(wikidata.ENDPOINT).mock(side_effect=answer)
    found = await wikidata.describe_class("Q515", language="en")
    assert found.id == "Q515"
    assert found.instance_count == 10542
    assert found.broader == [{"id": "Q486972", "label": "human settlement"}]
    assert found.narrower == [{"id": "Q1637706", "label": "million city"}]
    assert [(p.id, p.label, p.type, p.item, p.uses) for p in found.properties] == [
        ("P17", "country", ApiColumnType.string, True, 990),
        ("P1082", "population", ApiColumnType.number, False, 870),
        ("P571", "inception", ApiColumnType.timestamp, False, 400),
    ]
    assert found.filter_required is False
    queries = _queries(route)
    # The property count reads a bounded sample of the instances, never the whole class.
    counting = next(q for q in queries if "?uses" in q)
    assert f"LIMIT {wikidata.PROPERTY_SAMPLE}" in counting
    assert "wd:Q515" in counting
    # Subclass of: one hop each way, bounded.
    browsing = next(q for q in queries if "?direction" in q)
    assert "wdt:P279" in browsing and "P279*" not in browsing


@respx.mock
async def test_a_class_over_the_limit_requires_a_filter():
    def answer(request: httpx.Request) -> httpx.Response:
        query = parse_qs(request.content.decode())["query"][0]
        if "?count" in query:
            over = str(wikidata.MAX_ROWS + 1)
            return httpx.Response(
                200, json=_sparql_answer(["count"], [{"count": _lit(over, _INT)}])
            )
        return httpx.Response(200, json=_sparql_answer([], []))

    respx.post(wikidata.ENDPOINT).mock(side_effect=answer)
    found = await wikidata.describe_class("Q5", language="en")
    assert found.instance_count == wikidata.MAX_ROWS + 1
    assert found.filter_required is True


@pytest.mark.parametrize("bad", ["Q5; DROP", "P31", "Q", "wd:Q5", "Q5 }"])
async def test_a_class_id_is_a_wikidata_item_id(bad):
    with pytest.raises(wikidata.InvalidWikidataSource) as refused:
        await wikidata.describe_class(bad, language="en")
    assert refused.value.code == "wikidata.bad_id"


def _prop(pid: str, label: str, kind: ApiColumnType, item: bool) -> wikidata.ClassProperty:
    return wikidata.ClassProperty(id=pid, label=label, type=kind, item=item, uses=1)


@respx.mock
async def test_saving_a_class_table_defines_its_sparql_query_and_typed_columns():
    respx.post(wikidata.ENDPOINT).mock(
        return_value=httpx.Response(
            200, json=_sparql_answer(["count"], [{"count": _lit("10542", _INT)}])
        )
    )
    table = await wikidata.class_table(
        "Q515",
        [
            _prop("P17", "country", ApiColumnType.string, True),
            _prop("P1082", "population", ApiColumnType.number, False),
        ],
        row_filter=None,
        language="en",
    )
    assert [(c.name, c.type) for c in table.columns] == [
        ("id", ApiColumnType.string),
        ("name", ApiColumnType.string),
        ("country_id", ApiColumnType.string),
        ("country", ApiColumnType.string),
        ("population", ApiColumnType.number),
    ]
    assert extract_variables(table.query) == [c.name for c in table.columns]
    assert "wdt:P31 wd:Q515" in table.query
    assert "wdt:P17" in table.query and "wdt:P1082" in table.query
    assert "P279*" not in table.query


@respx.mock
async def test_saving_a_class_over_the_limit_without_a_filter_is_refused():
    respx.post(wikidata.ENDPOINT).mock(
        return_value=httpx.Response(
            200,
            json=_sparql_answer(["count"], [{"count": _lit(str(wikidata.MAX_ROWS + 1), _INT)}]),
        )
    )
    with pytest.raises(wikidata.InvalidWikidataSource) as refused:
        await wikidata.class_table("Q5", [], row_filter=None, language="en")
    assert refused.value.code == "wikidata.filter_required"
    assert refused.value.params == {"count": wikidata.MAX_ROWS + 1, "limit": wikidata.MAX_ROWS}


@respx.mock
async def test_a_filter_bounds_a_large_class_and_is_counted_again():
    route = respx.post(wikidata.ENDPOINT).mock(
        return_value=httpx.Response(
            200, json=_sparql_answer(["count"], [{"count": _lit("4200", _INT)}])
        )
    )
    table = await wikidata.class_table(
        "Q5",
        [_prop("P27", "country of citizenship", ApiColumnType.string, True)],
        row_filter=wikidata.RowFilter(property="P106", value="Q82955"),
        language="en",
    )
    assert "wdt:P106 wd:Q82955" in table.query
    counted = _queries(route)[-1]
    assert "wdt:P106 wd:Q82955" in counted and "wdt:P31 wd:Q5" in counted


@respx.mock
async def test_a_filter_that_still_leaves_too_many_rows_is_refused():
    respx.post(wikidata.ENDPOINT).mock(
        return_value=httpx.Response(
            200,
            json=_sparql_answer(["count"], [{"count": _lit(str(wikidata.MAX_ROWS + 5), _INT)}]),
        )
    )
    with pytest.raises(wikidata.InvalidWikidataSource) as refused:
        await wikidata.class_table(
            "Q5",
            [],
            row_filter=wikidata.RowFilter(property="P21", value="Q6581072"),
            language="en",
        )
    assert refused.value.code == "wikidata.filter_too_wide"


def test_column_names_from_property_labels_are_distinct_identifiers():
    names = wikidata.column_names(
        [
            _prop("P1", "date of birth", ApiColumnType.timestamp, False),
            _prop("P2", "Date of Birth!", ApiColumnType.timestamp, False),
            _prop("P3", "名前", ApiColumnType.string, False),
        ]
    )
    assert names == ["date_of_birth", "date_of_birth_p2", "p3"]
