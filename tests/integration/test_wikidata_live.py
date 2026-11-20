# Copyright (c) 2026 Kenneth Stott
# Canary: e5b3a917-2c48-4d6f-81a0-7d9c4b2e6f53
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Wikidata's queries against live Wikidata endpoints (REQ-1923). The large starters and the
class tables of large classes run against the QLever Wikidata mirror, which has no 60-second
limit, so their answers -- every row, each label joined to its id -- are checked whatever the
public service's load. A small set runs against the public query service through the product's
own call (``wikidata.select``). Reads live services: run as a targeted CI run, never in a
sweep."""

# Requirements: REQ-1923

from __future__ import annotations

import json

import httpx
import pytest

from provisa.api_source.models import ApiColumnType
from provisa.sparql import wikidata
from provisa.sparql.source import extract_variables, sparql_headers

#: The QLever Wikidata mirror's SPARQL endpoint (qlever.cs.uni-freiburg.de/api/wikidata
#: redirects here). It declares no prefixes and has no time limit of the public service's kind.
QLEVER = "https://qlever.dev/api/wikidata"
#: How long a QLever read may take: the largest here answers in about 10 s.
QLEVER_TIMEOUT = 300.0


async def _qlever(query: str) -> list[dict]:
    headers = {**sparql_headers(), "Content-Type": "application/x-www-form-urlencoded"}
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            QLEVER, data={"query": query}, headers=headers, timeout=QLEVER_TIMEOUT
        )
    # The mirror's error answer names its cause (a JSON body with an "exception"): kept in the
    # failure, which raise_for_status alone would drop.
    assert resp.status_code == 200, f"{resp.status_code}: {resp.text[:1000]}"
    # QLever writes a literal's control characters unescaped, which strict JSON refuses.
    return json.loads(resp.text, strict=False)["results"]["bindings"]


def _value(row: dict, var: str) -> str | None:
    term = row.get(var)
    return term["value"] if term is not None else None


def _assert_answer(rows: list[dict], columns: list[str], linked: list[str]) -> None:
    """The rows answer the declared columns, each with its id; each linked item's label is read
    only with its id (a label joined on an unbound id would put a label on rows without one),
    and most rows have their own label (some listed organizations have no English one)."""
    assert rows
    assert set().union(*(row.keys() for row in rows)) <= set(columns)
    assert all("id" in row for row in rows)
    for name in linked:
        assert all(f"{name}_id" in row for row in rows if name in row), name
    assert sum("name" in row for row in rows) > 0.8 * len(rows)


_STARTER = {s.name: s for s in wikidata.STARTER_TABLES}
_LINKED = {"countries": ["capital"], "admin_regions": ["country"], "organizations": ["exchange"]}


# ── The QLever mirror: the large starters and class tables ───────────────────


@pytest.mark.parametrize("name", ["admin_regions", "organizations", "languages", "countries"])
async def test_each_large_starter_answers_on_the_mirror(name):
    starter = _STARTER[name]
    columns = extract_variables(starter.query)
    assert columns == [c.name for c in starter.columns]
    _assert_answer(await _qlever(starter.query), columns, _LINKED.get(name, []))


async def test_countries_hold_the_iso_codes_and_their_capital():
    rows = await _qlever(_STARTER["countries"].query)
    by_code = {row["iso_alpha2"]["value"]: row for row in rows}
    france = by_code["FR"]
    assert _value(france, "id") == "Q142" and _value(france, "name") == "France"
    assert _value(france, "iso_alpha3") == "FRA"
    assert (_value(france, "capital_id"), _value(france, "capital")) == ("Q90", "Paris")
    assert int(france["population"]["value"]) > 1_000_000
    # One row per country: the labels are read after the rows are grouped.
    assert len({_value(row, "id") for row in rows}) == len(rows)


async def test_admin_regions_carry_their_country_by_id_and_label():
    rows = await _qlever(_STARTER["admin_regions"].query)
    assert len({_value(row, "id") for row in rows}) == len(rows)
    french = [row for row in rows if _value(row, "country_id") == "Q142"]
    assert french and all(_value(row, "country") == "France" for row in french)


def _prop(pid: str, label: str, item: bool, kind: ApiColumnType = ApiColumnType.string):
    return wikidata.ClassProperty(id=pid, label=label, type=kind, item=item, uses=0)


#: Large classes under the row limit, with the properties their instances most use (as
#: describe_class offers them): city and university, each read whole.
_LARGE_CLASSES = {
    "Q515": [
        _prop("P625", "coordinate location", False),
        _prop("P17", "country", True),
        _prop("P646", "Freebase ID", False),
        _prop("P131", "located in the administrative territorial entity", True),
        _prop("P18", "image", False),
        _prop("P1566", "GeoNames ID", False),
        _prop("P373", "Commons category", False),
        _prop("P1082", "population", False, ApiColumnType.number),
    ],
    "Q3918": [
        _prop("P17", "country", True),
        _prop("P571", "inception", False, ApiColumnType.timestamp),
        _prop("P646", "Freebase ID", False),
        _prop("P131", "located in the administrative territorial entity", True),
        _prop("P856", "official website", False),
        _prop("P213", "ISNI", False),
        _prop("P625", "coordinate location", False),
        _prop("P2427", "GRID ID", False),
    ],
}


@pytest.mark.parametrize("class_id", sorted(_LARGE_CLASSES))
async def test_a_large_class_table_answers_on_the_mirror(class_id):
    properties = _LARGE_CLASSES[class_id]
    table = wikidata.class_table_query(class_id, properties, None, "en")
    columns = [c.name for c in table.columns]
    rows = await _qlever(table.query)
    linked = [n for p, n in zip(properties, wikidata.column_names(properties)) if p.item]
    _assert_answer(rows, columns, linked)
    # One row per instance, thousands of them: the class is read whole.
    assert len(rows) > 5_000
    assert len({_value(row, "id") for row in rows}) == len(rows)
    # The country's label is the label of the country its id names.
    french = [row for row in rows if _value(row, "country_id") == "Q142"]
    assert french and all(_value(row, "country") == "France" for row in french)


# ── The public query service: a small set ────────────────────────────────────


async def test_currencies_answer_on_the_query_service():
    starter = _STARTER["currencies"]
    rows = await wikidata.select(starter.query)
    _assert_answer(rows, [c.name for c in starter.columns], [])
    by_code = {_value(row, "iso_4217"): row for row in rows}
    assert _value(by_code["EUR"], "id") == "Q4916"


async def test_a_small_class_table_answers_on_the_query_service():
    properties = [_prop("P30", "continent", True), _prop("P36", "capital", True)]
    table = await wikidata.class_table("Q6256", properties, row_filter=None, language="en")
    rows = await wikidata.select(table.query)
    _assert_answer(rows, [c.name for c in table.columns], ["continent", "capital"])
    assert len({_value(row, "id") for row in rows}) == len(rows) > 100
    france = next(row for row in rows if _value(row, "id") == "Q142")
    assert (_value(france, "capital_id"), _value(france, "capital")) == ("Q90", "Paris")


async def test_a_large_class_requires_a_filter():
    with pytest.raises(wikidata.InvalidWikidataSource) as refused:
        await wikidata.class_table("Q5", [], row_filter=None, language="en")  # human
    assert refused.value.code == "wikidata.filter_required"


async def test_search_finds_a_class_by_name():
    found = await wikidata.search_entities("sovereign state", language="en", classes=True)
    assert any(hit["id"] == "Q3624078" for hit in found)
    # A class search keeps only classes with instances: the class found has some.
    assert await wikidata.count_instances("Q3624078") > 0
