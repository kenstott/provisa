# Copyright (c) 2026 Kenneth Stott
# Canary: e5b3a917-2c48-4d6f-81a0-7d9c4b2e6f53
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Wikidata's queries against the public query service (REQ-1923): each starter table and a
class table answer within the endpoint's 60-second limit, with the columns they declare. Reads
the live public service: run as a targeted CI run, never in a sweep."""

# Requirements: REQ-1923

from __future__ import annotations

import pytest

from provisa.sparql import wikidata
from provisa.sparql.source import extract_variables


@pytest.mark.parametrize("starter", wikidata.STARTER_TABLES, ids=lambda s: s.name)
async def test_each_starter_answers_within_the_limit(starter):
    rows = await wikidata.select(starter.query)  # times out at QUERY_TIMEOUT (60 s)
    assert rows, f"{starter.name} answered no rows"
    declared = [c.name for c in starter.columns]
    assert extract_variables(starter.query) == declared
    assert set().union(*(row.keys() for row in rows)) <= set(declared)
    assert all("id" in row for row in rows)


async def test_countries_hold_the_iso_codes():
    rows = await wikidata.select(wikidata.STARTER_TABLES[0].query)
    by_code = {row["iso_alpha2"]["value"]: row for row in rows}
    assert by_code["FR"]["iso_alpha3"]["value"] == "FRA"
    assert int(by_code["FR"]["population"]["value"]) > 1_000_000


async def test_a_class_is_described_and_its_table_answers():
    found = await wikidata.describe_class("Q6256", language="en")  # country
    assert 100 < found.instance_count < wikidata.MAX_ROWS
    assert found.broader and found.properties
    table = await wikidata.class_table(
        "Q6256", found.properties[:5], row_filter=None, language="en"
    )
    rows = await wikidata.select(table.query)
    assert rows


async def test_a_large_class_requires_a_filter():
    found = await wikidata.describe_class("Q5", language="en")  # human
    assert found.filter_required


async def test_search_finds_a_class_by_name():
    found = await wikidata.search_entities("sovereign state", language="en", classes=True)
    assert any(hit["id"] == "Q3624078" for hit in found)
    # A class search keeps only classes with instances: the class found has some.
    assert (await wikidata.describe_class("Q3624078", language="en")).instance_count > 0
