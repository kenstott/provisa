# Copyright (c) 2026 Kenneth Stott
# Canary: b634a0ea-3af0-46e0-8af5-fae5710c25e4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2103, REQ-2089, REQ-2099, REQ-2101, REQ-1586: the named demo ``worldcup`` works as shipped.

A real Provisa server (DuckDB engine, its own control plane) loads a copy of the demo's directory:
its config.yaml as is, every source's declared rows (replica_rows/) and the country_alias code
archive (code/). Nothing is crawled, downloaded or run: the Wikipedia and Kaggle tables and the
country_alias function's rows are landed from the declared rows, and the server is given no LLM
key.

The names disagree, so a join on them misses West Germany's titles; joined through country_alias,
Germany's WON edges hold the 1954, 1974 and 1990 finals beside 2014, in Cypher, SQL and GraphQL.
"""

# Requirements: REQ-2103, REQ-2089, REQ-2099, REQ-2101, REQ-1586

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

import httpx
import pytest

pytestmark = [pytest.mark.integration, pytest.mark.local_platform]

_REPO = Path(__file__).resolve().parents[2]
_DEMO = _REPO / "demo" / "named" / "worldcup"
_HEADERS = {"X-Provisa-Role": "org_admin"}
#: The tour's answer and the card's: each confederation's titles, every team under its member.
TITLES_CYPHER = (
    "MATCH (c:W__ViewCountries)-[:WON]->(f:W__Finals) "
    "RETURN c.confederation AS confederation, c.country AS country, count(f) AS titles "
    "ORDER BY titles DESC, country"
)


@pytest.fixture(scope="module")
def home(tmp_path_factory) -> Path:
    """A copy of the demo's directory: the server reads its config, rows and code there."""
    copy = tmp_path_factory.mktemp("worldcup_demo") / "worldcup"
    shutil.copytree(_DEMO, copy)
    return copy


@pytest.fixture(scope="module")
def server(home, tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    data = tmp_path_factory.mktemp("worldcup_demo_data")
    srv = IsolatedServer(
        "worldcup_demo",
        engine="duckdb",
        config=str(home / "config.yaml"),
        control_plane="own",
        materialize_store_url=f"duckdb:///{data / 'store.duckdb'}",
        # No LLM key and no Kaggle token: the demo reads only what it declares.
        env={"PROVISA_DATA_DIR": str(data / "provisa"), "ANTHROPIC_API_KEY": ""},
    )
    srv.start(timeout=600)
    try:
        yield srv
    finally:
        srv.stop_process()


def _post(server, path: str, body: dict) -> httpx.Response:
    return httpx.post(f"{server.base_url}{path}", json=body, headers=_HEADERS, timeout=300)


def _sql(server, sql: str) -> list[dict[str, Any]]:
    resp = _post(server, "/data/sql", {"sql": sql})
    assert resp.status_code == 200, resp.text
    return resp.json()["data"]["sql"]


def _cypher(server, query: str) -> list[dict[str, Any]]:
    resp = _post(server, "/data/cypher", {"query": query})
    assert resp.status_code == 200, resp.text
    return resp.json()["rows"]


@pytest.mark.parametrize(
    ("table", "rows"),
    [
        ("finals", 23),
        ("afc_members", 47),
        ("caf_members", 56),
        ("concacaf_members", 41),
        ("conmebol_members", 10),
        ("ofc_members", 13),
        ("uefa_members", 55),
        ("world_cups", 20),
        ("matches", 4572),
        ("players", 37784),
        ("goalscorers", 47914),
    ],
)
def test_each_public_table_is_its_declared_rows(server, table, rows):
    [count] = _sql(server, f'SELECT COUNT(*) AS n FROM "worldcup"."{table}"')
    assert count["n"] == rows


def test_country_alias_is_its_reviewed_rows(server):
    [row] = _sql(
        server,
        'SELECT canonical_country, fifa_code FROM "worldcup"."country_alias" '
        "WHERE _nf_name = 'West Germany'",
    )
    assert row == {"canonical_country": "Germany", "fifa_code": "GER"}


def test_every_team_name_is_fed_to_country_alias(server):
    matched = _sql(
        server,
        'SELECT n.name, a.fifa_code FROM "worldcup"."view_team_names" AS n '
        'JOIN "worldcup"."country_alias" AS a ON a.name = n.name',
    )
    codes = {r["name"]: r["fifa_code"] for r in matched}
    assert len(codes) == 237
    assert codes["Germany FR"] == codes["West Germany"] == codes["Germany"] == "GER"
    assert codes["Zaire"] == "COD" and codes["Soviet Union"] == "RUS"
    assert codes["German DR"] == "GDR"  # its results passed to no member


def test_the_names_disagree(server):
    """Read as written, Germany's titles are split: Wikipedia lists 1954, 1974 and 1990 under West
    Germany and 2014 under Germany."""
    rows = _sql(
        server,
        'SELECT winners, COUNT(*) AS titles FROM "worldcup"."finals" '
        "WHERE winners IN ('Germany', 'West Germany') GROUP BY winners ORDER BY winners",
    )
    assert [(r["winners"], r["titles"]) for r in rows] == [("Germany", 1), ("West Germany", 3)]


def test_germany_won_four_finals_through_country_alias(server):
    rows = _cypher(
        server,
        "MATCH (c:W__ViewCountries)-[:WON]->(f:W__Finals) WHERE c.code = 'GER' "
        "RETURN f.year AS year, f.winners AS listed_as ORDER BY year",
    )
    assert [(r["year"], r["listed_as"]) for r in rows] == [
        (1954, "West Germany"),
        (1974, "West Germany"),
        (1990, "West Germany"),
        (2014, "Germany"),
    ]


def test_titles_by_confederation(server):
    rows = _cypher(server, TITLES_CYPHER)
    by_confederation: dict[str, int] = {}
    for r in rows:
        by_confederation[r["confederation"]] = (
            by_confederation.get(r["confederation"], 0) + r["titles"]
        )
    assert by_confederation == {"UEFA": 13, "CONMEBOL": 10}
    assert rows[0] == {"confederation": "CONMEBOL", "country": "Brazil", "titles": 5}


def test_germany_played_under_every_name(server):
    rows = _cypher(
        server,
        "MATCH (c:W__ViewCountries)-[:PLAYED_HOME]->(m:W__Matches) WHERE c.code = 'GER' "
        "RETURN DISTINCT m.home_team_name AS listed_as ORDER BY listed_as",
    )
    assert [r["listed_as"] for r in rows] == ["Germany", "Germany FR"]


def test_the_1954_final_s_scorers_scored_in_it(server):
    rows = _cypher(
        server,
        "MATCH (g:W__ViewGoals)-[:SCORED_IN]->(m:W__Matches) "
        "WHERE m.year = 1954 AND m.stage = 'Final' RETURN g.player_name AS scorer ORDER BY scorer",
    )
    assert [r["scorer"] for r in rows] == [
        "Ferenc PUSKAS",
        "Helmut RAHN",
        "Max MORLOCK",
        "Zoltan CZIBOR",
    ]


def test_the_same_model_in_graphql(server):
    query = (_DEMO / "queries" / "1-germany-titles.graphql").read_text()
    resp = _post(server, "/data/graphql", {"query": query})
    assert resp.status_code == 200, resp.text
    finals = {f["year"]: f for f in resp.json()["data"]["w__finals"]}
    assert len(finals) == 23
    for year in (1954, 1974, 1990):
        assert finals[year]["winners"] == "West Germany"
        assert finals[year]["winner"] == {"canonicalCountry": "Germany", "fifaCode": "GER"}
    assert finals[2014]["winner"] == {"canonicalCountry": "Germany", "fifaCode": "GER"}


def _tour_steps(kind: str) -> list[tuple[str, str]]:
    import yaml

    steps = yaml.safe_load((_DEMO / "tour.yaml").read_text())["steps"]
    return [(s["key"], s[kind]) for s in steps if kind in s]


@pytest.mark.parametrize(("key", "sql"), _tour_steps("sql"), ids=lambda v: str(v)[:24])
def test_each_tour_statement_answers(server, key, sql):
    assert _sql(server, sql), key


@pytest.mark.parametrize(("key", "query"), _tour_steps("cypher"), ids=lambda v: str(v)[:24])
def test_each_tour_graph_answers(server, key, query):
    assert _cypher(server, query), key
