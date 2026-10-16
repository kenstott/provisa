# Copyright (c) 2026 Kenneth Stott
# Canary: 6eb4ac8b-6997-45e5-8adb-e32f4095d3df
#
# This source code is licensed under the Business Source License 1.1
"""REQ-2010: a named demo's own tour names only what the demo has. Every step's target is an
object of the demo's config or one of its example queries, every page it opens is one the step's
capability opens, and every step has its words in every language the UI speaks."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
import sqlglot
import yaml

from provisa.core.models import config_table_name

from provisa.core import demo
from tests.integration.polly_bakeoff_questions import QUESTIONS

_REPO = Path(__file__).resolve().parents[2]
_NAMED = _REPO / "demo" / "named"
_LOCALES = _REPO / "provisa-ui" / "src" / "i18n" / "locales"
_TARGETS = (
    "source",
    "table",
    "relationship",
    "example",
    "sql",
    "api",
    "jsonapi",
    "grpc",
    "cypher",
    "group_by",
    "lineage",
    "term",
    "role",
    "env",
    "polly",
)
# The query parameters a table's REST endpoint declares (provisa/api/rest/openapi_spec.py).
_REST_PARAMS = {
    "limit",
    "offset",
    "fields",
    "filter",
    "orderBy",
    "params",
    "aggregate",
    "groupBy",
    "includeNodes",
}
# The page each surface's step runs on.
_SURFACE_PAGES = {"api": "/openapi", "jsonapi": "/jsonapi", "grpc": "/grpc", "cypher": "/graph"}

TOURS = sorted(_NAMED.glob("*/tour.yaml"))


def _route_capabilities() -> dict[str, str]:
    """path -> the capability its <Route> gate in App.tsx names (as tourRouteCapabilities.test.ts
    reads it for the product tour)."""
    source = (_REPO / "provisa-ui" / "src" / "App.tsx").read_text()
    out: dict[str, str] = {}
    for block in source.split("path=")[1:]:
        path = re.match(r'"([^"]+)"', block)
        cap = re.search(r'capability=\{?"([a-z_]+)"', block)
        if path and cap:
            out[path.group(1)] = cap.group(1)
    # The Admin surfaces are a `[path, capability]` table rather than JSX routes.
    for m in re.finditer(r'\["(/admin/[^"]+)",\s*"([a-z_]+)"\]', source):
        out[m.group(1)] = m.group(2)
    return out


def _relations(config: dict) -> set[str]:
    """The ``"domain"."name"`` each table and view of the demo is read by in SQL, and the ops
    record every organisation has (its audit log and reports), which a tour may show."""
    from provisa.api._meta_views import _OPS_LOG_TABLE_VIEWS, _OPS_REPORT_VIEWS
    from provisa.core.config_loader import views_as_tables

    tables = views_as_tables(json.loads(json.dumps(config)))["tables"]
    ops = {f"ops.{name}" for name in (*_OPS_LOG_TABLE_VIEWS, *_OPS_REPORT_VIEWS)}
    return {f"{t['domain_id']}.{t.get('alias') or config_table_name(t)}" for t in tables} | ops


def _surface(config: dict) -> tuple[set[tuple[str, str]], set[str], set[str]]:
    """What the demo's API surfaces name, as the schema build makes them for a role reaching every
    domain: the (domain, table) of each REST and JSON:API path, each gRPC method and each Cypher
    node label. Built from the config's tables and columns, with no server."""
    from provisa.compiler.context import build_context
    from provisa.compiler.introspect import ColumnMetadata
    from provisa.compiler.schema_gen import SchemaInput, build_table_path_map
    from provisa.core.config_loader import views_as_tables
    from provisa.cypher.label_map import CypherLabelMap
    from provisa.grpc.proto_gen import generate_proto

    tables = views_as_tables(json.loads(json.dumps(config)))["tables"]
    rows = [
        {
            "id": i,
            "source_id": t["source_id"],
            "domain_id": t["domain_id"],
            "schema_name": t.get("schema", "default"),
            "table_name": config_table_name(t),
            "alias": t.get("alias"),
            "enable_aggregates": t.get("enable_aggregates", False),
            "enable_group_by": t.get("enable_group_by", False),
            "columns": [{"column_name": c["name"], "visible_to": ["all"]} for c in t["columns"]],
            "write_ops": [],
            "write_returns_rows": True,
            "write_refused_forms": [],
        }
        for i, t in enumerate(tables, start=1)
    ]
    si = SchemaInput(
        tables=rows,
        relationships=[],
        column_types={
            row["id"]: [
                ColumnMetadata(column_name=c["name"], data_type=c["data_type"], is_nullable=True)
                for c in t["columns"]
            ]
            for row, t in zip(rows, tables)
        },
        naming_rules=config.get("naming", {}).get("rules", []),
        role={"id": "all", "capabilities": [], "domain_access": ["*"]},
        domains=config["domains"],
        domain_prefix=config.get("naming", {}).get("domain_prefix", False),
    )
    paths = {(m["domain_id"], m["table_name"]) for m in build_table_path_map(si).values()}
    methods = set(re.findall(r"^\s*rpc (\w+)\(", generate_proto(si), re.MULTILINE))
    labels = set(CypherLabelMap.from_schema(build_context(si), domain_access=["*"]).nodes)
    return paths, methods, labels


def _path_names(path: str, prefix: str) -> tuple[str, str]:
    """The (domain, table) a ``<prefix>/<domain>/<table>[?...]`` path names."""
    assert path.startswith(prefix + "/"), f"{path} is not on {prefix}"
    domain, table = path[len(prefix) + 1 :].split("?", 1)[0].split("/")
    return domain, table


def test_there_is_a_demo_tour_to_check():
    """The tours are found, not listed: every published demo has one, so none is left unchecked."""
    published = {
        spec.parent.name
        for spec in _NAMED.glob("*/demo.yaml")
        if yaml.safe_load(spec.read_text()).get("published") is True
    }
    assert published, "no published demo"
    assert published <= {p.parent.name for p in TOURS}, "a published demo with no tour.yaml"


@pytest.mark.parametrize("path", TOURS, ids=[p.parent.name for p in TOURS])
def test_every_step_names_what_the_demo_has(path):
    _check_steps(path.parent, yaml.safe_load(path.read_text())["steps"])


def _check_steps(home: Path, steps: list[dict]) -> None:
    """Every step of a tour of the demo at ``home`` names what the demo has, on the right page."""
    config = yaml.safe_load((home / "config.yaml").read_text())
    sources = {s["id"] for s in config["sources"]}
    aliases = {r["alias"] for r in config["relationships"] if r.get("alias")}
    examples = {q.stem for q in (home / "queries").glob("*.graphql")}
    roles = {r["id"] for r in config["roles"]} | {"all"}
    # prod, and each environment the demo's setup makes (REQ-2053).
    setup = home / "setup.yaml"
    made = (yaml.safe_load(setup.read_text()) or {}).get("steps") or [] if setup.exists() else []
    envs = {"prod"} | {s["environment"]["name"] for s in made if "environment" in s}
    terms = {t["name"] for t in config.get("glossary_terms", [])}
    tables = {f"{t['source_id']}.{config_table_name(t)}" for t in config["tables"]}
    relations = _relations(config)
    gates = _route_capabilities()
    paths, methods, labels = _surface(config)
    keys = [s["key"] for s in steps]
    assert len(keys) == len(set(keys)), keys
    assert "route" in steps[0], "the first step names the page the tour opens on"
    page = None
    before = None
    for step in steps:
        page = step.get("route", page)
        previous, before = before, step
        named = [k for k in _TARGETS if k in step]
        assert len(named) == 1, f"{step['key']}: names {named}, not one target"
        assert set(step) <= {"key", "route", "capability", *_TARGETS}, step
        if "route" in step:
            assert gates.get(step["route"]) == step["capability"], step
        else:
            assert "capability" not in step, step
        if "source" in step:
            assert step["source"] in sources, step
        surface = next((k for k in _SURFACE_PAGES if k in step), None)
        if surface is not None:
            assert page == _SURFACE_PAGES[surface], f"{step['key']}: {surface} runs on its page"
        if "api" in step:
            method, path = step["api"].split(" ", 1)
            assert method == "GET", f"{step['key']}: a table's REST endpoint is GET"
            assert _path_names(path, "/data/rest") in paths, step
            asked = {kv.split("=", 1)[0] for kv in path.partition("?")[2].split("&") if kv}
            assert asked <= _REST_PARAMS, f"{step['key']}: no REST parameter {asked - _REST_PARAMS}"
        if "jsonapi" in step:
            assert _path_names(step["jsonapi"], "/data/jsonapi") in paths, step
        if "grpc" in step:
            assert step["grpc"].split("(", 1)[0] in methods, step
        if "cypher" in step:
            named = set(re.findall(r"\(\s*\w*\s*:\s*(\w+)", step["cypher"]))
            assert named and named <= labels, f"{step['key']}: labels {named - labels}"
            types = set(re.findall(r"\[\s*\w*\s*:\s*(\w+)", step["cypher"]))
            assert types <= aliases, f"{step['key']}: relationships {types - aliases}"
        if "table" in step:
            assert step["table"] in tables, step
        if "polly" in step:
            # Its answer is stored at the template build and graded by the bake-off.
            assert "route" not in step, f"{step['key']}: Polly opens over the page it is on"
            # A demo with no bake-off questions of its own has none a step may ask.
            asked = {q.text for q in QUESTIONS.get(home.name, ())}
            assert step["polly"] in asked, f"{step['key']}: not one of the demo's own questions"
        if "relationship" in step:
            assert step["relationship"] in aliases, step
        if "example" in step:
            assert step["example"] in examples, step
            # An example query opens in a new tab of the GraphQL page: a step on any other page
            # waits for an editor that is not there, and the tour stalls on it.
            assert page == "/query", f"{step['key']}: an example runs on /query, not {page}"
        if "term" in step:
            assert step["term"] in terms, step
        if "role" in step:
            # One role, "all", or a set of the demo's roles and its declared pseudo-roles
            # (reviewer,region__eu, REQ-2057), which the tour hands to the explorer.
            claim_types = set(config.get("claim_roles") or [])
            for part in step["role"].split(","):
                claim, sep, value = part.partition("__")
                assert (
                    part in roles or part in claim_types or (sep and value and claim in claim_types)
                ), step
            # Picked in the SQL explorer's run-as picker, which is on that page alone.
            assert page == "/sql", f"{step['key']}: a role is picked on /sql, not {page}"
        if "group_by" in step:
            # The Graph page's frame on screen grouped: the one the Cypher step before it ran.
            assert page == "/graph", f"{step['key']}: a frame is grouped on /graph, not {page}"
            assert previous is not None and "cypher" in previous, (
                f"{step['key']}: groups the frame a Cypher step just before it ran"
            )
            assert re.fullmatch(r"[A-Za-z_]\w*", step["group_by"]), step
        if "env" in step:
            # Picked in the header's environment switcher, on any page.
            assert step["env"] in envs, f"{step['key']}: no environment {step['env']!r}"
        if "lineage" in step:
            # Built from the Lineage page's deep link, on that page.
            assert page == "/lineage", f"{step['key']}: a lineage is built on /lineage, not {page}"
            read = {
                f"{t.db}.{t.name}"
                for t in sqlglot.parse_one(step["lineage"], read="postgres").find_all(
                    sqlglot.exp.Table
                )
            }
            assert read and read <= relations, f"{step['key']}: reads {read - relations}"
        if "sql" in step:
            read = {
                f"{t.db}.{t.name}"
                for t in sqlglot.parse_one(step["sql"], read="postgres").find_all(sqlglot.exp.Table)
            }
            assert read and read <= relations, f"{step['key']}: reads {read - relations}"


@pytest.mark.parametrize("path", TOURS, ids=[p.parent.name for p in TOURS])
def test_every_step_has_its_words_in_every_language(path):
    keys = [s["key"] for s in yaml.safe_load(path.read_text())["steps"]]
    for locale in sorted(p for p in _LOCALES.iterdir() if p.is_dir()):
        words = json.loads((locale / "tour.json").read_text())["tour"]
        for key in keys:
            step = words["steps"].get(key)
            assert step and step["title"] and step["description"], f"{locale.name}: {key}"
        menu = words["demos"][path.parent.name]
        assert menu["title"] and menu["summary"], locale.name


def test_the_setup_status_carries_the_started_demo_s_tour(monkeypatch):
    monkeypatch.setenv(demo.NAMED_DEMO_DIR_ENV, str(_NAMED / "weblogs"))
    steps = demo.demo_tour(demo.started_demo_directory())
    assert steps[0] == {
        "key": "weblogsSources",
        "route": "/sources",
        "capability": "source_registration",
        "source": "shop_access",
    }


def test_no_named_demo_has_no_tour(tmp_path, monkeypatch):
    monkeypatch.delenv(demo.NAMED_DEMO_DIR_ENV, raising=False)
    assert demo.demo_tour(demo.started_demo_directory()) == []
    monkeypatch.setenv(demo.NAMED_DEMO_DIR_ENV, str(tmp_path))
    assert demo.demo_tour(demo.started_demo_directory()) == []


def _named_demo(tmp_path, steps, answers=None):
    (tmp_path / "tour.yaml").write_text(yaml.safe_dump({"steps": steps}))
    if answers is not None:
        demo.tour_answers_path(str(tmp_path)).write_text(json.dumps(answers))
    return str(tmp_path)


_ASK = {"key": "ask", "polly": "Which pages are slowest?"}
_SOURCES = {"key": "s", "route": "/sources", "capability": "source_registration", "source": "x"}


def test_a_polly_step_carries_the_answer_stored_for_its_question(tmp_path, monkeypatch):
    answers = {"ask": {"question": _ASK["polly"], "answer": "The **/checkout** page."}}
    monkeypatch.setenv(demo.NAMED_DEMO_DIR_ENV, _named_demo(tmp_path, [_SOURCES, _ASK], answers))
    assert demo.demo_tour(demo.started_demo_directory()) == [
        _SOURCES,
        {**_ASK, "answer": "The **/checkout** page."},
    ]


def test_an_answer_stored_for_another_question_is_not_served(tmp_path, monkeypatch):
    answers = {"ask": {"question": "An older question?", "answer": "Old."}}
    monkeypatch.setenv(demo.NAMED_DEMO_DIR_ENV, _named_demo(tmp_path, [_SOURCES, _ASK], answers))
    assert demo.demo_tour(demo.started_demo_directory()) == [_SOURCES, _ASK]


def test_a_demo_not_built_from_a_template_serves_no_answer(tmp_path, monkeypatch):
    monkeypatch.setenv(demo.NAMED_DEMO_DIR_ENV, _named_demo(tmp_path, [_SOURCES, _ASK]))
    assert demo.demo_tour(demo.started_demo_directory()) == [_SOURCES, _ASK]


_WEBLOGS = _NAMED / "weblogs"
_ON = {"route": "/openapi", "capability": "query_development"}


@pytest.mark.parametrize(
    "step",
    [
        {"key": "x", **_ON, "api": "GET /data/rest/shop/no_such_table"},
        {"key": "x", **_ON, "api": "POST /data/rest/shop/view_slowest_pages"},
        {"key": "x", **_ON, "api": "GET /data/rest/shop/view_slowest_pages?sortBy=path"},
        {
            "key": "x",
            "route": "/jsonapi",
            "capability": "query_development",
            "jsonapi": "/data/jsonapi/elsewhere/view_slowest_pages",
        },
        {
            "key": "x",
            "route": "/grpc",
            "capability": "query_development",
            "grpc": "QuerySNoSuchTable(by=[path])",
        },
        {
            "key": "x",
            "route": "/graph",
            "capability": "query_development",
            "cypher": "MATCH (p:Pages) RETURN p",
        },
        {
            "key": "x",
            "route": "/graph",
            "capability": "query_development",
            "cypher": "MATCH (p:S__ViewSlowestPages)-[:NO_SUCH]->(r) RETURN p",
        },
        # A surface's call on another surface's page.
        {
            "key": "x",
            "route": "/sql",
            "capability": "query_development",
            "api": "GET /data/rest/shop/view_slowest_pages",
        },
    ],
    ids=["path", "method", "parameter", "jsonapi", "grpc", "label", "relationship", "page"],
)
def test_a_call_the_demo_does_not_serve_is_refused(step):
    with pytest.raises(AssertionError):
        _check_steps(_WEBLOGS, [step])


_GRAPHED = {
    "key": "c",
    "route": "/graph",
    "capability": "query_development",
    "cypher": "MATCH (p:S__ViewSlowestPages)-[:RAN]->(r:S__ViewRequestSql) RETURN p, r",
}


def test_a_frame_is_grouped_on_the_graph_page_just_after_the_cypher_step_that_ran_it():
    _check_steps(_WEBLOGS, [_GRAPHED, {"key": "g", "group_by": "degTotal"}])


@pytest.mark.parametrize(
    "steps",
    [
        # No Cypher step just before it: there is no frame of the tour's to group.
        [
            {**_ON, "key": "a", "api": "GET /data/rest/shop/view_slowest_pages"},
            {"key": "g", "group_by": "degTotal"},
        ],
        # On another page than the Graph page.
        [
            _GRAPHED,
            {
                "key": "g",
                "route": "/sql",
                "capability": "query_development",
                "group_by": "degTotal",
            },
        ],
        # Not a property's name.
        [_GRAPHED, {"key": "g", "group_by": "deg Total"}],
    ],
    ids=["no-frame", "page", "property"],
)
def test_a_grouping_with_no_frame_to_group_is_refused(steps):
    with pytest.raises(AssertionError):
        _check_steps(_WEBLOGS, steps)


def test_each_surface_s_call_on_the_demo_is_accepted():
    _check_steps(
        _WEBLOGS,
        [
            {**_ON, "key": "a", "api": "GET /data/rest/shop/view_slowest_pages?groupBy=path"},
            {
                "key": "b",
                "route": "/jsonapi",
                "capability": "query_development",
                "jsonapi": "/data/jsonapi/shop/view_slowest_pages?groupBy=path&aggregate=count",
            },
            {
                "key": "c",
                "route": "/grpc",
                "capability": "query_development",
                "grpc": "QuerySViewSlowestPagesGroupBy(by=[path], funcs=[count])",
            },
            {
                "key": "d",
                "route": "/graph",
                "capability": "query_development",
                "cypher": "MATCH (p:S__ViewSlowestPages)-[:RAN]->(r:S__ViewRequestSql) RETURN p, r",
            },
        ],
    )


@pytest.mark.parametrize("path", TOURS, ids=[p.parent.name for p in TOURS])
def test_every_tour_ends_on_a_landing_page_the_ui_has(path):
    """The last step's "Look around on my own" leaves the visitor on the page the demo names:
    one the app routes, with the right its gate needs."""
    landing = yaml.safe_load(path.read_text())["landing"]
    assert set(landing) == {"route", "capability"}, landing
    gates = _route_capabilities()
    assert landing["route"] in gates, f"{path.parent.name}: no page at {landing['route']}"
    assert gates[landing["route"]] == landing["capability"], landing


def test_the_setup_status_carries_the_demo_s_landing_page():
    assert demo.demo_landing(_NAMED / "weblogs") == {
        "route": "/sql",
        "capability": "query_development",
    }
    assert demo.demo_landing(None) is None


@pytest.mark.parametrize("path", TOURS, ids=[p.parent.name for p in TOURS])
def test_a_demo_names_its_fields_as_its_sandbox_does(path):
    """domain_prefix is deployment-wide (org_settings: not org-overridable): a sandbox's schema is
    built with the sandbox tier's naming, which prefixes each domain (config/provisa-install-base),
    not the demo's own. A demo that turned it off had examples and Cypher its sandbox refused."""
    base = yaml.safe_load((_REPO / "config" / "provisa-install-base.yaml").read_text())
    config = yaml.safe_load((path.parent / "config.yaml").read_text())
    assert config["naming"]["domain_prefix"] == base["naming"]["domain_prefix"], path.parent.name
