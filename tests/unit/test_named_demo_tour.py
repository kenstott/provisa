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
from datetime import datetime
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
    "trigger",
    "example",
    "sql",
    "api",
    "jsonapi",
    "grpc",
    "cypher",
    "group_by",
    "lineage",
    "term",
    "product",
    "role",
    "env",
    "as_of",
    "as_recorded",
    "as_recorded_from",
    "polly",
    "image",
    "notebook",
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
# A table's detail sections a step may open (the read view anchors each one's panel), and the
# column tabs of its Columns section.
_UI = _REPO / "provisa-ui" / "src" / "pages" / "tables"
_TABLE_PANELS = set(
    re.findall(
        r'"([a-z]+)"',
        re.search(
            r"TABLE_SECTIONS: TableSection\[\] = \[([^\]]*)\]",
            (_UI / "tableSections.ts").read_text(),
        ).group(1),
    )
)
_COLUMN_TABS = set(
    re.findall(
        r'"([a-z]+)"',
        re.search(
            r"METADATA_TABS: ColumnTab\[\] = \[([^\]]*)\]", (_UI / "columnTopics.ts").read_text()
        ).group(1),
    )
) | {"testing"}
# The graphs of View Profile Runs a step may show (ProfileRunsModal's 3D views).
_PROFILE_GRAPHS = set(
    re.findall(
        r'"([A-Za-z]+)"',
        re.search(
            r"const VIEWS_3D = \[([^\]]*)\]", (_UI / "ProfileRunsModal.tsx").read_text()
        ).group(1),
    )
)
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
    # Routes made from a list of paths, one gate for all (the Python and Notebook tabs).
    for m in re.finditer(
        r'\{\[([^\]]+)\]\.map\(\(path\) => \([\s\S]*?capability=\{?"([a-z_]+)"', source
    ):
        for path in re.findall(r'"([^"]+)"', m.group(1)):
            out[path] = m.group(2)
    # The Admin surfaces are a `[path, capability]` table rather than JSX routes.
    for m in re.finditer(r'\["(/admin/[^"]+)",\s*"([a-z_]+)"\]', source):
        out[m.group(1)] = m.group(2)
    return out


def _relations(config: dict) -> set[str]:
    """The ``"domain"."name"`` each table and view of the demo is read by in SQL, and the ops
    record and meta tables every organisation has (its audit log and reports, its model's own
    tables, sensitive coverage among them), which a tour may show."""
    from provisa.api._meta_views import _OPS_LOG_TABLE_VIEWS, _OPS_REPORT_VIEWS
    from provisa.api.app_loaders import _META_TABLES
    from provisa.core.config_loader import views_as_tables

    tables = views_as_tables(json.loads(json.dumps(config)))["tables"]
    from provisa.observability.ops_schema import OPS_SIGNAL_VIEWS

    # REQ-862: and the ops signal views, each MV refresh's lineage among them.
    signals = [v[0] for v in OPS_SIGNAL_VIEWS]
    ops = {f"ops.{name}" for name in (*_OPS_LOG_TABLE_VIEWS, *_OPS_REPORT_VIEWS, *signals)}
    meta = {f"meta.{name}" for name in _META_TABLES}
    return (
        {f"{t['domain_id']}.{t.get('alias') or config_table_name(t)}" for t in tables} | ops | meta
    )


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
    """The tours are found, not listed: every published demo has one, so none is left unchecked.
    A demo whose demo.yaml says ``tour: core`` is toured by the product's core tour and has no
    tour.yaml of its own."""
    specs = {
        spec.parent.name: yaml.safe_load(spec.read_text()) for spec in _NAMED.glob("*/demo.yaml")
    }
    core = {name for name, spec in specs.items() if spec.get("tour") == "core"}
    assert all(spec.get("tour") in (None, "core") for spec in specs.values()), "tour is only core"
    published = {name for name, spec in specs.items() if spec.get("published") is True}
    tours = {p.parent.name for p in TOURS}
    assert published, "no published demo"
    assert published - core <= tours, "a published demo with no tour.yaml"
    assert not core & tours, "a demo toured by the core tour has a tour.yaml"


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
    products = {p["id"] for p in config.get("data_products", [])}
    tables = {f"{t['source_id']}.{config_table_name(t)}" for t in config["tables"]}
    profiled = {
        f"{t['source_id']}.{config_table_name(t)}"
        for t in config["tables"]
        if t.get("profiler_source_id")
    }
    relations = _relations(config)
    gates = _route_capabilities()
    paths, methods, labels = _surface(config)
    keys = [s["key"] for s in steps]
    assert len(keys) == len(set(keys)), keys
    assert "route" in steps[0], "the first step names the page the tour opens on"
    page = None
    before = None
    # REQ-2083: each sql step's statement, by key, for a later step that takes a value of its answer.
    sql_steps: dict[str, str] = {}
    for step in steps:
        page = step.get("route", page)
        previous, before = before, step
        named = [k for k in _TARGETS if k in step]
        assert len(named) == 1, f"{step['key']}: names {named}, not one target"
        assert set(step) <= {"key", "route", "capability", "open", "graph", "demo", *_TARGETS}, step
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
        if "open" in step:
            # The table's detail panel, on a section whose panel a step can anchor on and, in
            # Columns, a column tab the read view has.
            assert "table" in step and page == "/tables", f"{step['key']}: open is a table's panel"
            section, _, tab = step["open"].partition(".")
            assert section in _TABLE_PANELS, f"{step['key']}: no anchored section {section!r}"
            assert not tab or tab in _COLUMN_TABS, f"{step['key']}: no column tab {tab!r}"
        if "graph" in step:
            # One graph of a profiler member's latest run, in View Profile Runs, which the read
            # view offers in the table's quality section; the step's caption says what the tour
            # does before it explains.
            assert step.get("open") == "quality", f"{step['key']}: a graph opens from quality"
            assert step["graph"] in _PROFILE_GRAPHS, f"{step['key']}: no graph {step['graph']!r}"
            assert step["table"] in profiled, f"{step['key']}: {step['table']} has no profiler"
        if "polly" in step:
            # Its answer is stored at the template build and graded by the bake-off.
            assert "route" not in step, f"{step['key']}: Polly opens over the page it is on"
            # A demo with no bake-off questions of its own has none a step may ask.
            asked = {q.text for q in QUESTIONS.get(home.name, ())}
            assert step["polly"] in asked, f"{step['key']}: not one of the demo's own questions"
        if "demo" in step:
            # Another named demo, opened from the step's popover: one that is published, its words
            # in the tour menu.
            linked = _NAMED / step["demo"]
            assert step["demo"] != home.name, f"{step['key']}: a demo links another demo"
            assert (linked / "tour.yaml").is_file(), (
                f"{step['key']}: no named demo {step['demo']!r}"
            )
            published = yaml.safe_load((linked / "demo.yaml").read_text()).get("published")
            assert published is True, f"{step['key']}: {step['demo']} is not published"
        if "image" in step:
            # A picture beside tour.yaml, shown over the page of the step before it.
            assert previous is not None, f"{step['key']}: an image shows beside a step before it"
            assert "route" not in step, f"{step['key']}: an image shows on the page it is on"
            picture = home / step["image"]
            assert picture.parent == home and picture.suffix == ".png", step
            assert picture.read_bytes()[:8] == b"\x89PNG\r\n\x1a\n", f"{picture} is not a PNG"
        if "relationship" in step:
            assert step["relationship"] in aliases, step
        if "trigger" in step:
            # A scheduled trigger's row is on the Triggers page, under its id.
            assert step["trigger"] in {t["id"] for t in config.get("scheduled_triggers", [])}, step
            assert page == "/admin/scheduled-tasks", step
        if "example" in step:
            assert step["example"] in examples, step
            # An example query opens in a new tab of the GraphQL page: a step on any other page
            # waits for an editor that is not there, and the tour stalls on it.
            assert page == "/query", f"{step['key']}: an example runs on /query, not {page}"
        if "term" in step:
            assert step["term"] in terms, step
        if "product" in step:
            assert step["product"] in products, step
            # The Data Products page opens the product its deep link names.
            assert page == "/data-products", f"{step['key']}: a product opens on /data-products"
        if "role" in step:
            # One role, "all", or a set of the demo's roles and its declared derived roles
            # (reviewer,region__eu, REQ-2057), which the tour hands to the explorer.
            claim_types = {d["attribute"] for d in config.get("derived_roles") or []}
            for part in step["role"].split(","):
                claim, sep, value = part.partition("__")
                assert (
                    part in roles or part in claim_types or (sep and value and claim in claim_types)
                ), step
            # Picked in the SQL explorer's run-as picker on /sql; anywhere else, in the header's role
            # picker, which every other page's requests run as. A set of roles is the explorer's.
            if "," in step["role"]:
                assert page == "/sql", (
                    f"{step['key']}: a set of roles is picked on /sql, not {page}"
                )
        if "group_by" in step:
            # The Graph page's frame on screen grouped: the one the Cypher step before it ran.
            assert page == "/graph", f"{step['key']}: a frame is grouped on /graph, not {page}"
            assert previous is not None and "cypher" in previous, (
                f"{step['key']}: groups the frame a Cypher step just before it ran"
            )
            assert re.fullmatch(r"[A-Za-z_]\w*", step["group_by"]), step
        for travel in ("as_of", "as_recorded"):
            if travel in step:
                # REQ-1163: set in the SQL explorer's picker, on that page, as one ISO 8601 time.
                assert page == "/sql", f"{step['key']}: {travel} is set on /sql, not {page}"
                datetime.fromisoformat(step[travel])
        if "as_recorded_from" in step:
            # REQ-2083: set in the SQL explorer's picker from a value of an EARLIER sql step's
            # answer: its row (0-based) and a column its statement returns.
            assert page == "/sql", f"{step['key']}: as_recorded_from is set on /sql, not {page}"
            taken = step["as_recorded_from"]
            assert set(taken) == {"step", "row", "column"}, f"{step['key']}: {taken}"
            assert taken["step"] in sql_steps, (
                f"{step['key']}: no sql step {taken['step']!r} before it"
            )
            assert isinstance(taken["row"], int) and taken["row"] >= 0, (
                f"{step['key']}: row {taken}"
            )
            returned = [
                e.alias_or_name
                for e in sqlglot.parse_one(sql_steps[taken["step"]], read="postgres").expressions
            ]
            assert taken["column"] in returned, (
                f"{step['key']}: {taken['column']!r} not in {returned}"
            )
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
        if "notebook" in step:
            # REQ-2084: the demo's notebook, its cells added to the Notebook tab's and run there.
            assert step["notebook"] is True, step
            assert page == "/notebook", f"{step['key']}: the notebook runs on /notebook, not {page}"
            _check_notebook(home, relations)
        if "sql" in step:
            sql_steps[step["key"]] = step["sql"]
            read = {
                f"{t.db}.{t.name}"
                for t in sqlglot.parse_one(step["sql"], read="postgres").find_all(sqlglot.exp.Table)
            }
            assert read and read <= relations, f"{step['key']}: reads {read - relations}"


def _notebook_code(cells: list[str]) -> list[str]:
    """Each cell as Python: a line IPython runs as a magic or a shell command (``%pip``) is left
    out, as the kernel takes it before Python does."""
    return [
        "\n".join(line for line in cell.splitlines() if not line.lstrip().startswith(("%", "!")))
        for cell in cells
    ]


def _check_notebook(home: Path, relations: set[str]) -> None:
    """The demo's notebook: Python cells, the first signing in as the page's user, and every
    statement it reads (``provisa_client.query("...")``) over the demo's tables and views."""
    import ast

    cells = demo.demo_notebook(home)
    assert cells, f"{home.name}: a notebook step needs {demo.DEMO_NOTEBOOK} beside tour.yaml"
    code = _notebook_code(cells)
    assert "import provisa_client" in code[0] and "provisa_client.sign_in()" in code[0], code[0]
    for cell in code:
        for call in ast.walk(ast.parse(cell)):
            if (
                isinstance(call, ast.Call)
                and isinstance(call.func, ast.Attribute)
                and call.func.attr == "query"
                and call.args
                and isinstance(call.args[0], ast.Constant)
            ):
                read = {
                    f"{t.db}.{t.name}"
                    for t in sqlglot.parse_one(call.args[0].value, read="postgres").find_all(
                        sqlglot.exp.Table
                    )
                }
                assert read and read <= relations, f"{home.name}: reads {read - relations}"


def test_a_demo_s_notebook_is_its_code_cells_in_order(tmp_path):
    import json

    (tmp_path / "notebook.ipynb").write_text(
        json.dumps(
            {
                "cells": [
                    {"cell_type": "code", "source": ["import provisa_client\n", "x = 1"]},
                    {"cell_type": "markdown", "source": "Words"},
                    {"cell_type": "code", "source": "x + 1"},
                ]
            }
        )
    )
    assert demo.demo_notebook(tmp_path) == ["import provisa_client\nx = 1", "x + 1"]
    assert demo.demo_notebook(tmp_path / "none") == []
    assert demo.demo_notebook(None) == []


def test_a_notebook_step_needs_the_demo_to_ship_one():
    with pytest.raises(AssertionError, match="needs notebook.ipynb"):
        _check_steps(
            _WEBLOGS,
            [
                {
                    "key": "n",
                    "route": "/notebook",
                    "capability": "query_development",
                    "notebook": True,
                }
            ],
        )


@pytest.mark.parametrize("path", TOURS, ids=[p.parent.name for p in TOURS])
def test_every_step_has_its_words_in_every_language(path):
    steps = yaml.safe_load(path.read_text())["steps"]
    keys = [s["key"] for s in steps]
    # A step that acts first (a graph it opens) shows its caption while it does.
    acting = {s["key"] for s in steps if "graph" in s}
    for locale in sorted(p for p in _LOCALES.iterdir() if p.is_dir()):
        words = json.loads((locale / "tour.json").read_text())["tour"]
        for key in keys:
            step = words["steps"].get(key)
            assert step and step["title"] and step["description"], f"{locale.name}: {key}"
            assert key not in acting or step.get("acting"), f"{locale.name}: {key} caption"
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


_PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 8
_SHOT = {"key": "shot", "image": "app.png"}


def test_an_image_step_carries_its_picture_as_a_data_url(tmp_path, monkeypatch):
    """The tour is all the visitor's browser needs: on the sandbox tier a separate request for the
    picture would not name the demo it is of."""
    import base64

    (tmp_path / "app.png").write_bytes(_PNG)
    monkeypatch.setenv(demo.NAMED_DEMO_DIR_ENV, _named_demo(tmp_path, [_SOURCES, _SHOT]))
    src = "data:image/png;base64," + base64.b64encode(_PNG).decode()
    assert demo.demo_tour(demo.started_demo_directory()) == [_SOURCES, {**_SHOT, "image_src": src}]


@pytest.mark.parametrize("name", ["missing.png", "../app.png", "app.jpg"])
def test_an_image_not_a_png_beside_the_tour_is_refused_by_name(tmp_path, monkeypatch, name):
    (tmp_path / "app.jpg").write_bytes(_PNG)
    monkeypatch.setenv(
        demo.NAMED_DEMO_DIR_ENV, _named_demo(tmp_path, [_SOURCES, {**_SHOT, "image": name}])
    )
    with pytest.raises(ValueError, match=re.escape(repr(name))):
        demo.demo_tour(demo.started_demo_directory())


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


# REQ-2083: a step takes its "as recorded at" time from a value of an earlier sql step's answer.
_VERSIONS = {
    "key": "v",
    "route": "/sql",
    "capability": "query_development",
    "sql": 'SELECT p.path, p.page_ms AS recorded_at FROM "shop"."view_slowest_pages" AS p',
}


def test_a_recording_time_is_taken_from_an_earlier_statements_answer():
    taken = {"step": "v", "row": 0, "column": "recorded_at"}
    _check_steps(_WEBLOGS, [_VERSIONS, {"key": "r", "as_recorded_from": taken}])


@pytest.mark.parametrize(
    "steps",
    [
        # No sql step of that key before it.
        [
            {**_VERSIONS, "key": "w"},
            {"key": "r", "as_recorded_from": {"step": "v", "row": 0, "column": "recorded_at"}},
        ],
        # A column the statement does not return.
        [_VERSIONS, {"key": "r", "as_recorded_from": {"step": "v", "row": 0, "column": "nope"}}],
        # Not a row index.
        [
            _VERSIONS,
            {"key": "r", "as_recorded_from": {"step": "v", "row": -1, "column": "recorded_at"}},
        ],
        # On another page than the SQL explorer.
        [
            _VERSIONS,
            {
                "key": "r",
                "route": "/graph",
                "capability": "query_development",
                "as_recorded_from": {"step": "v", "row": 0, "column": "recorded_at"},
            },
        ],
    ],
    ids=["no-step", "column", "row", "page"],
)
def test_a_recording_time_from_nothing_the_tour_ran_is_refused(steps):
    with pytest.raises(AssertionError):
        _check_steps(_WEBLOGS, steps)
