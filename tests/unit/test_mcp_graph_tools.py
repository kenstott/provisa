# Copyright (c) 2026 Kenneth Stott
# Canary: 8d1af2c5-fecd-4610-adbd-deeddaecf7a7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The external MCP server's graph tools: run_cypher answers a Cypher query's rows and the graph
they hold as Cytoscape elements; render_graph hands back those elements with the layout the UI
draws them with; generate_explore_queries answers the six query surfaces under the caller's role."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from provisa.api.mcp import tools
from tests.unit.test_mcp_server import _FakeJobStore, _make_state


def _node(label: str, nid: int, **props) -> dict:
    return {"id": nid, "label": label, "tableLabel": label.split(":")[-1], "properties": props}


def _edge(identity: int, start: dict, end: dict, rel: str, **extra) -> dict:
    return {
        "identity": identity,
        "start": start["id"],
        "end": end["id"],
        "type": rel,
        "properties": {},
        "startNode": start,
        "endNode": end,
        **extra,
    }


ADA = _node("Sales:Customer", 1, name="Ada")
ORDER = _node("Sales:Order", 7, total=10.5)
PLACED = _edge(70, ADA, ORDER, "PLACED")


# --- the elements -------------------------------------------------------------


def test_a_relationship_s_ends_and_itself_become_nodes_and_an_edge_with_the_graph_page_s_ids():
    elements = tools.cytoscape_elements([{"c": ADA, "r": PLACED, "o": ORDER}])
    assert elements["nodes"] == [
        {
            "data": {
                "id": "Sales:Customer:1",
                "label": "Sales:Customer",
                "tableLabel": "Customer",
                "properties": {"name": "Ada"},
            }
        },
        {
            "data": {
                "id": "Sales:Order:7",
                "label": "Sales:Order",
                "tableLabel": "Order",
                "properties": {"total": 10.5},
            }
        },
    ]
    assert elements["edges"] == [
        {
            "data": {
                "id": "70",
                "source": "Sales:Customer:1",
                "target": "Sales:Order:7",
                "label": "PLACED",
                "properties": {},
            }
        }
    ]


def test_nodes_inside_paths_lists_and_maps_are_found_once_each():
    path = {"nodes": [ADA, ORDER], "edges": [PLACED]}
    elements = tools.cytoscape_elements([{"p": path}, {"xs": [ADA, {"m": ORDER}]}, {"n": 3}])
    assert [n["data"]["id"] for n in elements["nodes"]] == ["Sales:Customer:1", "Sales:Order:7"]
    assert [e["data"]["id"] for e in elements["edges"]] == ["70"]


def test_a_junction_backed_edge_keeps_its_junction_and_scalars_add_nothing():
    via = _edge(71, ADA, ORDER, "BONDED_PAIR", junctionTable="pet_relations")
    elements = tools.cytoscape_elements([{"r": via, "count": 2, "name": "x"}])
    assert elements["edges"][0]["data"]["junctionTable"] == "pet_relations"
    assert tools.cytoscape_elements([{"count": 2}]) == {"nodes": [], "edges": []}


# --- run_cypher -----------------------------------------------------------------


async def test_run_cypher_runs_through_the_cypher_pipeline_under_the_role():
    state = _make_state()
    run = AsyncMock(return_value=(["c", "r", "o"], [[ADA, PLACED, ORDER]], None))
    with patch("provisa.bolt.session._execute_cypher", run):
        result = await tools.run_cypher(
            state, "analyst", "MATCH (c)-[r:PLACED]->(o) RETURN c, r, o", {"n": 1}
        )
    run.assert_awaited_once_with(
        "MATCH (c)-[r:PLACED]->(o) RETURN c, r, o", {"n": 1}, "analyst", roles=["analyst"]
    )
    assert result["columns"] == ["c", "r", "o"]
    assert result["rows"] == [{"c": ADA, "r": PLACED, "o": ORDER}]
    assert [n["data"]["id"] for n in result["elements"]["nodes"]] == [
        "Sales:Customer:1",
        "Sales:Order:7",
    ]
    assert result["truncated"] is False


async def test_run_cypher_caps_its_rows_and_draws_only_the_rows_returned(monkeypatch):
    monkeypatch.setenv("PROVISA_MCP_MAX_ROWS", "1")
    second = _node("Sales:Customer", 2, name="Grace")
    run = AsyncMock(return_value=(["c"], [[ADA], [second]], None))
    with patch("provisa.bolt.session._execute_cypher", run):
        result = await tools.run_cypher(_make_state(), "analyst", "MATCH (c) RETURN c")
    assert result["row_count"] == 1 and result["total_rows"] == 2 and result["truncated"] is True
    assert [n["data"]["id"] for n in result["elements"]["nodes"]] == ["Sales:Customer:1"]


async def test_run_cypher_refuses_no_role_and_lets_a_refusal_through():
    with pytest.raises(ValueError, match="role is required"):
        await tools.run_cypher(_make_state(), "", "MATCH (n) RETURN n")
    run = AsyncMock(side_effect=PermissionError("denied for role analyst"))
    with patch("provisa.bolt.session._execute_cypher", run):
        with pytest.raises(PermissionError):
            await tools.run_cypher(_make_state(), "analyst", "MATCH (s:Secret) RETURN s")


# --- render_graph -------------------------------------------------------------------


class _Done:
    def __init__(self, returncode=0, stdout=b"\x89PNG\r\n\x1a\nimage", stderr=b""):
        self.returncode, self.stdout, self.stderr = returncode, stdout, stderr


@pytest.fixture
def renderer(tmp_path, monkeypatch):
    """A renderer file where the tool looks for it, and `node` on PATH."""
    script = tmp_path / "render.mjs"
    script.write_text("// stand-in")
    monkeypatch.setattr(tools, "GRAPH_RENDERER", script)
    monkeypatch.setattr(
        "shutil.which", lambda name: "/usr/local/bin/node" if name == "node" else None
    )
    return script


def test_render_graph_hands_the_renderer_the_graph_and_returns_its_png(renderer, monkeypatch):
    monkeypatch.setenv("PROVISA_MCP_RENDER_TIMEOUT_S", "7")
    elements = tools.cytoscape_elements([{"r": PLACED}])
    run = MagicMock(return_value=_Done())
    with patch("subprocess.run", run):
        png = tools.render_graph(elements, "elk", "dark", 1024, 768)
    assert png.startswith(b"\x89PNG")
    (argv,), kwargs = run.call_args
    assert argv == ["/usr/local/bin/node", str(renderer)]
    assert kwargs["timeout"] == 7 and kwargs["capture_output"] is True
    assert json.loads(kwargs["input"]) == {
        "elements": elements,
        "layout": "elk",
        "theme": "dark",
        "width": 1024,
        "height": 768,
    }


def test_render_graph_draws_light_with_the_force_layout_unless_told(renderer):
    run = MagicMock(return_value=_Done())
    with patch("subprocess.run", run):
        tools.render_graph(tools.cytoscape_elements([{"r": PLACED}]))
    request = json.loads(run.call_args.kwargs["input"])
    assert (request["layout"], request["theme"], request["width"], request["height"]) == (
        "force",
        "light",
        800,
        600,
    )


def test_render_graph_says_why_a_drawing_failed(renderer, monkeypatch):
    import subprocess

    elements = tools.cytoscape_elements([{"r": PLACED}])
    with patch("subprocess.run", MagicMock(return_value=_Done(1, b"", b'unknown layout "x"\n'))):
        with pytest.raises(RuntimeError, match='render_graph failed: unknown layout "x"'):
            tools.render_graph(elements)
    monkeypatch.setenv("PROVISA_MCP_RENDER_TIMEOUT_S", "2")
    with patch("subprocess.run", MagicMock(side_effect=subprocess.TimeoutExpired("node", 2))):
        with pytest.raises(RuntimeError, match=r"longer than mcp.render_timeout_s \(2 s\)"):
            tools.render_graph(elements)


def test_render_graph_names_what_the_host_lacks(tmp_path, monkeypatch):
    elements = tools.cytoscape_elements([{"r": PLACED}])
    monkeypatch.setattr("shutil.which", lambda name: None)
    with pytest.raises(RuntimeError, match="`node` is not on this host's PATH"):
        tools.render_graph(elements)
    monkeypatch.setattr("shutil.which", lambda name: "/usr/local/bin/node")
    monkeypatch.setattr(tools, "GRAPH_RENDERER", tmp_path / "absent" / "render.mjs")
    with pytest.raises(RuntimeError, match="renderer is not built"):
        tools.render_graph(elements)


def test_render_graph_refuses_what_it_cannot_draw(renderer):
    elements = tools.cytoscape_elements([{"r": PLACED}])
    with pytest.raises(ValueError, match="layout must be one of"):
        tools.render_graph(elements, "circle")
    with pytest.raises(ValueError, match="theme must be one of"):
        tools.render_graph(elements, theme="sepia")
    with pytest.raises(ValueError, match="width must be between 100 and 4000"):
        tools.render_graph(elements, width=50)
    with pytest.raises(ValueError, match="as run_cypher returns them"):
        tools.render_graph({"nodes": []})
    dangling = {"nodes": [elements["nodes"][0]], "edges": elements["edges"]}
    with pytest.raises(ValueError, match="target names no node"):
        tools.render_graph(dangling)


def test_the_render_timeout_is_an_operator_setting_with_a_floor():
    from provisa.core import settings_catalog

    s = next(s for s in settings_catalog.DECLARED if s.key == "mcp.render_timeout_s")
    assert (s.type, s.effect, s.default, s.min, s.unit, s.env) == (
        "int",
        "live",
        30,
        1,
        "seconds",
        "PROVISA_MCP_RENDER_TIMEOUT_S",
    )


async def test_the_server_s_render_graph_answers_an_mcp_png_image(renderer):
    from provisa.api.mcp.server import build_mcp_server

    mcp = build_mcp_server(_make_state())
    elements = tools.cytoscape_elements([{"r": PLACED}])
    with patch("subprocess.run", MagicMock(return_value=_Done())):
        content = await mcp.call_tool("render_graph", {"elements": elements, "theme": "dark"})
    blocks = content[0] if isinstance(content, tuple) else content
    image = next(b for b in blocks if getattr(b, "type", None) == "image")
    assert image.mimeType == "image/png"
    import base64

    assert base64.b64decode(image.data).startswith(b"\x89PNG")


# --- the server --------------------------------------------------------------------


async def test_the_server_offers_run_cypher_and_render_graph():
    from provisa.api.mcp.server import build_mcp_server

    names = {t.name for t in await build_mcp_server(_make_state()).list_tools()}
    assert {"run_cypher", "render_graph", "generate_explore_queries"} <= names


# --- generate_explore_queries: all six surfaces, under the caller's role ---------------


async def test_generate_explore_queries_answers_all_nine_surfaces_under_the_caller_s_role():
    from provisa.nl.job import BranchResult
    from provisa.nl.runner import _TARGETS

    state = _make_state()
    state.config.nl = SimpleNamespace(rate_limit=None)
    store = _FakeJobStore()
    seen: dict = {}

    async def fake_run_nl_job(
        job_id, question, role, app_state, job_store, llm, strict=False, *, deliver
    ):
        seen["role"] = role
        job = await job_store.get(job_id)
        for target in _TARGETS:
            job.branches[target] = BranchResult(query=f"{target} query", error=None)

    with (
        patch("provisa.nl.job.make_job_store", return_value=store),
        patch("provisa.core.org_settings.resolve_org_config", AsyncMock(return_value={})),
        patch("provisa.core.org_secrets.read_org_api_keys", AsyncMock(return_value={})),
        patch("provisa.llm.client.ProvisaLLMClient"),
        patch("provisa.nl.runner.run_nl_job", fake_run_nl_job),
    ):
        result = await tools.generate_explore_queries(state, "analyst", "orders by region")

    assert sorted(_TARGETS) == ["cypher", "graphql", "grpc", "jsonapi", "openapi", "sql"]
    # REQ-2114: the six the NL pipeline builds, and the three built on its SQL and question.
    assert sorted(result) == sorted([*_TARGETS, "nl", "python", "notebook"])
    assert all(result[t] == {"query": f"{t} query", "error": None} for t in _TARGETS)
    assert result["python"]["query"].endswith("df = provisa_client.query('sql query')\ndf")
    assert result["notebook"]["cells"][-1] == "%%sql\nsql query"
    # The pipeline ran as the caller, and the job it ran was the caller's.
    assert store.job is not None
    assert seen["role"] == "analyst" and store.job.role == "analyst"
    with pytest.raises(PermissionError):
        await tools.generate_explore_queries(state, "nobody", "orders by region")
