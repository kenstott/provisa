# Copyright (c) 2026 Kenneth Stott
# Canary: 8f2c6a17-d3e4-4b90-a6c1-52e9b07d13fa
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Polly page parity: every route of the app has a row in the one table, every row is a route,
and what the UI publishes to read_screen is what the table says."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from provisa.api.mcp import chat, polly_pages

ROOT = Path(__file__).resolve().parents[2]
APP = (ROOT / "provisa-ui/src/App.tsx").read_text()
SCREEN = (ROOT / "provisa-ui/src/mcp/screenState.ts").read_text()

# Rendered by name above the route table (App.tsx: DEMO_ROUTE, SignedOutGate).
OUTSIDE_THE_ROUTES = {"/demos/:name", "/login", "/register"}


def _app_routes() -> set[str]:
    routes = set(re.findall(r'path="(/[^"]*)"', APP))
    routes |= {
        path for path, _cap in re.findall(r'\[\s*"(/[^"]*)"\s*,\s*"([a-z_]+)"\s*,?\s*\]', APP)
    }
    for mapped in re.findall(r'\{\[("/[^\]]*)\]\.map', APP):
        routes |= set(re.findall(r'"(/[^"]*)"', mapped))
    return routes


def _published(name: str) -> set[str]:
    body = re.search(rf"export const {name}[^=]*= new Set\(\[(.*?)\]\)", SCREEN, re.S)
    assert body is not None, name
    return set(re.findall(r'"(/[^"]*)"', body.group(1)))


def test_the_routes_are_read_from_the_app():
    routes = _app_routes()
    assert {"/tables", "/python", "/notebook", "/admin/ontology", "/sparql"} <= routes
    assert len(routes) > 50


def test_every_route_has_a_polly_entry():
    assert _app_routes() - set(polly_pages.PAGES) == set()


def test_every_entry_is_a_route():
    assert set(polly_pages.PAGES) - _app_routes() - OUTSIDE_THE_ROUTES == set()


def test_every_entry_is_in_her_system_prompt():
    for route in polly_pages.PAGES:
        assert f"- {route}: " in chat._SYSTEM


def test_a_route_that_publishes_inputs_says_what_they_are():
    for route in _published("INPUT_ROUTES"):
        assert polly_pages.PAGES[route].reads.startswith("inputs: "), route


def test_a_route_with_an_editor_says_so():
    for route in _published("EDITOR_ROUTES"):
        assert "editor" in polly_pages.PAGES[route].reads, route


@pytest.mark.parametrize(
    "words",
    [
        "view=erd",
        "collapsed=<a,b>",
        "erd-export-svg",
        "flat true (2D) or false (3D)",
        "layout 'auto', 'force' or 'layered'",
        "state.panel 'graph' or 'terms'",
        "product=<id>",
        "press erd-download, then erd-export-svg",
        "layout=<saved layout name>",
        "erd-layout-save-as",
        "tables-model-star",
        "wizard-create",
        "glossary-find-terms-btn",
        "find-terms-start",
        "jdbc-source-driver-select",
    ],
)
def test_she_is_told_how_to_set_each_page(words):
    assert words in chat._SYSTEM


def test_she_can_drive_the_odata_explorer():
    """The OData page was display-only: she could open it and do nothing on it."""
    odata = polly_pages.PAGES["/odata"]
    assert "state.odataUrl" in odata.sets
    assert "state.autoRun" in odata.sets
    for option in ("$filter", "$select", "$expand", "$orderby", "$top", "$skip", "$count"):
        assert option in odata.sets, option
    for read in ("set", "url", "status", "rows", "warnings", "error"):
        assert f" {read}," in f" {odata.reads.removeprefix('inputs: ')},", read
    assert "/odata" in _published("INPUT_ROUTES")


def test_the_odata_page_takes_the_payload_she_is_told_of():
    page = (ROOT / "provisa-ui/src/pages/ODataPage.tsx").read_text()
    assert "odataUrl?: string; autoRun?: boolean" in page
    assert 'useScreenInputs("/odata"' in page


def _reads(route: str) -> set[str]:
    """The input names a row's read_screen lists, each without what its parentheses say."""
    listed = polly_pages.PAGES[route].reads.removeprefix("inputs: ")
    return {re.match(r"\w+", part).group() for part in re.split(r",\s*(?![^()]*\))", listed)}


@pytest.mark.parametrize(
    ("route", "keys"),
    [
        ("/tables", {"modeling", "wizardStep", "layout", "layoutUnsaved", "collapsed"}),
        ("/relationships", {"layout", "layoutUnsaved", "collapsed"}),
        ("/admin/ontology", {"findTerms"}),
    ],
)
def test_the_new_screens_inputs_are_named(route, keys):
    """The saved layout (REQ-2189), the modeling wizard (REQ-2199) and Find terms (REQ-2179) are
    published to read_screen; the table names each input so she knows to read it."""
    assert keys <= _reads(route)


@pytest.mark.parametrize(
    ("source", "published"),
    [
        ("components/erd/ErdView.tsx", "layout, layoutUnsaved: unsaved"),
        ("pages/TablesPage.tsx", "modeling: showModeling || null"),
        ("pages/tables/modeling/WizardFrame.tsx", 'useScreenInputs("/tables", { wizardStep:'),
        ("components/admin/GlossaryTab.tsx", "findTerms: findTermsOpen"),
    ],
)
def test_the_page_publishes_what_the_table_names(source, published):
    assert published in (ROOT / "provisa-ui/src" / source).read_text()


def test_she_is_told_read_screen_reports_the_data_node():
    assert "dataNode" in polly_pages.SYSTEM_SECTION
    assert "dataNode" in chat._SYSTEM
    tool = next(t for t in chat._CLIENT_TOOLS if t["name"] == "read_screen")
    assert "dataNode" in tool["description"]


def test_she_can_read_the_mcp_explorer():
    """The MCP explorer was described as her own chat, and published nothing to read_screen."""
    explore = polly_pages.PAGES["/explore"]
    assert "MCP explorer" in explore.what
    assert "separate chat" in explore.what
    assert "same chat" not in explore.what
    assert "state.mcpQuestion" in explore.sets
    assert {"status", "error", "draft", "tools", "messageCount", "messages"} <= _reads("/explore")
    assert "/explore" in _published("INPUT_ROUTES")
    assert "this same chat" not in chat._SYSTEM
    assert "this same chat" not in json.dumps(chat._CLIENT_TOOLS)


def test_the_mcp_explorer_publishes_what_the_table_names():
    page = (ROOT / "provisa-ui/src/pages/McpExplorePage.tsx").read_text()
    assert 'useScreenInputs("/explore"' in page
    assert "mcpQuestion?: string" in page
