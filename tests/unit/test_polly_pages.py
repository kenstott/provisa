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


# -- tags on screen ------------------------------------------------------------------------------

UI = ROOT / "provisa-ui/src"
_TAG_SOURCES = "\n".join(
    (UI / f).read_text()
    for f in (
        "components/TagControl.tsx",
        "components/TagAssignmentForm.tsx",
        "components/TagCitations.tsx",
    )
)
_TAGS_TAB = (UI / "components/admin/TagsTab.tsx").read_text()


def _in_code(test_id: str, source: str) -> bool:
    """A test id the table names is in the code: literally, or as the template the code builds it
    from (`tag-pill-<tag id>` is `tag-pill-${tag.id}`)."""
    built = test_id.replace("<tag id>", "${tag.id}")
    return f'"{test_id}"' in source or f"`{built}`" in source or f'"{built}"' in source


def _a_citation_list(test_id: str) -> None:
    """The citation list with ``test_id``, and its picker's document, section and Add."""
    cited = (UI / "components/TagCitations.tsx").read_text()
    for built in ("${testId}-picker", "${testId}-document", "${testId}-section", "${testId}-add"):
        assert f"`{built}`" in cited, built
    for suffix in ("-picker-document", "-picker-section", "-picker-add"):
        assert f"{test_id}{suffix}" in chat._SYSTEM, suffix


def test_the_pages_that_show_tag_chips_say_read_screen_reports_their_tags():
    for route in polly_pages.TAG_ROUTES:
        assert polly_pages.PAGES[route].tags, route
        assert "read_screen's tags" in chat._SYSTEM.split(f"- {route}: ", 1)[1].split("\n", 1)[0]


@pytest.mark.parametrize(
    "mount",
    [
        "pages/SourcesPage.tsx",
        "pages/TablesPage.tsx",
        "pages/tables/TableReadView.tsx",
        "components/relationships/RelationshipRow.tsx",
        "pages/CommandsPage.tsx",
        "pages/data-products/ProductHeader.tsx",
        "pages/metrics/MetricDetailPanel.tsx",
        "components/admin/TermReadView.tsx",
        "pages/security/RoleRead.tsx",
        "pages/security/RlsRuleRead.tsx",
        "pages/library/LibraryDetail.tsx",
    ],
)
def test_every_surface_that_mounts_the_tag_control_is_a_tag_page(mount):
    assert "<TagControl" in (UI / mount).read_text()
    assert polly_pages.TAG_ROUTES == tuple(
        r for r in polly_pages.PAGES if polly_pages.PAGES[r].tags
    )


_TAG_CONTROL_IDS = (
    "tag-pill-<tag id>",
    "tag-picker-toggle",
    "tag-option-<tag id>",
    "tag-edit-<tag id>",
    "tag-param-<tag id>",
    "tag-reason-<tag id>",
    "tag-expires-<tag id>",
    "tag-citations-<tag id>",
    "tag-apply-<tag id>",
    "tag-cancel-<tag id>",
)


@pytest.mark.parametrize("test_id", _TAG_CONTROL_IDS)
def test_the_tag_controls_named_are_in_the_code(test_id):
    assert test_id in polly_pages.TAG_CONTROLS
    assert _in_code(test_id, _TAG_SOURCES), test_id


def test_the_assignment_forms_citation_picker_is_named():
    _a_citation_list("tag-citations-<tag id>")


_TAG_EDITOR_IDS = (
    "tags-create-button",
    "tags-scope-bar",
    "tags-reason-policy",
    "tags-expires-policy",
    "tags-param-policy",
    "tags-requires-citation",
    "tags-defined-by",
    "tags-save",
    "tags-delete-<tag id>",
)


@pytest.mark.parametrize("test_id", _TAG_EDITOR_IDS)
def test_the_tag_editors_controls_named_are_in_the_code(test_id):
    assert test_id in polly_pages.PAGES["/admin/tags"].what
    assert _in_code(test_id, _TAGS_TAB), test_id


def test_the_tag_editor_names_its_citation_fields_as_the_form_labels_them():
    tags = polly_pages.PAGES["/admin/tags"].what
    locale = json.loads((UI / "i18n/locales/en/tagCitations.json").read_text())["tagCitations"]
    assert f'"{locale["definitionLabel"]}"' in tags  # Why this tag exists
    assert f'"{locale["requiresCitationLabel"]}"' in tags  # Assignments require a citation
    _a_citation_list("tags-defined-by")
    edit = json.loads((UI / "i18n/locales/en/tagsTab.json").read_text())["tagsTab"]["editTag"]
    assert edit.replace("{{id}}", "<tag id>") in tags  # the row's edit button, by its name


def test_she_is_told_how_to_learn_more_about_a_tag_and_that_an_edit_is_confirmed():
    section = polly_pages.TAG_CONTROLS
    for view in (
        "meta.tag_instances",
        "meta.tag_citations",
        "meta.section_tags",
        "meta.policy_coverage",
        "meta.library_documents",
    ):
        assert view in section, view
    for tool in ("assign_tag", "unassign_tag", "cite_tag_assignment", "cite_tag_definition"):
        assert tool in section, tool
    assert "confirm" in section
    assert section in chat._SYSTEM
    assert "REQ-" not in section


def test_the_screen_state_publishes_the_tags_shape_she_is_told_of():
    for key in ("kind", "key", "control", "via", "citationGap", "needsReview", "expired"):
        assert f"{key}" in SCREEN, key
        assert key in polly_pages.TAG_CONTROLS, key
    assert "useScreenTags(" in (UI / "components/TagControl.tsx").read_text()
    tool = next(t for t in chat._CLIENT_TOOLS if t["name"] == "read_screen")
    assert "`tags`" in tool["description"]
