# Copyright (c) 2026 Kenneth Stott
# Canary: 3b58e1d7-6a24-4c09-9f13-70e5c8a4d621
#
# This source code is licensed under the Business Source License 1.1

"""Step definitions for REQ-1368 — the published metadata-export documentation page.

The page is checked against the code it describes rather than against a copy of itself: the
provider list comes from the registry, the settings list from ``MetadataExportConfig``. A
provider or a setting added later fails this test until the page mentions it, which is the only
thing that keeps a docs page from drifting into fiction.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from pytest_bdd import given, scenarios, then, when

from provisa.api.metadata_export import registered_providers
from provisa.core.models import MetadataExportConfig

scenarios("../features/REQ-1368.feature")

_REPO_ROOT = Path(__file__).resolve().parents[2]
DOC_PATH = _REPO_ROOT / "docs" / "metadata-export.md"
MKDOCS_PATH = _REPO_ROOT / "mkdocs.yml"

# The nav section the page must sit in, and the doc it must sit beside.
NAV_SECTION = "Security & Governance"


@pytest.fixture
def shared_data() -> dict:
    return {}


def _nav_entries(node, section: str) -> list[dict]:
    """Every page entry under the named nav section, wherever it sits in the nav tree."""
    if isinstance(node, list):
        return [entry for item in node for entry in _nav_entries(item, section)]
    if isinstance(node, dict):
        found: list[dict] = []
        for key, value in node.items():
            if key == section and isinstance(value, list):
                found.extend(item for item in value if isinstance(item, dict))
            found.extend(_nav_entries(value, section))
        return found
    return []


@given("the published documentation set")
def _docs(shared_data):
    # mkdocs.yml uses tags PyYAML's safe loader rejects (e.g. !!python/name: for extensions);
    # the nav itself is plain, so an unknown-tag-tolerant loader reads it without executing
    # anything. REQ-1368 asserts on the nav only.
    class _NavLoader(yaml.SafeLoader):
        pass

    _NavLoader.add_multi_constructor("tag:yaml.org,2002:python/name", lambda *_: None)
    _NavLoader.add_multi_constructor("!", lambda *_: None)

    shared_data["mkdocs"] = yaml.load(MKDOCS_PATH.read_text(), Loader=_NavLoader)
    shared_data["text"] = DOC_PATH.read_text()


@when("a reader looks for how to publish metadata to an external catalog")
def _read(shared_data):
    shared_data["nav"] = _nav_entries(shared_data["mkdocs"].get("nav", []), NAV_SECTION)


@then("the metadata export page is reachable from the Security and Governance navigation")
def _navigable(shared_data):
    targets = [value for entry in shared_data["nav"] for value in entry.values()]
    assert "metadata-export.md" in targets, targets
    excluded = shared_data["mkdocs"].get("exclude_docs", "") or ""
    assert "metadata-export.md" not in excluded


@then("the page states that publication is outbound only")
def _outbound_only(shared_data):
    text = shared_data["text"].lower()
    assert "outbound only" in text
    # The claim that matters is the absence of an ingest path, not the phrase alone.
    assert "reads an external catalog back" in text


@then("the page names every registered export provider")
def _providers(shared_data):
    for name in registered_providers():
        assert f"`{name}`" in shared_data["text"], name


@then("the page documents each configuration setting the export config accepts")
def _settings(shared_data):
    for field in MetadataExportConfig.model_fields:
        assert f"`{field}`" in shared_data["text"], field


@then("the page describes the sync model the scheduler actually runs")
def _sync_model_matches_the_scheduler(shared_data):
    # The page is checked against the wiring, not against itself: REQ-1072's drain and reconcile
    # are armed at startup, so a page still calling them unbuilt sends an admin publishing by
    # hand for a sync that is already firing.
    text = shared_data["text"]
    assert "not yet running" not in text
    startup = (_REPO_ROOT / "provisa" / "api" / "app_startup.py").read_text()
    assert "register_all_orgs" in startup, (
        "the sync jobs are no longer armed at startup — REQ-1368's docs page describes a sync "
        "the scheduler does not run"
    )
    for claim in ("Change-driven", "Scheduled reconcile", "On demand"):
        assert claim in text, claim
